import os
import psycopg2
from dotenv import load_dotenv
from datetime import datetime, timedelta
import calendar
import random
import itertools
import time
import copy
from audit_logger import audit_log

load_dotenv()
DB_PARAMS = {
    "host": os.getenv("DB_HOST", "localhost"),
    "database": os.getenv("DB_NAME", "roster_db"),
    "user": os.getenv("DB_USER", "roster_bot"),
    "password": os.getenv("DB_PASS")
}

def fetch_team_settings(team_id):
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    cursor.execute("SELECT sat_coverage, sun_coverage, min_preferences FROM teams WHERE id = %s", (team_id,))
    res = cursor.fetchone()
    cursor.close()
    conn.close()
    return {"sat_coverage": res[0], "sun_coverage": res[1], "min_preferences": res[2]}

def fetch_data_from_db(year, month, weekend_dates, team_id):
    year_month = f"{year}-{month:02d}"
    weekend_set = set(weekend_dates)

    engineers = []
    availability = {}
    eng_max_shifts = {}
    eng_leaves = {}

    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()

    # Source of truth for eligibility: preferences table
    # Hard cap for assignments: engineers.max_shifts
    cursor.execute("""
        SELECT e.id, e.name, e.max_shifts, p.priority_dates
        FROM preferences p
        JOIN engineers e ON e.id = p.engineer_id
        WHERE e.is_active = TRUE
          AND e.team_id = %s
          AND p.target_month = %s
          AND p.status = 'submitted'
          AND p.preferred_count > 0
          AND cardinality(p.priority_dates) > 0
    """, (team_id, year_month))
    eligible_rows = cursor.fetchall()

    for eng_id, name, max_shifts, priority_dates in eligible_rows:
        engineers.append(name)

        # Enforce hard per-engineer cap from engineers table
        eng_max_shifts[name] = max_shifts if max_shifts is not None else 0

        # Keep ranked order from date[]; filter to weekends in the target month
        pref_dates = [d.strftime('%Y-%m-%d') for d in (priority_dates or [])]
        availability[name] = [d for d in pref_dates if d in weekend_set]

        # Leave blockouts
        cursor.execute("""
            SELECT block_date
            FROM leave_blockouts
            WHERE engineer_id = %s
              AND TO_CHAR(block_date, 'YYYY-MM') = %s
        """, (eng_id, year_month))
        eng_leaves[name] = [row[0].strftime('%Y-%m-%d') for row in cursor.fetchall()]

    cursor.close()
    conn.close()
    return engineers, availability, eng_max_shifts, eng_leaves

def fetch_boundary_roster(year, month, team_id):
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    first_day = datetime(year, month, 1)
    last_day = datetime(year, month, calendar.monthrange(year, month)[1])
    start_bound = (first_day - timedelta(days=7)).strftime('%Y-%m-%d')
    end_bound = (last_day + timedelta(days=7)).strftime('%Y-%m-%d')

    cursor.execute("""
        SELECT shift_date, e.name FROM roster_assignments r
        JOIN engineers e ON r.engineer_id = e.id
        WHERE r.team_id = %s AND r.shift_date >= %s::date AND r.shift_date <= %s::date
    """, (team_id, start_bound, end_bound))

    boundary_roster = {}
    for row in cursor.fetchall():
        date_str = row[0].strftime('%Y-%m-%d')
        if date_str not in boundary_roster:
            boundary_roster[date_str] = []
        boundary_roster[date_str].append(row[1])

    cursor.close()
    conn.close()
    return boundary_roster

def save_roster_to_db(roster, team_id):
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    dates = list(roster.keys())
    if dates:
        cursor.execute("DELETE FROM roster_assignments WHERE team_id = %s AND shift_date = ANY(%s::date[])", (team_id, dates))
        for date_str, assigned_names in roster.items():
            for name in assigned_names:
                cursor.execute("SELECT id FROM engineers WHERE name = %s AND team_id = %s", (name, team_id))
                eng = cursor.fetchone()
                if eng:
                    cursor.execute(
                        "INSERT INTO roster_assignments (shift_date, team_id, engineer_id) VALUES (%s, %s, %s)",
                        (date_str, team_id, eng[0])
                    )
    conn.commit()
    cursor.close()
    conn.close()

def get_weekend_dates(year, month):
    return [day.strftime('%Y-%m-%d') for day in calendar.Calendar().itermonthdates(year, month) if day.month == month and day.weekday() in (5, 6)]

def day_name(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d").strftime("%A")

def get_same_weekend_day(date_str):
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    if dt.weekday() == 5:
        return (dt + timedelta(days=1)).strftime("%Y-%m-%d")
    elif dt.weekday() == 6:
        return (dt - timedelta(days=1)).strftime("%Y-%m-%d")
    return None

def count_7_day_violations(roster):
    violations = 0
    for d in sorted(roster.keys()):
        if day_name(d) == "Sunday":
            next_sat = (datetime.strptime(d, "%Y-%m-%d") + timedelta(days=6)).strftime("%Y-%m-%d")
            if next_sat in roster:
                violations += len(set(roster[d]).intersection(set(roster[next_sat])))
    return violations

def evaluate_solution(solution, availability):
    active_engs = [e for e, dates in availability.items() if len(dates) > 0]
    if not active_engs:
        return 0, 0, 0, 0
    shifts = [solution["shifts_count"][e] for e in active_engs]
    spread = max(shifts) - min(shifts)
    unbalanced = sum(
        1 for e in active_engs
        if solution["shifts_count"][e] >= 2 and
        (solution["saturday_count"][e] == 0 or solution["sunday_count"][e] == 0)
    )
    seven_day = count_7_day_violations(solution["roster"])
    pref_score = sum(
        availability[e].index(day)
        for day, assigned_engs in solution["roster"].items()
        for e in assigned_engs
        if day in availability[e]
    )
    return (spread, unbalanced, seven_day, pref_score)

def draft_roster(availability, eng_max_shifts, eng_leaves, weekend_dates, settings, boundary_roster, seed=42, timeout_seconds=10):
    rng = random.Random(seed)
    engineers = sorted(availability.keys())
    day_avail_counts = {
        d: sum(1 for e in engineers if d in availability.get(e, []) and d not in eng_leaves.get(e, []))
        for d in weekend_dates
    }
    sorted_days = sorted(weekend_dates, key=lambda d: day_avail_counts[d])

    roster = copy.deepcopy(boundary_roster)
    for d in weekend_dates:
        roster[d] = []

    shifts_count = {e: 0 for e in engineers}
    saturday_count = {e: 0 for e in engineers}
    sunday_count = {e: 0 for e in engineers}

    start_time = time.time()
    valid_solutions = []

    def solve(day_idx):
        if time.time() - start_time > timeout_seconds:
            if valid_solutions:
                return True
            raise TimeoutError(f"Timed out after {timeout_seconds}s. No valid combinations found.")

        if day_idx == len(sorted_days):
            valid_solutions.append({
                "roster": copy.deepcopy(roster),
                "shifts_count": shifts_count.copy(),
                "saturday_count": saturday_count.copy(),
                "sunday_count": sunday_count.copy()
            })
            return len(valid_solutions) >= 50

        current_day = sorted_days[day_idx]
        is_sat = day_name(current_day) == "Saturday"
        needed = settings['sat_coverage'] if is_sat else settings['sun_coverage']

        candidates = []
        for e in engineers:
            if current_day not in availability.get(e, []):
                continue
            if current_day in eng_leaves.get(e, []):
                continue
            # max_shifts constraint enforced here
            if shifts_count[e] >= eng_max_shifts.get(e, 0):
                continue

            same_weekend_day = get_same_weekend_day(current_day)
            if same_weekend_day in roster and e in roster[same_weekend_day]:
                continue

            candidates.append(e)

        def candidate_sort_key(e):
            causes_7_day = 0
            if is_sat:
                prev_sun = (datetime.strptime(current_day, "%Y-%m-%d") - timedelta(days=6)).strftime("%Y-%m-%d")
                if prev_sun in roster and e in roster[prev_sun]:
                    causes_7_day = 1
            else:
                next_sat = (datetime.strptime(current_day, "%Y-%m-%d") + timedelta(days=6)).strftime("%Y-%m-%d")
                if next_sat in roster and e in roster[next_sat]:
                    causes_7_day = 1

            return (
                shifts_count[e],
                1 if (saturday_count[e] > 0 if is_sat else sunday_count[e] > 0) else 0,
                causes_7_day,
                availability[e].index(current_day),
                len(availability.get(e, [])),
                rng.random()
            )

        candidates.sort(key=candidate_sort_key)

        if len(candidates) < needed:
            return False

        for combo in itertools.combinations(candidates, needed):
            for e in combo:
                shifts_count[e] += 1
                if is_sat:
                    saturday_count[e] += 1
                else:
                    sunday_count[e] += 1
                roster[current_day].append(e)

            if solve(day_idx + 1):
                return True

            for e in combo:
                shifts_count[e] -= 1
                if is_sat:
                    saturday_count[e] -= 1
                else:
                    sunday_count[e] -= 1
                roster[current_day].remove(e)

        return False

    solve(0)

    if not valid_solutions:
        raise ValueError("Unable to find a valid roster satisfying all constraints.")

    valid_solutions.sort(key=lambda sol: evaluate_solution(sol, availability))
    best_solution = valid_solutions[0]
    final_roster = {d: best_solution["roster"][d] for d in weekend_dates}
    return final_roster, 0

def get_smart_suggestion(engineers, availability, eng_max_shifts, eng_leaves, weekend_dates, settings):
    lowest_margin = 999
    bottleneck_day = None

    for d in weekend_dates:
        needed = settings['sat_coverage'] if day_name(d) == "Saturday" else settings['sun_coverage']
        avail_count = sum(1 for e in engineers if d in availability.get(e, []) and d not in eng_leaves.get(e, []))
        margin = avail_count - needed

        if margin < lowest_margin:
            lowest_margin = margin
            bottleneck_day = d

    if bottleneck_day:
        potential_helpers = [
            e for e in engineers
            if bottleneck_day not in availability.get(e, [])
            and bottleneck_day not in eng_leaves.get(e, [])
            and eng_max_shifts.get(e, 0) > 0
        ]
        if potential_helpers:
            helpers_str = ", ".join(potential_helpers[:3])
            return f"💡 AI Suggestion: {bottleneck_day} ({day_name(bottleneck_day)}) is critically understaffed. Ask {helpers_str} to add this date to their availability."

    return "💡 AI Suggestion: Try reducing required coverage in Team Settings, or increasing Max Shifts for engineers."

def generate_monthly_roster(year, month, team_id, seed=42):
    year_month = f"{year}-{month:02d}"
    engineers, availability, eng_max_shifts, eng_leaves, weekend_dates, settings = [], {}, {}, {}, [], {}
    conn_audit = None

    try:
        conn_audit = psycopg2.connect(**DB_PARAMS)

        # START log
        audit_log(
            conn=conn_audit,
            source="scheduler",
            action="RUN_ALGORITHM_START",
            status="success",
            team_id=team_id,
            target_month=year_month,
            entity_type="roster_assignments",
            entity_id=year_month,
            details={"year": year, "month": month, "seed": seed}
        )
        conn_audit.commit()

        settings = fetch_team_settings(team_id)
        weekend_dates = get_weekend_dates(year, month)
        engineers, availability, eng_max_shifts, eng_leaves = fetch_data_from_db(year, month, weekend_dates, team_id)
        boundary_roster = fetch_boundary_roster(year, month, team_id)

        if not engineers:
            msg = "No active engineers found for this team."
            audit_log(
                conn=conn_audit,
                source="scheduler",
                action="RUN_ALGORITHM_FAILED",
                status="failed",
                team_id=team_id,
                target_month=year_month,
                entity_type="roster_assignments",
                entity_id=year_month,
                details={"reason": "no_eligible_engineers"},
                error_message=msg
            )
            conn_audit.commit()
            return {"success": False, "message": msg}

        # Capacity Check
        total_needed = (
            len([d for d in weekend_dates if day_name(d) == "Saturday"]) * settings['sat_coverage'] +
            len([d for d in weekend_dates if day_name(d) == "Sunday"]) * settings['sun_coverage']
        )
        total_capacity = sum(eng_max_shifts.values())

        if total_capacity < total_needed:
            suggestion = get_smart_suggestion(engineers, availability, eng_max_shifts, eng_leaves, weekend_dates, settings)
            msg = f"Insufficient capacity. Needed: {total_needed}, Max Capacity: {total_capacity}.<br><br>{suggestion}"

            audit_log(
                conn=conn_audit,
                source="scheduler",
                action="RUN_ALGORITHM_FAILED",
                status="failed",
                team_id=team_id,
                target_month=year_month,
                entity_type="roster_assignments",
                entity_id=year_month,
                details={
                    "reason": "insufficient_capacity",
                    "total_needed": total_needed,
                    "total_capacity": total_capacity
                },
                error_message="insufficient_capacity"
            )
            conn_audit.commit()
            return {"success": False, "message": msg}

        roster, _ = draft_roster(
            availability, eng_max_shifts, eng_leaves,
            weekend_dates, settings, boundary_roster, seed, 10
        )
        save_roster_to_db(roster, team_id)

        total_assigned = sum(len(v) for v in roster.values())

        # SUCCESS log
        audit_log(
            conn=conn_audit,
            source="scheduler",
            action="RUN_ALGORITHM_SUCCESS",
            status="success",
            team_id=team_id,
            target_month=year_month,
            entity_type="roster_assignments",
            entity_id=year_month,
            details={
                "total_needed": total_needed,
                "total_capacity": total_capacity,
                "total_assigned": total_assigned,
                "weekend_days": len(weekend_dates),
                "eligible_engineers": len(engineers)
            }
        )
        conn_audit.commit()

        return {"success": True, "message": "Roster successfully generated and saved!"}

    except Exception as e:
        suggestion = get_smart_suggestion(engineers, availability, eng_max_shifts, eng_leaves, weekend_dates, settings) if weekend_dates else ""

        try:
            if conn_audit:
                audit_log(
                    conn=conn_audit,
                    source="scheduler",
                    action="RUN_ALGORITHM_FAILED",
                    status="failed",
                    team_id=team_id,
                    target_month=year_month,
                    entity_type="roster_assignments",
                    entity_id=year_month,
                    details={"seed": seed},
                    error_message=str(e)
                )
                conn_audit.commit()
        except Exception:
            pass

        return {"success": False, "message": f"Algorithm failed to find a valid combination: {e}<br><br>{suggestion}"}

    finally:
        if conn_audit:
            conn_audit.close()