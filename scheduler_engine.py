import os
import psycopg2
from psycopg2.extras import RealDictCursor
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
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("""
        SELECT sat_coverage, sun_coverage, min_preferences, 
               strict_7_day_rest, allow_same_weekend 
        FROM teams WHERE id = %s
    """, (team_id,))
    res = cursor.fetchone()
    cursor.close()
    conn.close()
    
    if not res:
        return {"sat_coverage": 1, "sun_coverage": 1, "strict_7_day_rest": False, "allow_same_weekend": False}
        
    return {
        "sat_coverage": res["sat_coverage"], 
        "sun_coverage": res["sun_coverage"], 
        "strict_7_day_rest": res["strict_7_day_rest"] or False,
        "allow_same_weekend": res["allow_same_weekend"] or False
    }

def fetch_data_from_db(year, month, weekend_dates, team_id):
    year_month = f"{year}-{month:02d}"
    weekend_set = set(weekend_dates)

    engineers = []
    availability = {}
    eng_max_shifts = {}
    eng_leaves = {}

    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()

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
        eng_max_shifts[name] = max_shifts if max_shifts is not None else 0
        pref_dates = [d.strftime('%Y-%m-%d') for d in (priority_dates or [])]
        availability[name] = [d for d in pref_dates if d in weekend_set]

        cursor.execute("""
            SELECT block_date FROM leave_blockouts
            WHERE engineer_id = %s AND TO_CHAR(block_date, 'YYYY-MM') = %s
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
    
    # The formatted current month to exclude (e.g., '2026-05')
    current_month_str = f"{year}-{month:02d}"

    # NEW: Added AND TO_CHAR(r.shift_date, 'YYYY-MM') != %s to exclude the current month
    cursor.execute("""
        SELECT shift_date, e.name FROM roster_assignments r
        JOIN engineers e ON r.engineer_id = e.id
        WHERE r.team_id = %s 
          AND r.shift_date >= %s::date 
          AND r.shift_date <= %s::date
          AND TO_CHAR(r.shift_date, 'YYYY-MM') != %s
    """, (team_id, start_bound, end_bound, current_month_str))

    boundary_roster = {}
    for row in cursor.fetchall():
        date_str = row[0].strftime('%Y-%m-%d')
        if date_str not in boundary_roster:
            boundary_roster[date_str] = []
        boundary_roster[date_str].append(row[1])

    cursor.close()
    conn.close()
    return boundary_roster

def save_roster_to_db(roster, team_id, year, month):
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    year_month = f"{year}-{month:02d}"
    
    try:
        # 1. Delete the old roster for this month ONLY (happens inside the transaction)
        cursor.execute("""
            DELETE FROM roster_assignments 
            WHERE team_id = %s AND TO_CHAR(shift_date, 'YYYY-MM') = %s
        """, (team_id, year_month))
        
        # 2. Insert the newly generated roster
        for date_str, assigned_names in roster.items():
            for name in assigned_names:
                cursor.execute("SELECT id FROM engineers WHERE name = %s AND team_id = %s", (name, team_id))
                eng = cursor.fetchone()
                if eng:
                    cursor.execute(
                        "INSERT INTO roster_assignments (shift_date, team_id, engineer_id) VALUES (%s, %s, %s)",
                        (date_str, team_id, eng[0])
                    )
                    
        # 3. Commit the transaction (This safely swaps the old for the new instantly)
        conn.commit()
        
    except Exception as e:
        # If anything fails, cancel the deletion and keep the old roster!
        conn.rollback()
        raise e
    finally:
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

def draft_roster(availability, eng_max_shifts, eng_leaves, weekend_dates, settings, boundary_roster, seed=42, timeout_seconds=10):
    rng = random.Random(seed)
    engineers = sorted(availability.keys())
    
    roster = copy.deepcopy(boundary_roster)
    for d in weekend_dates:
        if d not in roster:
            roster[d] = []

    shifts_count = {e: 0 for e in engineers}
    saturday_count = {e: 0 for e in engineers}
    sunday_count = {e: 0 for e in engineers}

    start_time = time.time()
    
    # Tracker for smart error messages
    bottleneck_tracker = {"day": None, "reason": None, "max_depth": -1}

    def get_valid_candidates(current_day, current_roster, current_shifts):
        is_sat = day_name(current_day) == "Saturday"
        candidates = []
        blocked_reasons = {"max_shifts": [], "same_weekend": [], "7_day_rest": []}

        for e in engineers:
            if current_day not in availability.get(e, []):
                continue
            if current_day in eng_leaves.get(e, []):
                continue
            
            # 1. Hard Cap Check
            if current_shifts[e] >= eng_max_shifts.get(e, 0):
                blocked_reasons["max_shifts"].append(e)
                continue

            # 2. Configurable Toggle: Same Weekend
            if not settings.get('allow_same_weekend', False):
                same_weekend_day = get_same_weekend_day(current_day)
                if same_weekend_day in current_roster and e in current_roster[same_weekend_day]:
                    blocked_reasons["same_weekend"].append(e)
                    continue

            # 3. Configurable Toggle: Strict 7-Day Rest
            if settings.get('strict_7_day_rest', False):
                if is_sat:
                    prev_sun = (datetime.strptime(current_day, "%Y-%m-%d") - timedelta(days=6)).strftime("%Y-%m-%d")
                    if prev_sun in current_roster and e in current_roster[prev_sun]:
                        blocked_reasons["7_day_rest"].append(e)
                        continue
                else:
                    next_sat = (datetime.strptime(current_day, "%Y-%m-%d") + timedelta(days=6)).strftime("%Y-%m-%d")
                    if next_sat in current_roster and e in current_roster[next_sat]:
                        blocked_reasons["7_day_rest"].append(e)
                        continue

            candidates.append(e)
        return candidates, blocked_reasons

    def solve(depth):
        if time.time() - start_time > timeout_seconds:
            raise TimeoutError("Algorithm timed out.")

        # DYNAMIC MRV: Find the unassigned day with the fewest valid candidates
        unassigned_days = []
        for d in weekend_dates:
            needed = settings['sat_coverage'] if day_name(d) == "Saturday" else settings['sun_coverage']
            if len(roster[d]) < needed:
                cands, reasons = get_valid_candidates(d, roster, shifts_count)
                unassigned_days.append((d, needed - len(roster[d]), cands, reasons))

        # If all days are filled, we are done!
        if not unassigned_days:
            return True

        # Sort by fewest candidates available (solve the hardest puzzle piece first)
        unassigned_days.sort(key=lambda x: len(x[2]))
        current_day, needed_now, candidates, blocked_reasons = unassigned_days[0]

        # Track bottlenecks if we are stuck
        if len(candidates) < needed_now:
            if depth > bottleneck_tracker["max_depth"]:
                bottleneck_tracker["max_depth"] = depth
                bottleneck_tracker["day"] = current_day
                bottleneck_tracker["reason"] = blocked_reasons
            return False

        is_sat = day_name(current_day) == "Saturday"

        # FAIRNESS SORTER
        def candidate_sort_key(e):
            # 1. Spread shifts evenly
            # 2. Balance Sat vs Sun
            # 3. Reward flexibility (people who gave more dates get priority)
            # 4. Respect their ranked preference
            return (
                shifts_count[e], 
                saturday_count[e] if is_sat else sunday_count[e], 
                -len(availability.get(e, [])), 
                availability[e].index(current_day),
                rng.random()
            )

        candidates.sort(key=candidate_sort_key)

        for combo in itertools.combinations(candidates, needed_now):
            # Apply assignments
            for e in combo:
                shifts_count[e] += 1
                if is_sat: saturday_count[e] += 1
                else: sunday_count[e] += 1
                roster[current_day].append(e)

            # Recurse
            if solve(depth + 1):
                return True

            # Backtrack
            for e in combo:
                shifts_count[e] -= 1
                if is_sat: saturday_count[e] -= 1
                else: sunday_count[e] -= 1
                roster[current_day].remove(e)

        return False

    if solve(0):
        final_roster = {d: roster[d] for d in weekend_dates}
        return final_roster, None
    else:
        return None, bottleneck_tracker

def generate_monthly_roster(year, month, team_id, seed=42):
    year_month = f"{year}-{month:02d}"
    conn_audit = None

    try:
        conn_audit = psycopg2.connect(**DB_PARAMS)

        audit_log(
            conn=conn_audit, source="scheduler", action="RUN_ALGORITHM_START", status="success",
            team_id=team_id, target_month=year_month, entity_type="roster_assignments",
            entity_id=year_month, details={"year": year, "month": month, "seed": seed}
        )
        conn_audit.commit()

        settings = fetch_team_settings(team_id)
        weekend_dates = get_weekend_dates(year, month)
        engineers, availability, eng_max_shifts, eng_leaves = fetch_data_from_db(year, month, weekend_dates, team_id)
        boundary_roster = fetch_boundary_roster(year, month, team_id)

        if not engineers:
            msg = "No active engineers found with submitted preferences."
            audit_log(conn=conn_audit, source="scheduler", action="RUN_ALGORITHM_FAILED", status="failed", team_id=team_id, target_month=year_month, error_message=msg)
            conn_audit.commit()
            return {"success": False, "message": msg}

        # Run the Dynamic Engine
        roster, bottleneck = draft_roster(
            availability, eng_max_shifts, eng_leaves,
            weekend_dates, settings, boundary_roster, seed, 10
        )

        if roster:
            save_roster_to_db(roster, team_id, year, month)
            total_assigned = sum(len(v) for v in roster.values())
            audit_log(
                conn=conn_audit, source="scheduler", action="RUN_ALGORITHM_SUCCESS", status="success",
                team_id=team_id, target_month=year_month, details={"total_assigned": total_assigned}
            )
            conn_audit.commit()
            return {"success": True, "message": "Roster successfully generated and saved!"}
        else:
            # Smart Error Generation based on tracked bottleneck
            bad_day = bottleneck["day"]
            reasons = bottleneck["reason"]
            msg = f"Algorithm failed to find a valid combination. <br><br><b>🚨 Critical Bottleneck: {bad_day} ({day_name(bad_day)})</b><br>"
            msg += f"Not enough engineers available. "
            
            if reasons["same_weekend"]:
                msg += f"<br>• <b>{len(reasons['same_weekend'])}</b> engineers were blocked by the 'Same Weekend' rule."
            if reasons["7_day_rest"]:
                msg += f"<br>• <b>{len(reasons['7_day_rest'])}</b> engineers were blocked by the '7-Day Rest' rule."
            if reasons["max_shifts"]:
                msg += f"<br>• <b>{len(reasons['max_shifts'])}</b> engineers maxed out their shifts."
                
            msg += "<br><br><i>Suggestion: Ask engineers to add this date, or relax the constraints in Team Settings.</i>"

            audit_log(conn=conn_audit, source="scheduler", action="RUN_ALGORITHM_FAILED", status="failed", team_id=team_id, target_month=year_month, error_message="bottleneck_hit")
            conn_audit.commit()
            return {"success": False, "message": msg}

    except Exception as e:
        if conn_audit:
            audit_log(conn=conn_audit, source="scheduler", action="RUN_ALGORITHM_FAILED", status="failed", team_id=team_id, target_month=year_month, error_message=str(e))
            conn_audit.commit()
        return {"success": False, "message": f"System Error: {str(e)}"}
    finally:
        if conn_audit:
            conn_audit.close()