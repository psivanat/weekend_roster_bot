import os
import psycopg2
from dotenv import load_dotenv
from datetime import datetime, timedelta
import calendar
import random
import itertools
import time
import copy

load_dotenv()
DB_PARAMS = {
    "host": os.getenv("DB_HOST", "localhost"),
    "database": os.getenv("DB_NAME", "roster_db"),
    "user": os.getenv("DB_USER", "roster_bot"),
    "password": os.getenv("DB_PASS")
}

# ==========================================
# DATABASE FUNCTIONS
# ==========================================
def fetch_team_settings(team_id):
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    cursor.execute("SELECT max_shifts, sat_coverage, sun_coverage, min_preferences, shift_start_time, shift_end_time FROM teams WHERE id = %s", (team_id,))
    res = cursor.fetchone()
    cursor.close()
    conn.close()
    return {
        "max_shifts": res[0], "sat_coverage": res[1], "sun_coverage": res[2],
        "min_preferences": res[3], "shift_start_time": res[4], "shift_end_time": res[5]
    }

def fetch_data_from_db(year, month, weekend_dates, team_id):
    year_month = f"{year}-{month:02d}"
    weekend_day_map = {int(datetime.strptime(d, "%Y-%m-%d").day): d for d in weekend_dates}
    engineers = []
    availability = {}
    
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    cursor.execute("SELECT id, name FROM engineers WHERE is_active = TRUE AND team_id = %s", (team_id,))
    active_engs = cursor.fetchall()
    
    for eng_id, name in active_engs:
        engineers.append(name)
        cursor.execute("SELECT preferences FROM availability WHERE engineer_id = %s AND year_month = %s", (eng_id, year_month))
        result = cursor.fetchone()
        if result:
            raw_prefs = result[0]
            nums = [int(x.strip()) for x in raw_prefs.split(",") if x.strip()]
            unique_dates = []
            seen = set()
            for n in nums:
                if n in weekend_day_map and n not in seen:
                    unique_dates.append(weekend_day_map[n])
                    seen.add(n)
            availability[name] = unique_dates
        else:
            availability[name] = []
            
    cursor.close()
    conn.close()
    return engineers, availability

def fetch_boundary_roster(year, month, team_id):
    """Fetches the roster for the week before and week after the current month to prevent cross-month 7-day violations."""
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    
    first_day = datetime(year, month, 1)
    last_day = datetime(year, month, calendar.monthrange(year, month)[1])
    
    start_bound = (first_day - timedelta(days=7)).strftime('%Y-%m-%d')
    end_bound = (last_day + timedelta(days=7)).strftime('%Y-%m-%d')
    
    cursor.execute("""
        SELECT shift_date, e.name 
        FROM roster_assignments r
        JOIN engineers e ON r.engineer_id = e.id
        WHERE r.team_id = %s AND r.shift_date >= %s::date AND r.shift_date <= %s::date
    """, (team_id, start_bound, end_bound))
    
    boundary_roster = {}
    for row in cursor.fetchall():
        date_str = row[0].strftime('%Y-%m-%d')
        name = row[1]
        if date_str not in boundary_roster:
            boundary_roster[date_str] = []
        boundary_roster[date_str].append(name)
        
    cursor.close()
    conn.close()
    return boundary_roster

def save_roster_to_db(roster, team_id, settings):
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    
    dates = list(roster.keys())
    if dates:
        # FIX: Added ::date[] cast to solve the UndefinedFunction error
        cursor.execute("DELETE FROM roster_assignments WHERE team_id = %s AND shift_date = ANY(%s::date[])", (team_id, dates))
        
        for date_str, assigned_names in roster.items():
            for name in assigned_names:
                cursor.execute("SELECT id FROM engineers WHERE name = %s AND team_id = %s", (name, team_id))
                eng_id = cursor.fetchone()[0]
                cursor.execute(
                    "INSERT INTO roster_assignments (shift_date, team_id, engineer_id) VALUES (%s, %s, %s)",
                    (date_str, team_id, eng_id)
                )
    conn.commit()
    cursor.close()
    conn.close()

# ==========================================
# DATE HELPERS & VALIDATION
# ==========================================
def get_weekend_dates(year, month):
    weekend_dates = []
    cal = calendar.Calendar()
    for day in cal.itermonthdates(year, month):
        if day.month == month and day.weekday() in (5, 6):
            weekend_dates.append(day.strftime('%Y-%m-%d'))
    return weekend_dates

def day_name(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d").strftime("%A")

def get_same_weekend_day(date_str):
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    if dt.weekday() == 5: return (dt + timedelta(days=1)).strftime("%Y-%m-%d")
    elif dt.weekday() == 6: return (dt - timedelta(days=1)).strftime("%Y-%m-%d")
    return None

def count_7_day_violations(roster):
    violations = 0
    for d in sorted(roster.keys()):
        if day_name(d) == "Sunday":
            next_sat = (datetime.strptime(d, "%Y-%m-%d") + timedelta(days=6)).strftime("%Y-%m-%d")
            if next_sat in roster:
                violations += len(set(roster[d]).intersection(set(roster[next_sat])))
    return violations

def validate_availability(engineers, availability, weekend_dates, settings):
    errors, warnings = [], []
    weekend_set = set(weekend_dates)
    for eng in engineers:
        if eng not in availability: errors.append(f"Missing availability for engineer: {eng}")
    for eng, dates in availability.items():
        for d in dates:
            if d not in weekend_set: errors.append(f"Non-weekend date for {eng}: {d}")
        if 0 < len(dates) < settings['min_preferences']:
            errors.append(f"Engineer '{eng}' provided {len(dates)} preferences. Minimum required is {settings['min_preferences']}.")
        elif len(dates) == 0: 
            warnings.append(f"Engineer '{eng}' has zero availability.")
    for d in weekend_dates:
        needed = settings['sat_coverage'] if day_name(d) == "Saturday" else settings['sun_coverage']
        available_count = sum(1 for e in engineers if d in availability.get(e, []))
        if available_count < needed: warnings.append(f"Low coverage on {d} ({day_name(d)}): available={available_count}, needed={needed}")
    return errors, warnings

def capacity_checks(engineers, availability, weekend_dates, settings):
    hard_errors, warnings, notes = [], [], []
    sat_dates = [d for d in weekend_dates if day_name(d) == "Saturday"]
    sun_dates = [d for d in weekend_dates if day_name(d) == "Sunday"]
    
    total_needed = len(sat_dates) * settings['sat_coverage'] + len(sun_dates) * settings['sun_coverage']
    total_capacity = len(engineers) * settings['max_shifts']
    
    if total_capacity < total_needed:
        hard_errors.append(f"Insufficient total capacity: needed={total_needed}, max_capacity={total_capacity}")

    for d in sat_dates:
        if sum(1 for e in engineers if d in availability.get(e, [])) < settings['sat_coverage']: hard_errors.append(f"Saturday {d} infeasible: needed={settings['sat_coverage']}")
    for d in sun_dates:
        if sum(1 for e in engineers if d in availability.get(e, [])) < settings['sun_coverage']: hard_errors.append(f"Sunday {d} infeasible: needed={settings['sun_coverage']}")

    for sat in sat_dates:
        sun = get_same_weekend_day(sat)
        if sun in sun_dates:
            combined_avail = set(e for e in engineers if sat in availability.get(e, [])).union(set(e for e in engineers if sun in availability.get(e, [])))
            needed_weekend = settings['sat_coverage'] + settings['sun_coverage']
            if len(combined_avail) < needed_weekend:
                hard_errors.append(f"Weekend Trap on {sat}/{sun}: Need {needed_weekend} unique engineers, but only {len(combined_avail)} are available.")
    return hard_errors, warnings, notes

# ==========================================
# ROSTER LOGIC
# ==========================================
def evaluate_solution(solution, availability):
    active_engs = [e for e, dates in availability.items() if len(dates) > 0]
    if not active_engs: return 0, 0, 0, 0
    shifts = [solution["shifts_count"][e] for e in active_engs]
    spread = max(shifts) - min(shifts)
    unbalanced_engineers = sum(1 for e in active_engs if solution["shifts_count"][e] >= 2 and (solution["saturday_count"][e] == 0 or solution["sunday_count"][e] == 0))
    seven_day_violations = count_7_day_violations(solution["roster"])
    pref_score = sum(availability[e].index(day) for day, assigned_engs in solution["roster"].items() for e in assigned_engs if day in availability[e])
    return (spread, unbalanced_engineers, seven_day_violations, pref_score)

def draft_roster(availability, weekend_dates, settings, boundary_roster, seed=42, timeout_seconds=10):
    rng = random.Random(seed)
    engineers = sorted(availability.keys())
    day_avail_counts = {d: sum(1 for e in engineers if d in availability.get(e, [])) for d in weekend_dates}
    sorted_days = sorted(weekend_dates, key=lambda d: day_avail_counts[d])
    
    # Initialize roster with boundary dates for cross-month checking
    roster = copy.deepcopy(boundary_roster)
    for d in weekend_dates:
        roster[d] = [] # Clear current month dates
        
    shifts_count = {e: 0 for e in engineers}
    saturday_count = {e: 0 for e in engineers}
    sunday_count = {e: 0 for e in engineers}
    
    start_time = time.time()
    valid_solutions = []
    
    def solve(day_idx):
        if time.time() - start_time > timeout_seconds:
            if valid_solutions: return True
            raise TimeoutError(f"Timed out after {timeout_seconds}s. No valid combinations found.")

        if day_idx == len(sorted_days):
            valid_solutions.append({
                "roster": copy.deepcopy(roster), "shifts_count": shifts_count.copy(),
                "saturday_count": saturday_count.copy(), "sunday_count": sunday_count.copy()
            })
            return len(valid_solutions) >= 50 
            
        current_day = sorted_days[day_idx]
        is_sat = day_name(current_day) == "Saturday"
        needed = settings['sat_coverage'] if is_sat else settings['sun_coverage']
        
        candidates = []
        for e in engineers:
            if current_day not in availability.get(e, []) or shifts_count[e] >= settings['max_shifts']: continue
            same_weekend_day = get_same_weekend_day(current_day)
            if same_weekend_day in roster and e in roster[same_weekend_day]: continue
            candidates.append(e)

        def candidate_sort_key(e):
            causes_7_day = 0
            if is_sat:
                prev_sun = (datetime.strptime(current_day, "%Y-%m-%d") - timedelta(days=6)).strftime("%Y-%m-%d")
                if prev_sun in roster and e in roster[prev_sun]: causes_7_day = 1
            else:
                next_sat = (datetime.strptime(current_day, "%Y-%m-%d") + timedelta(days=6)).strftime("%Y-%m-%d")
                if next_sat in roster and e in roster[next_sat]: causes_7_day = 1
            
            return (shifts_count[e], 1 if (saturday_count[e] > 0 if is_sat else sunday_count[e] > 0) else 0, 
                    causes_7_day, availability[e].index(current_day), len(availability.get(e, [])), rng.random())
            
        candidates.sort(key=candidate_sort_key)
        for combo in itertools.combinations(candidates, needed):
            for e in combo:
                shifts_count[e] += 1
                if is_sat: saturday_count[e] += 1
                else: sunday_count[e] += 1
                roster[current_day].append(e)
                
            if solve(day_idx + 1): return True
                
            for e in combo:
                shifts_count[e] -= 1
                if is_sat: saturday_count[e] -= 1
                else: sunday_count[e] -= 1
                roster[current_day].remove(e)
        return False

    solve(0)
    if not valid_solutions: raise ValueError("Unable to find a valid roster satisfying all constraints.")
        
    valid_solutions.sort(key=lambda sol: evaluate_solution(sol, availability))
    best_solution = valid_solutions[0]
    
    # Filter out the boundary dates before returning so we only save the current month
    final_roster = {d: best_solution["roster"][d] for d in weekend_dates}
    
    total_shifts = sum(best_solution["shifts_count"].values())
    avg_pref = evaluate_solution(best_solution, availability)[3] / total_shifts if total_shifts > 0 else 0
        
    return final_roster, {}, avg_pref

# ==========================================
# STREAMLIT INTEGRATION FUNCTION
# ==========================================
def run_algorithm_for_month(year, month, team_id, seed=42):
    settings = fetch_team_settings(team_id)
    weekend_dates = get_weekend_dates(year, month)
    engineers, availability = fetch_data_from_db(year, month, weekend_dates, team_id)
    
    # Fetch boundary dates to prevent cross-month 7-day violations
    boundary_roster = fetch_boundary_roster(year, month, team_id)

    if not engineers: return False, "No active engineers found for this team."

    errors, warnings_val = validate_availability(engineers, availability, weekend_dates, settings)
    hard_errors, warnings_cap, notes = capacity_checks(engineers, availability, weekend_dates, settings)
    
    if errors or hard_errors:
        return False, f"Validation Failed:\n" + "\n".join(errors + hard_errors)

    try:
        roster, stats, avg_pref = draft_roster(availability, weekend_dates, settings, boundary_roster, seed, 10)
        save_roster_to_db(roster, team_id, settings)
        return True, f"Roster successfully generated! Avg Preference Score: {avg_pref:.2f}"
    except (ValueError, TimeoutError) as ex:
        return False, f"Generation failed: {ex}"
