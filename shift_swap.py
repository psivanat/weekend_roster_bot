import os
import psycopg2
from datetime import datetime, timedelta, timezone
from webex_bot.models.command import Command
from webex_bot.models.response import Response
from audit_logger import audit_log
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Use your existing DB config
DB_PARAMS = {
    "host": os.getenv("DB_HOST", "localhost"),
    "database": os.getenv("DB_NAME", "roster_db"),
    "user": os.getenv("DB_USER", "roster_bot"),
    "password": os.getenv("DB_PASS")
}

IST = timezone(timedelta(hours=5, minutes=30))

def get_db_connection():
    return psycopg2.connect(**DB_PARAMS)

def bot_audit(action, status="success", team_id=None, target_month=None, entity_type=None, entity_id=None, details=None, error_message=None):
    conn = None
    try:
        conn = get_db_connection()
        audit_log(conn, "webex_bot", action, status, team_id, target_month, entity_type, entity_id, details, error_message)
        conn.commit()
    except Exception as e:
        print(f"[AUDIT_FAIL] {e}")
    finally:
        if conn: conn.close()

def get_team_admins_text(team_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT u.name FROM users u
        JOIN user_teams ut ON u.id = ut.user_id
        WHERE ut.team_id = %s AND u.role IN ('team_admin', 'super_admin')
    """, (team_id,))
    admins = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.close()
    if not admins: return "your Admin"
    return " or ".join(admins)

def get_friday_deadline(shift_date, shift_end_time):
    # Find the Friday immediately preceding the shift_date
    days_to_subtract = (shift_date.weekday() - 4) % 7
    if days_to_subtract == 0: days_to_subtract = 7 # If shift is Friday, get previous Friday
    friday_date = shift_date - timedelta(days=days_to_subtract)
    
    # Combine with shift end time and subtract 2 hours
    if not shift_end_time:
        shift_end_time = datetime.strptime("17:00", "%H:%M").time() # Fallback to 5 PM
    
    friday_dt = datetime.combine(friday_date, shift_end_time).replace(tzinfo=IST)
    return friday_dt - timedelta(hours=2)

def is_within_shift_hours(team_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT shift_start_time, shift_end_time FROM teams WHERE id = %s", (team_id,))
    res = cur.fetchone()
    cur.close()
    conn.close()
    
    if not res or not res[0] or not res[1]: return True # Fallback if not configured
    
    now_time = datetime.now(IST).time()
    return res[0] <= now_time <= res[1]

# ==========================================
# 1. INITIATE RELIEF COMMAND
# ==========================================
class UnableToWorkCommand(Command):
    def __init__(self):
        super().__init__(command_keyword="unable_to_work", help_message="Request automated shift relief.", card=None)

    def execute(self, message, attachment_actions, activity):
        sender = activity.get("personEmail") or activity.get("actor", {}).get("emailAddress")
        
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT id, name, team_id FROM engineers WHERE webex_email = %s AND is_active = TRUE", (sender,))
        eng = cur.fetchone()
        
        if not eng:
            cur.close(); conn.close()
            return "⛔ Access Denied."
            
        eng_id, name, team_id = eng
        
        if not is_within_shift_hours(team_id):
            cur.close(); conn.close()
            return "❌ Please initiate relief requests during active shift hours."

        cur.execute("SELECT shift_date FROM roster_assignments WHERE engineer_id = %s AND shift_date >= CURRENT_DATE ORDER BY shift_date", (eng_id,))
        shifts = cur.fetchall()
        cur.close(); conn.close()

        if not shifts:
            return "You have no upcoming shifts to request relief for."

        choices = [{"title": d[0].strftime('%A, %d %b %Y'), "value": str(d[0])} for d in shifts]

        card = {
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.2",
                "body": [
                    {"type": "TextBlock", "text": "🚨 Request Shift Relief", "weight": "Bolder", "size": "Medium"},
                    {"type": "TextBlock", "text": "Select the shift you are unable to work. The automated system will attempt to find a replacement.", "wrap": True},
                    {
                        "type": "Input.ChoiceSet",
                        "id": "shift_date",
                        "choices": choices,
                        "placeholder": "Select a shift..."
                    }
                ],
                "actions": [
                    {"type": "Action.Submit", "title": "Submit Request", "data": {"callback_keyword": "submit_relief_request"}}
                ]
            }
        }
        r = Response()
        r.text = "Relief Request"
        r.attachments = card
        return r

# ==========================================
# 2. PROCESS RELIEF REQUEST & BUILD QUEUE
# ==========================================
class SubmitReliefRequestCommand(Command):
    def __init__(self):
        super().__init__(command_keyword="submit_relief_request", help_message="Process relief", card=None)

    def execute(self, message, attachment_actions, activity):
        sender = activity.get("actor", {}).get("emailAddress")
        shift_date_str = attachment_actions.inputs.get("shift_date")
        if not shift_date_str: return "⚠️ Please select a date."
        
        shift_date = datetime.strptime(shift_date_str, "%Y-%m-%d").date()
        target_month = f"{shift_date.year}-{shift_date.month:02d}"

        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT id, team_id FROM engineers WHERE webex_email = %s", (sender,))
        eng_id, team_id = cur.fetchone()

        # Gate: Friday Deadline
        cur.execute("SELECT shift_end_time FROM teams WHERE id = %s", (team_id,))
        shift_end = cur.fetchone()[0]
        deadline = get_friday_deadline(shift_date, shift_end)
        
        if datetime.now(IST) > deadline:
            admins = get_team_admins_text(team_id)
            cur.close(); conn.close()
            return f"❌ The deadline to request automated relief for this weekend has passed. Please contact your Admin directly: **{admins}**."

        # Create Request
        cur.execute("INSERT INTO relief_requests (team_id, requester_id, shift_date) VALUES (%s, %s, %s) RETURNING id", (team_id, eng_id, shift_date))
        request_id = cur.fetchone()[0]

        # Build Queue (Simplified for brevity: In production, apply your 4 Tiers here)
        # For now, we fetch all active engineers who preferred this date, excluding requester
        cur.execute("""
            SELECT e.id FROM engineers e
            JOIN preferences p ON e.id = p.engineer_id
            WHERE e.team_id = %s AND e.id != %s AND e.is_active = TRUE
              AND p.target_month = %s AND %s = ANY(p.priority_dates)
        """, (team_id, eng_id, target_month, shift_date))
        
        candidates = [r[0] for r in cur.fetchall()]
        
        for c_id in candidates:
            cur.execute("INSERT INTO relief_candidates (request_id, engineer_id, tier) VALUES (%s, %s, 1)", (request_id, c_id))

        conn.commit()
        cur.close(); conn.close()

        bot_audit("RELIEF_REQUEST_STARTED", team_id=team_id, entity_type="relief_requests", entity_id=request_id, details={"shift_date": shift_date_str})
        
        # Trigger the first dispatch
        escalate_relief_request(request_id)
        
        return "✅ Relief request initiated. The system is contacting available engineers. You will be notified of the outcome."

def escalate_relief_request(request_id):
    """Finds the next pending candidate and sends them a card."""
    from bot import bot_instance # Import your bot instance
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Get request details
    cur.execute("""
        SELECT r.shift_date, e.name, r.team_id 
        FROM relief_requests r JOIN engineers e ON r.requester_id = e.id 
        WHERE r.id = %s AND r.status = 'active'
    """, (request_id,))
    req = cur.fetchone()
    if not req: return # Already completed/failed
    shift_date, req_name, team_id = req

    # Find next pending candidate
    cur.execute("""
        SELECT c.id, e.webex_email, e.name 
        FROM relief_candidates c JOIN engineers e ON c.engineer_id = e.id
        WHERE c.request_id = %s AND c.status = 'pending' ORDER BY c.tier, c.id LIMIT 1
    """, (request_id,))
    cand = cur.fetchone()

    if cand:
        cand_id, email, cand_name = cand
        cur.execute("UPDATE relief_candidates SET dispatched_at = CURRENT_TIMESTAMP WHERE id = %s", (cand_id,))
        conn.commit()
        
        # Send Card
        card = {
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.2",
                "body": [
                    {"type": "TextBlock", "text": "🚨 Shift Coverage Needed", "weight": "Bolder", "color": "Attention"},
                    {"type": "TextBlock", "text": f"**{req_name}** is unable to work on **{shift_date.strftime('%A, %d %b')}**.", "wrap": True},
                    {"type": "TextBlock", "text": "You are eligible to cover this shift. Can you help?", "wrap": True}
                ],
                "actions": [
                    {"type": "Action.Submit", "title": "✅ Accept", "data": {"callback_keyword": "relief_response", "action": "accept", "cand_id": cand_id}},
                    {"type": "Action.Submit", "title": "❌ Decline", "data": {"callback_keyword": "relief_response", "action": "decline", "cand_id": cand_id}},
                    {"type": "Action.Submit", "title": "⏳ Last Resort", "data": {"callback_keyword": "relief_response", "action": "last_resort", "cand_id": cand_id}}
                ]
            }
        }
        if bot_instance and email:
            bot_instance.teams.messages.create(toPersonEmail=email, attachments=[card])
            bot_audit("RELIEF_DISPATCHED", team_id=team_id, entity_id=request_id, details={"sent_to": cand_name})
            
    else:
        # No pending candidates left. Check Last Resorts.
        cur.execute("SELECT id FROM relief_candidates WHERE request_id = %s AND status = 'last_resort'", (request_id,))
        if cur.fetchone():
            # Reset last resorts to pending and re-escalate
            cur.execute("UPDATE relief_candidates SET status = 'pending' WHERE request_id = %s AND status = 'last_resort'", (request_id,))
            conn.commit()
            escalate_relief_request(request_id)
        else:
            # Total Failure
            fail_relief_request(request_id, "All candidates declined or timed out.")
            
    cur.close(); conn.close()

# ==========================================
# 3. HANDLE CANDIDATE RESPONSES
# ==========================================
class ReliefResponseCommand(Command):
    def __init__(self):
        super().__init__(command_keyword="relief_response", help_message="Handle relief response", card=None)

    def execute(self, message, attachment_actions, activity):
        action = attachment_actions.inputs.get("action")
        cand_id = attachment_actions.inputs.get("cand_id")
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Check if request is still active
        cur.execute("""
            SELECT r.id, r.requester_id, r.shift_date, r.team_id, c.engineer_id, r.status
            FROM relief_candidates c JOIN relief_requests r ON c.request_id = r.id
            WHERE c.id = %s
        """, (cand_id,))
        req = cur.fetchone()
        
        if not req or req[5] != 'active':
            cur.close(); conn.close()
            return "ℹ️ Thank you, but this request has already been resolved or expired."

        req_id, req_eng_id, shift_date, team_id, cand_eng_id, _ = req
        target_month = f"{shift_date.year}-{shift_date.month:02d}"

        if action == "accept":
            # 1. Update Roster
            cur.execute("UPDATE roster_assignments SET engineer_id = %s WHERE engineer_id = %s AND shift_date = %s", (cand_eng_id, req_eng_id, shift_date))
            
            # 2. Remove Requester's Preference
            cur.execute("UPDATE preferences SET priority_dates = array_remove(priority_dates, %s) WHERE engineer_id = %s AND target_month = %s", (shift_date, req_eng_id, target_month))
            
            # 3. Mark request complete & expire others
            cur.execute("UPDATE relief_requests SET status = 'completed' WHERE id = %s", (req_id,))
            cur.execute("UPDATE relief_candidates SET status = 'expired' WHERE request_id = %s AND id != %s", (req_id, cand_id))
            cur.execute("UPDATE relief_candidates SET status = 'accepted' WHERE id = %s", (cand_id,))
            
            conn.commit()
            bot_audit("RELIEF_ACCEPTED", team_id=team_id, entity_id=req_id, details={"accepted_by": cand_eng_id})
            
            # Notify Admin & Requester (pseudo-code, use bot_instance to send messages here)
            return "✅ Thank you! The roster has been updated and the Admin has been notified."

        elif action == "decline":
            cur.execute("UPDATE relief_candidates SET status = 'declined' WHERE id = %s", (cand_id,))
            conn.commit()
            bot_audit("RELIEF_DECLINED", team_id=team_id, entity_id=req_id, details={"declined_by": cand_eng_id})
            escalate_relief_request(req_id)
            return "✅ Your decline has been recorded."

        elif action == "last_resort":
            cur.execute("UPDATE relief_candidates SET status = 'last_resort' WHERE id = %s", (cand_id,))
            conn.commit()
            escalate_relief_request(req_id)
            return "✅ We will only contact you again if no one else is available."

        cur.close(); conn.close()

def fail_relief_request(request_id, reason):
    """Handles the Supercharged Admin Alert."""
    from bot import bot_instance
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("UPDATE relief_requests SET status = 'failed' WHERE id = %s", (request_id,))
    
    # Fetch data for the Admin alert
    cur.execute("SELECT team_id, shift_date, requester_id FROM relief_requests WHERE id = %s", (request_id,))
    team_id, shift_date, req_id = cur.fetchone()
    
    cur.execute("SELECT e.name, c.status FROM relief_candidates c JOIN engineers e ON c.engineer_id = e.id WHERE c.request_id = %s", (request_id,))
    cands = cur.fetchall()
    
    declined = [c[0] for c in cands if c[1] == 'declined']
    expired = [c[0] for c in cands if c[1] == 'expired' or c[1] == 'pending']
    
    conn.commit()
    cur.close(); conn.close()
    
    bot_audit("RELIEF_FAILED", status="failed", team_id=team_id, entity_id=request_id, error_message=reason)
    
    # Send message to Admin via bot_instance...
    print(f"🚨 ALERT ADMIN: Relief failed for {shift_date}. Declined: {declined}. No Response: {expired}.")

# ==========================================
# 4. BACKGROUND TIMER JOB (Run every 5 mins)
# ==========================================
def tick_relief_timers():
    """Called by APScheduler every 5 minutes."""
    print("[SCHEDULER] Ticking relief timers...")
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("SELECT id, team_id, shift_date FROM relief_requests WHERE status = 'active'")
    active_requests = cur.fetchall()
    
    for req_id, team_id, shift_date in active_requests:
        # Check Friday Deadline
        cur.execute("SELECT shift_end_time FROM teams WHERE id = %s", (team_id,))
        shift_end = cur.fetchone()[0]
        deadline = get_friday_deadline(shift_date, shift_end)
        
        if datetime.now(IST) > deadline:
            cur.execute("UPDATE relief_candidates SET status = 'expired' WHERE request_id = %s AND status IN ('pending', 'pinged')", (req_id,))
            conn.commit()
            fail_relief_request(req_id, "Friday deadline reached.")
            continue

        # Add active minutes if within shift hours
        if is_within_shift_hours(team_id):
            cur.execute("UPDATE relief_candidates SET active_minutes = active_minutes + 5 WHERE request_id = %s AND status IN ('pending', 'pinged')", (req_id,))
            conn.commit()
            
            # Check for Pings (60 mins)
            cur.execute("SELECT id FROM relief_candidates WHERE request_id = %s AND status = 'pending' AND active_minutes >= 60", (req_id,))
            for (cand_id,) in cur.fetchall():
                cur.execute("UPDATE relief_candidates SET status = 'pinged' WHERE id = %s", (cand_id,))
                conn.commit()
                # Send ping via bot_instance...
                
            # Check for Escalations (120 mins)
            cur.execute("SELECT id FROM relief_candidates WHERE request_id = %s AND status = 'pinged' AND active_minutes >= 120", (req_id,))
            for (cand_id,) in cur.fetchall():
                # We leave their status as 'pinged' so their card stays open, but we trigger the next dispatch
                escalate_relief_request(req_id)

    cur.close(); conn.close()