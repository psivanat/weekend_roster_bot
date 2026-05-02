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

def is_shift_safe(engineer_id, shift_date, team_id):
    """Checks if assigning this date to the engineer breaks any team rules. Returns True if safe (⭐)."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Get Team Settings
    cur.execute("SELECT strict_7_day_rest, allow_same_weekend FROM teams WHERE id = %s", (team_id,))
    settings = cur.fetchone()
    if not settings:
        cur.close(); conn.close()
        return True
    strict_7_day, allow_same = settings

    # Get Engineer's current shifts
    cur.execute("SELECT shift_date FROM roster_assignments WHERE engineer_id = %s", (engineer_id,))
    current_shifts = [r[0] for r in cur.fetchall()]
    cur.close(); conn.close()

    is_sat = shift_date.weekday() == 5
    
    # 1. Same Weekend Check
    if not allow_same:
        paired_day = shift_date + timedelta(days=1) if is_sat else shift_date - timedelta(days=1)
        if paired_day in current_shifts:
            return False

    # 2. Strict 7-Day Rest Check
    if strict_7_day:
        if is_sat:
            prev_sun = shift_date - timedelta(days=6)
            if prev_sun in current_shifts: return False
        else:
            next_sat = shift_date + timedelta(days=6)
            if next_sat in current_shifts: return False

    return True

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
        super().__init__(command_keyword="submit_relief_request", help_message=None, card=None)

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
        super().__init__(command_keyword="relief_response", help_message=None, card=None)

    def execute(self, message, attachment_actions, activity):
        from bot import bot_instance, get_team_admin_emails
        
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
            # SAFETY CHECK: Is the candidate already working this exact date? (Race condition prevention)
            cur.execute("SELECT id FROM roster_assignments WHERE engineer_id = %s AND shift_date = %s", (cand_eng_id, shift_date))
            if cur.fetchone():
                cur.close(); conn.close()
                return "❌ Swap failed: You are already scheduled to work on this date."

            # 1. Update Roster
            cur.execute("UPDATE roster_assignments SET engineer_id = %s WHERE engineer_id = %s AND shift_date = %s", (cand_eng_id, req_eng_id, shift_date))
            
            # 2. Remove Requester's Preference
            cur.execute("UPDATE preferences SET priority_dates = array_remove(priority_dates, %s) WHERE engineer_id = %s AND target_month = %s", (shift_date, req_eng_id, target_month))
            
            # 3. Mark request complete & expire others
            cur.execute("UPDATE relief_requests SET status = 'completed' WHERE id = %s", (req_id,))
            cur.execute("UPDATE relief_candidates SET status = 'expired' WHERE request_id = %s AND id != %s", (req_id, cand_id))
            cur.execute("UPDATE relief_candidates SET status = 'accepted' WHERE id = %s", (cand_id,))
            
            # Fetch names for notifications
            cur.execute("SELECT name FROM teams WHERE id = %s", (team_id,))
            team_name = cur.fetchone()[0]
            
            cur.execute("SELECT name, webex_email FROM engineers WHERE id = %s", (req_eng_id,))
            req_name, req_email = cur.fetchone()
            
            cur.execute("SELECT name FROM engineers WHERE id = %s", (cand_eng_id,))
            cand_name = cur.fetchone()[0]

            conn.commit()
            bot_audit("RELIEF_ACCEPTED", team_id=team_id, entity_id=req_id, details={"accepted_by": cand_eng_id})
            
            # 4. Send Notifications via Webex
            if bot_instance:
                # Notify Requester
                if req_email:
                    try:
                        bot_instance.teams.messages.create(
                            toPersonEmail=req_email,
                            markdown=f"🎉 **Relief Request Accepted!**\n\n**{cand_name}** has agreed to cover your shift on **{shift_date.strftime('%A, %d %b %Y')}**.\nYour schedule has been automatically updated."
                        )
                    except Exception as e:
                        print(f"Failed to notify requester: {e}")

                # Notify Admins
                admin_emails = get_team_admin_emails(team_id)
                for admin_email in admin_emails:
                    try:
                        bot_instance.teams.messages.create(
                            toPersonEmail=admin_email,
                            markdown=f"✅ **Shift Relief Completed**\n\nEngineer **{cand_name}** has accepted the relief request for **{req_name}** on **{shift_date.strftime('%A, %d %b %Y')}** for your team **{team_name}**.\nThe roster has been automatically updated."
                        )
                    except Exception as e:
                        print(f"Failed to notify admin: {e}")

            cur.close(); conn.close()
            return f"✅ Thank you! The roster has been updated for your team **{team_name}** and the Admin has been notified."

        elif action == "decline":
            cur.execute("UPDATE relief_candidates SET status = 'declined' WHERE id = %s", (cand_id,))
            conn.commit()
            bot_audit("RELIEF_DECLINED", team_id=team_id, entity_id=req_id, details={"declined_by": cand_eng_id})
            escalate_relief_request(req_id)
            cur.close(); conn.close()
            return "✅ Your decline has been recorded."

        elif action == "last_resort":
            cur.execute("UPDATE relief_candidates SET status = 'last_resort' WHERE id = %s", (cand_id,))
            conn.commit()
            escalate_relief_request(req_id)
            cur.close(); conn.close()
            return "✅ We will only contact you again if no one else is available."

        cur.close(); conn.close()

def fail_relief_request(request_id, reason):
    """Handles the Supercharged Admin Alert."""
    from bot import bot_instance, get_team_admin_emails
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    # 1. Mark request as failed
    cur.execute("UPDATE relief_requests SET status = 'failed' WHERE id = %s", (request_id,))
    
    # 2. Fetch core data (Team, Date, Requester Name)
    cur.execute("""
        SELECT r.team_id, r.shift_date, r.requester_id, t.name, e.name 
        FROM relief_requests r 
        JOIN teams t ON r.team_id = t.id
        JOIN engineers e ON r.requester_id = e.id
        WHERE r.id = %s
    """, (request_id,))
    team_id, shift_date, req_id, team_name, req_name = cur.fetchone()
    target_month = f"{shift_date.year}-{shift_date.month:02d}"
    
    # 3. Fetch candidate responses
    cur.execute("""
        SELECT e.name, c.status 
        FROM relief_candidates c 
        JOIN engineers e ON c.engineer_id = e.id 
        WHERE c.request_id = %s
    """, (request_id,))
    cands = cur.fetchall()
    
    declined = [c[0] for c in cands if c[1] == 'declined']
    expired = [c[0] for c in cands if c[1] in ('expired', 'pending', 'pinged')]
    
    # 4. AI SUGGESTIONS: Find active engineers NOT in the candidate list, sorted by fewest current shifts
    cur.execute("""
        SELECT e.name, 
               (SELECT COUNT(*) FROM roster_assignments r2 WHERE r2.engineer_id = e.id AND TO_CHAR(r2.shift_date, 'YYYY-MM') = %s) as current_shifts
        FROM engineers e
        WHERE e.team_id = %s 
          AND e.id != %s 
          AND e.is_active = TRUE
          AND e.id NOT IN (SELECT engineer_id FROM relief_candidates WHERE request_id = %s)
        ORDER BY current_shifts ASC
        LIMIT 3
    """, (target_month, team_id, req_id, request_id))
    
    suggestions = cur.fetchall()
    
    conn.commit()
    cur.close()
    conn.close()
    
    bot_audit("RELIEF_FAILED", status="failed", team_id=team_id, entity_id=request_id, error_message=reason)
    
    # 5. Build the Webex Markdown Message
    msg = f"🚨 **Manual Intervention Required: Relief Request Failed**\n\n"
    msg += f"**{req_name}** requested coverage for **{shift_date.strftime('%A, %d %b %Y')}**, but the automated system could not secure a replacement for your team **{team_name}**.\n\n"
    
    msg += f"❌ **Declined:** {', '.join(declined) if declined else 'None'}\n"
    msg += f"⏳ **No Response (Cards Expired):** {', '.join(expired) if expired else 'None'}\n\n"
    
    msg += f"💡 **AI Suggestion (Engineers to contact manually):**\n"
    msg += f"*These engineers have the lowest shift counts this month and have not been asked yet:*\n"
    
    if suggestions:
        for i, (sugg_name, shift_count) in enumerate(suggestions, 1):
            msg += f"{i}. **{sugg_name}** ({shift_count} shifts this month)\n"
    else:
        msg += "*- No other active engineers available on this team.*\n"
        
    msg += f"\n*Reason for failure: {reason}*"

    # 6. Send to all Team Admins
    admin_emails = get_team_admin_emails(team_id)
    for admin_email in admin_emails:
        if bot_instance:
            try:
                bot_instance.teams.messages.create(toPersonEmail=admin_email, markdown=msg)
            except Exception as e:
                print(f"Failed to send relief failure alert to {admin_email}: {e}")

# ==========================================
# UNIFIED SHIFT SWAP (DIRECT & OPEN MARKET)
# ==========================================

class InitiateSwapCommand(Command):
    def __init__(self):
        super().__init__(command_keyword="initiate_swap", help_message=None, card=None)

    def execute(self, message, attachment_actions, activity):
        sender = activity.get("personEmail") or activity.get("actor", {}).get("emailAddress")
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("SELECT id, team_id FROM engineers WHERE webex_email = %s AND is_active = TRUE", (sender,))
        eng = cur.fetchone()
        if not eng:
            cur.close(); conn.close()
            return "⛔ Access Denied."
            
        eng_id, team_id = eng

        # Card 1: Get Alice's future shifts
        cur.execute("SELECT shift_date FROM roster_assignments WHERE engineer_id = %s AND shift_date > CURRENT_DATE ORDER BY shift_date", (eng_id,))
        my_shifts = cur.fetchall()
        cur.close(); conn.close()
        
        if not my_shifts:
            return "You have no upcoming shifts to swap."

        shift_choices = [{"title": d[0].strftime('%A, %d %b %Y'), "value": str(d[0])} for d in my_shifts]

        card = {
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.2",
                "body": [
                    {"type": "TextBlock", "text": "🔄 Shift Swap (Step 1 of 3)", "weight": "Bolder", "size": "Medium"},
                    {"type": "TextBlock", "text": "Which of your shifts do you want to give away?", "wrap": True},
                    {"type": "Input.ChoiceSet", "id": "my_shift_date", "choices": shift_choices}
                ],
                "actions": [{"type": "Action.Submit", "title": "Next ➡️", "data": {"callback_keyword": "select_swap_target"}}]
            }
        }
        r = Response()
        r.text = "Swap Step 1"
        r.attachments = card
        return r

class SelectSwapTargetCommand(Command):
    def __init__(self):
        super().__init__(command_keyword="select_swap_target", help_message=None, card=None)

    def execute(self, message, attachment_actions, activity):
        sender = activity.get("actor", {}).get("emailAddress")
        my_shift_date_str = attachment_actions.inputs.get("my_shift_date")
        if not my_shift_date_str: return "⚠️ Please select a shift."
        
        my_shift_date = datetime.strptime(my_shift_date_str, "%Y-%m-%d").date()

        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT id, team_id, webex_space_id FROM engineers e JOIN teams t ON e.team_id = t.id WHERE e.webex_email = %s", (sender,))
        eng_id, team_id, space_id = cur.fetchone()

        # Get colleagues NOT working on this date
        cur.execute("""
            SELECT id, name FROM engineers 
            WHERE team_id = %s AND id != %s AND is_active = TRUE
              AND id NOT IN (SELECT engineer_id FROM roster_assignments WHERE shift_date = %s)
            ORDER BY name
        """, (team_id, eng_id, my_shift_date))
        colleagues = cur.fetchall()
        cur.close(); conn.close()

        # Build Card 2 Choices with Pro-Tips (⭐)
        choices = []
        if space_id:
            choices.append({"title": "🌐 Anyone (Broadcast to Team Space)", "value": "OPEN_MARKET"})
            
        for c_id, c_name in colleagues:
            safe = is_shift_safe(c_id, my_shift_date, team_id)
            title = f"{c_name} ⭐ (Recommended)" if safe else c_name
            choices.append({"title": title, "value": str(c_id)})

        card = {
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.2",
                "body": [
                    {"type": "TextBlock", "text": "🔄 Shift Swap (Step 2 of 3)", "weight": "Bolder", "size": "Medium"},
                    {"type": "TextBlock", "text": f"You are giving away: **{my_shift_date_str}**", "wrap": True},
                    {"type": "TextBlock", "text": "Who do you want to ask for a swap?", "wrap": True},
                    {"type": "TextBlock", "text": "💡 Pro-Tip: Engineers marked with a ⭐ can take your shift without breaking the team's rest rules.", "wrap": True, "size": "Small", "color": "Good"},
                    {"type": "Input.ChoiceSet", "id": "target_id", "choices": choices}
                ],
                "actions": [{"type": "Action.Submit", "title": "Next ➡️", "data": {"callback_keyword": "select_return_shifts", "my_shift_date": my_shift_date_str}}]
            }
        }
        r = Response()
        r.text = "Swap Step 2"
        r.attachments = card
        return r

class SelectReturnShiftsCommand(Command):
    def __init__(self):
        super().__init__(command_keyword="select_return_shifts", help_message=None, card=None)

    def execute(self, message, attachment_actions, activity):
        sender = activity.get("actor", {}).get("emailAddress")
        inputs = attachment_actions.inputs
        my_shift_date_str = inputs.get("my_shift_date")
        target_id = inputs.get("target_id")

        if not target_id: return "⚠️ Please select a colleague or Open Market."

        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT id, team_id FROM engineers WHERE webex_email = %s", (sender,))
        eng_id, team_id = cur.fetchone()

        choices = []
        is_open = (target_id == "OPEN_MARKET")

        if is_open:
            # Branch B: Open Market (Fetch all future weekends for the next 60 days)
            today = datetime.now().date()
            for i in range(1, 60):
                d = today + timedelta(days=i)
                if d.weekday() in [5, 6] and str(d) != my_shift_date_str:
                    safe = is_shift_safe(eng_id, d, team_id)
                    title = f"{d.strftime('%A, %d %b')} ⭐" if safe else d.strftime('%A, %d %b')
                    choices.append({"title": title, "value": str(d)})
            target_name = "the Team"
        else:
            # Branch A: Direct Swap (Fetch Bob's shifts)
            cur.execute("SELECT name FROM engineers WHERE id = %s", (target_id,))
            target_name = cur.fetchone()[0]
            cur.execute("SELECT shift_date FROM roster_assignments WHERE engineer_id = %s AND shift_date > CURRENT_DATE", (target_id,))
            for (d,) in cur.fetchall():
                safe = is_shift_safe(eng_id, d, team_id)
                title = f"{d.strftime('%A, %d %b')} ⭐" if safe else d.strftime('%A, %d %b')
                choices.append({"title": title, "value": str(d)})

        cur.close(); conn.close()

        if not choices:
            return f"❌ {target_name} has no available shifts to trade."

        card = {
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.2",
                "body": [
                    {"type": "TextBlock", "text": "🔄 Shift Swap (Step 3 of 3)", "weight": "Bolder", "size": "Medium"},
                    {"type": "TextBlock", "text": f"Select one or more shifts you are willing to take from **{target_name}** in return:", "wrap": True},
                    {"type": "TextBlock", "text": "💡 Pro-Tip: Shifts marked with a ⭐ will not break your own rest rules.", "wrap": True, "size": "Small", "color": "Good"},
                    {"type": "Input.ChoiceSet", "id": "return_dates", "isMultiSelect": True, "choices": choices}
                ],
                "actions": [{"type": "Action.Submit", "title": "Send Swap Request", "data": {"callback_keyword": "submit_swap_request", "my_shift_date": my_shift_date_str, "target_id": target_id}}]
            }
        }
        r = Response()
        r.text = "Swap Step 3"
        r.attachments = card
        return r

class SubmitSwapRequestCommand(Command):
    def __init__(self):
        super().__init__(command_keyword="submit_swap_request", help_message=None, card=None)

    def execute(self, message, attachment_actions, activity):
        from bot import bot_instance
        sender = activity.get("actor", {}).get("emailAddress")
        inputs = attachment_actions.inputs
        
        my_shift_date = inputs.get("my_shift_date")
        target_id = inputs.get("target_id")
        return_dates_raw = inputs.get("return_dates")

        if not return_dates_raw: return "⚠️ You must select at least one return shift."
        return_dates = [d.strip() for d in return_dates_raw.split(",")]
        is_open = (target_id == "OPEN_MARKET")

        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT id, name, team_id FROM engineers WHERE webex_email = %s", (sender,))
        req_id, req_name, team_id = cur.fetchone()

        # Save to DB
        db_target_id = None if is_open else target_id
        cur.execute("""
            INSERT INTO shift_swaps (team_id, requester_id, target_id, requester_shift_date, acceptable_return_dates, is_open_market)
            VALUES (%s, %s, %s, %s, %s::date[], %s) RETURNING id
        """, (team_id, req_id, db_target_id, my_shift_date, return_dates, is_open))
        swap_id = cur.fetchone()[0]
        conn.commit()

        bot_audit("SWAP_REQUESTED", team_id=team_id, entity_id=swap_id, details={"is_open": is_open})

        if is_open:
            # POST TO TEAM SPACE
            cur.execute("SELECT webex_space_id FROM teams WHERE id = %s", (team_id,))
            space_id = cur.fetchone()[0]
            
            dates_str = ", ".join(return_dates)
            msg = f"📢 **Open Shift Swap!**\n\n**{req_name}** is offering their shift on **{my_shift_date}**.\nIn exchange, they are looking for a shift on: **{dates_str}**.\n\n*(To claim this, reply to the bot privately with `/claim_swap {swap_id}`)*"
            
            if bot_instance and space_id:
                bot_instance.teams.messages.create(roomId=space_id, markdown=msg)
            
            cur.close(); conn.close()
            return "✅ Your Open Market swap has been broadcasted to the Team Space!"

        else:
            # DIRECT MESSAGE TO BOB
            cur.execute("SELECT name, webex_email FROM engineers WHERE id = %s", (target_id,))
            target_name, target_email = cur.fetchone()
            cur.close(); conn.close()

            bob_choices = [{"title": d, "value": d} for d in return_dates]
            bob_card = {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard",
                    "version": "1.2",
                    "body": [
                        {"type": "TextBlock", "text": "🔄 Shift Swap Request", "weight": "Bolder", "size": "Medium", "color": "Attention"},
                        {"type": "TextBlock", "text": f"**{req_name}** wants to give you their shift on **{my_shift_date}**.", "wrap": True},
                        {"type": "TextBlock", "text": "In exchange, they will take one of these shifts from you. Which one will you give them?", "wrap": True},
                        {"type": "Input.ChoiceSet", "id": "selected_return_shift", "choices": bob_choices}
                    ],
                    "actions": [
                        {"type": "Action.Submit", "title": "✅ Accept Swap", "data": {"callback_keyword": "respond_swap", "action": "accept", "swap_id": swap_id}},
                        {"type": "Action.Submit", "title": "❌ Decline", "data": {"callback_keyword": "respond_swap", "action": "decline", "swap_id": swap_id}}
                    ]
                }
            }
            if bot_instance and target_email:
                bot_instance.teams.messages.create(toPersonEmail=target_email, attachments=[bob_card])

            return f"✅ Swap request sent to **{target_name}**. You will be notified when they respond."


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