import os
import calendar
import psycopg2
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler
from webex_bot.webex_bot import WebexBot
from webex_bot.models.command import Command
from webex_bot.models.response import Response
from shift_swap import UnableToWorkCommand, SubmitReliefRequestCommand, ReliefResponseCommand, tick_relief_timers
from audit_logger import audit_log

load_dotenv()

# Proxy for Cisco network
os.environ["HTTP_PROXY"] = "http://proxy-wsa.esl.cisco.com:80"
os.environ["HTTPS_PROXY"] = "http://proxy-wsa.esl.cisco.com:80"

bot_instance = None

DB_PARAMS = {
    "host": os.getenv("DB_HOST", "localhost"),
    "database": os.getenv("DB_NAME", "roster_db"),
    "user": os.getenv("DB_USER", "roster_bot"),
    "password": os.getenv("DB_PASS")
}

def get_upcoming_weekend():
    now = datetime.now()
    days_ahead_sat = 5 - now.weekday()
    if days_ahead_sat < 0:
        days_ahead_sat += 7
    next_sat = now + timedelta(days=days_ahead_sat)
    next_sun = next_sat + timedelta(days=1)
    return next_sat.date(), next_sun.date()

def is_user_admin(email, team_id):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT u.role FROM users u
            JOIN user_teams ut ON u.id = ut.user_id
            WHERE u.email = %s AND ut.team_id = %s
        """, (email, team_id))
        row = cur.fetchone()
        cur.close()
        conn.close()
        return row and row[0] in ['team_admin', 'super_admin']
    except Exception:
        return False

def bot_audit(action, status="success", team_id=None, target_month=None, entity_type=None,
              entity_id=None, details=None, error_message=None):
    conn = None
    try:
        conn = get_db_connection()
        audit_log(
            conn=conn,
            source="webex_bot",
            action=action,
            status=status,
            team_id=team_id,
            target_month=target_month,
            entity_type=entity_type,
            entity_id=entity_id,
            details=details,
            error_message=error_message
        )
        conn.commit()
    except Exception as ex:
        print(f"[AUDIT_FAIL] {action}: {ex}")
    finally:
        if conn:
            conn.close()

def get_triggered_team_ids(target_month):
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT team_id
            FROM preference_requests
            WHERE target_month = %s
        """, (target_month,))
        return [r[0] for r in cur.fetchall()]
    except Exception as ex:
        print(f"DB error get_triggered_team_ids: {ex}")
        return []
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def send_friday_shift_reminders():
    print("[SCHEDULER] Checking Friday shift reminders...")
    
    # Set timezone to UTC+5:30 (IST)
    IST = timezone(timedelta(hours=5, minutes=30))
    now_ist = datetime.now(IST)

    # 1. Only proceed if today is Friday (weekday() == 4)
    if now_ist.weekday() != 4:
        return

    # Calculate this weekend's dates
    tomorrow_sat = now_ist.date() + timedelta(days=1)
    sunday = now_ist.date() + timedelta(days=2)
    weekend_dates = [tomorrow_sat, sunday]

    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # 2. Get all teams that have a shift_end_time configured
        cur.execute("SELECT id, name, shift_end_time FROM teams WHERE shift_end_time IS NOT NULL")
        teams = cur.fetchall()

        for team_id, team_name, shift_end in teams:
            # Calculate target reminder time (shift_end_time - 2 hours)
            dummy_dt = datetime.combine(now_ist.date(), shift_end)
            reminder_dt = dummy_dt - timedelta(hours=2)
            reminder_time = reminder_dt.time()

            # 3. If current IST time hasn't reached the reminder time yet, skip this team
            if now_ist.time() < reminder_time:
                continue

            # 4. Fetch engineers assigned to shifts this weekend for this team
            cur.execute("""
                SELECT r.engineer_id, e.name, e.webex_email, r.shift_date
                FROM roster_assignments r
                JOIN engineers e ON r.engineer_id = e.id
                WHERE r.team_id = %s AND r.shift_date = ANY(%s::date[])
            """, (team_id, weekend_dates))
            assignments = cur.fetchall()

            for eng_id, eng_name, email, shift_date in assignments:
                if not email:
                    continue

                # 5. Check deduplication table to ensure we don't spam them
                cur.execute("""
                    SELECT id FROM reminder_dispatches
                    WHERE team_id = %s AND engineer_id = %s AND shift_date = %s AND reminder_type = 'WEEKEND_T_MINUS_2H'
                """, (team_id, eng_id, shift_date))
                
                if cur.fetchone():
                    continue  # Already sent this reminder

                # 6. Send Webex Message
                status = 'sent'
                error_msg = None
                try:
                    msg = (
                        f"🔔 **Upcoming Weekend Shift Reminder**\n\n"
                        f"Hi {eng_name},\n"
                        f"This is an automated reminder that you are scheduled for a shift on **{shift_date.strftime('%A, %d %b %Y')}**.\n\n"
                        f"**Team:** {team_name}"
                    )
                    if bot_instance:
                        bot_instance.teams.messages.create(toPersonEmail=email, markdown=msg)
                    print(f"Friday reminder sent to {eng_name} for {shift_date}")
                except Exception as e:
                    status = 'failed'
                    error_msg = str(e)
                    print(f"Failed to send Friday reminder to {eng_name}: {e}")

                # 7. Log to reminder_dispatches so it doesn't send again
                cur.execute("""
                    INSERT INTO reminder_dispatches (team_id, engineer_id, shift_date, reminder_type, status, error_message)
                    VALUES (%s, %s, %s, 'WEEKEND_T_MINUS_2H', %s, %s)
                """, (team_id, eng_id, shift_date, status, error_msg))
                conn.commit()

                # 8. Write to main Audit Log
                bot_audit(
                    action="WEEKEND_SHIFT_REMINDER",
                    status=status,
                    team_id=team_id,
                    entity_type="roster_assignments",
                    entity_id=eng_id,
                    details={"shift_date": str(shift_date), "email": email},
                    error_message=error_msg
                )

    except Exception as ex:
        print(f"Error in send_friday_shift_reminders: {ex}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def get_pending_engineers_for_triggered_teams(target_month):
    pending_all = []
    team_ids = get_triggered_team_ids(target_month)
    for team_id in team_ids:
        rows = get_pending_engineers(target_month, team_id=team_id)
        pending_all.extend(rows)
    return pending_all

def get_db_connection():
    return psycopg2.connect(**DB_PARAMS)

def get_next_month_info():
    now = datetime.now()
    next_m = now.month + 1 if now.month < 12 else 1
    next_y = now.year if now.month < 12 else now.year + 1
    target_month = f"{next_y}-{next_m:02d}"
    display = datetime(next_y, next_m, 1).strftime("%B %Y")
    return next_m, next_y, target_month, display

def get_weekend_dates(year, month):
    cal = calendar.Calendar()
    return [d for d in cal.itermonthdates(year, month) if d.month == month and d.weekday() in [5, 6]]

def get_engineer_by_email(email):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, name, team_id
            FROM engineers
            WHERE webex_email = %s AND is_active = true
        """, (email,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        return row  # (id, name, team_id)
    except Exception as e:
        print(f"DB error get_engineer_by_email: {e}")
        return None

def get_team_name(team_id):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT name FROM teams WHERE id = %s", (team_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        return row[0] if row else f"Team {team_id}"
    except Exception:
        return f"Team {team_id}"

def get_team_admin_emails(team_id):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT u.email
            FROM users u
            JOIN user_teams ut ON u.id = ut.user_id
            WHERE ut.team_id = %s
              AND u.role IN ('team_admin', 'super_admin')
              AND u.email IS NOT NULL
        """, (team_id,))
        emails = [r[0] for r in cur.fetchall()]
        cur.close()
        conn.close()
        return emails
    except Exception as e:
        print(f"DB error get_team_admin_emails: {e}")
        return []

def get_pending_engineers(target_month, team_id=None):
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        if team_id is None:
            cur.execute("""
                SELECT e.id, e.name, e.webex_email, e.team_id
                FROM engineers e
                WHERE e.is_active = true
                  AND e.id NOT IN (
                      SELECT p.engineer_id
                      FROM preferences p
                      WHERE p.target_month = %s
                        AND p.status IN ('submitted', 'opted_out')
                  )
            """, (target_month,))
        else:
            cur.execute("""
                SELECT e.id, e.name, e.webex_email, e.team_id
                FROM engineers e
                WHERE e.is_active = true
                  AND e.team_id = %s
                  AND e.id NOT IN (
                      SELECT p.engineer_id
                      FROM preferences p
                      WHERE p.target_month = %s
                        AND p.status IN ('submitted', 'opted_out')
                  )
            """, (team_id, target_month))

        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        print(f"DB error get_pending_engineers: {e}")
        return []

def check_all_complete_and_notify(target_month, team_id):
    pending = get_pending_engineers(target_month, team_id=team_id)
    if len(pending) > 0:
        return

    if not bot_instance:
        print("bot_instance not initialized; cannot send all-complete notification")
        return

    team_name = get_team_name(team_id)
    admin_emails = get_team_admin_emails(team_id)
    for admin_email in admin_emails:
        try:
            bot_instance.teams.messages.create(
                toPersonEmail=admin_email,
                markdown=(
                    f"🎉 **All Preferences Collected**\n\n"
                    f"**{team_name}** has completed preference submission for **{target_month}**.\n"
                    f"You can now go to the dashboard and run roster generation."
                )
            )
            print(f"All-complete sent to {admin_email} for team {team_id}")
        except Exception as e:
            print(f"Error sending all-complete to {admin_email}: {e}")

def build_step1_card(month_display, team_name=None):
    title = f"📅 Preferences for {month_display}" if not team_name else f"📅 {team_name} — Preferences for {month_display}"
    return {
        "contentType": "application/vnd.microsoft.card.adaptive",
        "content": {
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "type": "AdaptiveCard",
            "version": "1.2",
            "body": [
                {"type": "TextBlock", "text": title, "weight": "Bolder", "size": "Medium"},
                {"type": "TextBlock", "text": "How many weekend shifts do you prefer this month?", "wrap": True},
                {
                    "type": "Input.ChoiceSet",
                    "id": "preferred_count",
                    "style": "compact",
                    "value": "2",
                    "choices": [
                        {"title": "1 Shift", "value": "1"},
                        {"title": "2 Shifts", "value": "2"},
                        {"title": "3 Shifts", "value": "3"},
                        {"title": "4 Shifts", "value": "4"}
                    ]
                }
            ],
            "actions": [
                {"type": "Action.Submit", "title": "Next ➡️", "data": {"callback_keyword": "step2_preferences"}},
                {"type": "Action.Submit", "title": "🏖️ Opt-Out (Unavailable)", "data": {"callback_keyword": "opt_out_preferences"}}
            ]
        }
    }

class HelloCommand(Command):
    def __init__(self):
        super().__init__(command_keyword="hi", help_message="Show the main interactive menu.", card=None)
        self.aliases = ["hello", "help", "menu"]

    def execute(self, message, attachment_actions, activity):
        sender = activity.get("personEmail") or activity.get("actor", {}).get("emailAddress")
        eng = get_engineer_by_email(sender)
        if not eng:
            return "⛔ Access Denied: You are not registered as an active engineer."

        engineer_id, name, team_id = eng
        _, _, target_month, display_month = get_next_month_info()
        is_admin = is_user_admin(sender, team_id)

        bot_audit("BOT_MENU_OPEN", team_id=team_id, entity_type="engineer", entity_id=engineer_id, details={"email": sender})

        # Build the Adaptive Card
        body = [
            {"type": "TextBlock", "text": f"👋 Welcome back, {name}!", "weight": "Bolder", "size": "Large"},
            {"type": "TextBlock", "text": "What would you like to do today?", "wrap": True},
            {"type": "TextBlock", "text": "📅 My Schedule", "weight": "Bolder", "spacing": "Medium", "color": "Accent"}
        ]

        actions = [
            {"type": "Action.Submit", "title": f"📝 Update Preferences ({display_month})", "data": {"callback_keyword": "step1_preferences"}},
            {"type": "Action.Submit", "title": "🔍 View My Submitted Preferences", "data": {"callback_keyword": "my_preferences"}},
            {"type": "Action.Submit", "title": "📆 View My Upcoming Shifts", "data": {"callback_keyword": "my_shifts"}},
            {"type": "Action.Submit", "title": "🚨 Unable to Work (Request Relief)", "data": {"callback_keyword": "unable_to_work"}}
        ]

        body.append({"type": "ActionSet", "actions": actions})
        body.append({"type": "TextBlock", "text": "👥 Team Roster", "weight": "Bolder", "spacing": "Medium", "color": "Accent"})
        body.append({
            "type": "ActionSet", 
            "actions": [{"type": "Action.Submit", "title": "👀 Who is working this weekend?", "data": {"callback_keyword": "who_is_working"}}]
        })

        # Admin Section
        if is_admin:
            body.append({"type": "TextBlock", "text": "⚙️ Admin Tools", "weight": "Bolder", "spacing": "Medium", "color": "Attention"})
            body.append({
                "type": "ActionSet", 
                "actions": [{"type": "Action.Submit", "title": "⏳ Check Pending Submissions", "data": {"callback_keyword": "pending_status"}}]
            })

        card = {
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.2",
                "body": body
            }
        }

        r = Response()
        r.text = "Main Menu"
        r.attachments = card
        return r

class StatusCommand(Command):
    def __init__(self):
        super().__init__(command_keyword="status", help_message="Bot status", card=None)

    def execute(self, message, attachment_actions, activity):
        bot_audit("BOT_STATUS_CHECK", details={"message": "status command"})
        return "✅ Roster Bot is online via WebSockets."

class Step1PreferencesCommand(Command):
    def __init__(self):
        super().__init__(command_keyword="step1_preferences", help_message="Step 1", card=None)
        self.card_callback_keyword = "step1_preferences"

    def execute(self, message, attachment_actions, activity):
        sender = activity.get("personEmail") or activity.get("actor", {}).get("emailAddress")
        eng = get_engineer_by_email(sender)
        team_name = get_team_name(eng[2]) if eng else None

        _, _, _, display = get_next_month_info()
        r = Response()
        r.text = "Preference Step 1"
        r.attachments = build_step1_card(display, team_name=team_name)
        return r

class Step2PreferencesCommand(Command):
    def __init__(self):
        super().__init__(command_keyword="step2_preferences", help_message="Step 2", card=None)
        self.card_callback_keyword = "step2_preferences"

    def execute(self, message, attachment_actions, activity):
        inputs = attachment_actions.inputs if attachment_actions else {}
        preferred_count = int(inputs.get("preferred_count", 2))
        min_required = preferred_count + 2

        # Use target_month from card if present, else fallback to next month
        target_month = inputs.get("target_month")
        if target_month:
            y, m = map(int, target_month.split("-"))
        else:
            m, y, target_month, _ = get_next_month_info()

        weekends = get_weekend_dates(y, m)
        min_required = min(min_required, len(weekends))

        # Add dummy blank option to prevent auto-select in some Webex clients
        choices = [{"title": "-- Select a date --", "value": ""}] + [
            {"title": d.strftime("%A, %d-%m-%Y"), "value": d.strftime("%Y-%m-%d")}
            for d in weekends
        ]

        body = [
            {"type": "TextBlock", "text": "📅 Step 2: Rank Date Preferences", "weight": "Bolder", "size": "Medium"},
            {"type": "TextBlock", "text": f"Minimum required: {min_required} dates (n + 2).", "wrap": True},
            {"type": "TextBlock", "text": "⚠️ Do not select the same date twice.", "wrap": True, "color": "Attention"}
        ]

        for i in range(1, len(weekends) + 1):
            req = i <= min_required
            body.append({"type": "TextBlock", "text": f"Priority {i} {'(Required)' if req else '(Optional)'}"})
            body.append({
                "type": "Input.ChoiceSet",
                "id": f"priority_{i}",
                "style": "compact",
                "choices": choices,
                "value": "",  # force blank
                "placeholder": "Select a date...",
                "isRequired": req,
                "errorMessage": f"Priority {i} is required."
            })

        card = {
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.2",
                "body": body,
                "actions": [
                    {
                        "type": "Action.Submit",
                        "title": "✅ Submit Preferences",
                        "data": {
                            "callback_keyword": "save_preferences",
                            "target_month": target_month,
                            "preferred_count": preferred_count
                        }
                    }
                ]
            }
        }

        # --- AUDIT LOGGING (Safe placement after variables are defined) ---
        sender = activity.get("actor", {}).get("emailAddress") or activity.get("personEmail")
        eng = get_engineer_by_email(sender)
        if eng:
            bot_audit(
                action="BOT_PREF_STEP2_OPEN", 
                team_id=eng[2], 
                target_month=target_month,
                entity_type="preferences", 
                entity_id=eng[0],
                details={"preferred_count": preferred_count, "min_required": min_required}
            )
        # ------------------------------------------------------------------

        r = Response()
        r.text = "Preference Step 2"
        r.attachments = card
        return r

class MyPreferencesCommand(Command):
    def __init__(self):
        super().__init__(command_keyword="my_preferences", help_message="View submitted preferences.", card=None)

    def execute(self, message, attachment_actions, activity):
        sender = activity.get("personEmail") or activity.get("actor", {}).get("emailAddress")
        eng = get_engineer_by_email(sender)
        if not eng: return "⛔ Access Denied."
        
        engineer_id, name, team_id = eng
        _, _, target_month, display_month = get_next_month_info()

        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("SELECT status, preferred_count, priority_dates FROM preferences WHERE engineer_id=%s AND target_month=%s", (engineer_id, target_month))
            pref = cur.fetchone()
            cur.close()
            conn.close()

            bot_audit("BOT_MY_PREFS_CHECK", team_id=team_id, entity_type="engineer", entity_id=engineer_id)

            if not pref:
                return f"⏳ You have not submitted preferences for **{display_month}** yet. Type `hi` to submit them."
            
            status, count, dates = pref
            if status == 'opted_out':
                return f"🏖️ You have **opted out** of shifts for **{display_month}**."
            
            msg = f"📋 **Your Preferences for {display_month}:**\n\n"
            msg += f"**Requested Shifts:** {count}\n**Ranked Dates:**\n"
            for i, d in enumerate(dates or [], 1):
                msg += f"{i}. {d.strftime('%A, %d %b %Y')}\n"
            return msg
        except Exception as e:
            return f"❌ DB Error: {e}"

class MyShiftsCommand(Command):
    def __init__(self):
        super().__init__(command_keyword="my_shifts", help_message="View your upcoming shifts.", card=None)

    def execute(self, message, attachment_actions, activity):
        sender = activity.get("personEmail") or activity.get("actor", {}).get("emailAddress")
        eng = get_engineer_by_email(sender)
        if not eng: return "⛔ Access Denied."

        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("SELECT shift_date FROM roster_assignments WHERE engineer_id=%s AND shift_date >= CURRENT_DATE ORDER BY shift_date", (eng[0],))
            shifts = cur.fetchall()
            cur.close()
            conn.close()

            bot_audit("BOT_MY_SHIFTS_CHECK", team_id=eng[2], entity_type="engineer", entity_id=eng[0])

            if not shifts: return f"Hi {eng[1]}, you currently have no upcoming shifts scheduled."
            msg = f"📅 **Your Upcoming Shifts, {eng[1]}:**\n\n"
            for (d,) in shifts: msg += f"- {d.strftime('%A, %d %b %Y')}\n"
            return msg
        except Exception as e:
            return f"❌ DB Error: {e}"

class WhoIsWorkingCommand(Command):
    def __init__(self):
        super().__init__(command_keyword="who_is_working", help_message="See who is working this weekend.", card=None)

    def execute(self, message, attachment_actions, activity):
        sender = activity.get("personEmail") or activity.get("actor", {}).get("emailAddress")
        eng = get_engineer_by_email(sender)
        if not eng: return "⛔ Access Denied."

        sat_date, sun_date = get_upcoming_weekend()
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                SELECT r.shift_date, e.name FROM roster_assignments r
                JOIN engineers e ON r.engineer_id = e.id
                WHERE r.team_id=%s AND r.shift_date IN (%s, %s) ORDER BY r.shift_date, e.name
            """, (eng[2], sat_date, sun_date))
            results = cur.fetchall()
            cur.close()
            conn.close()

            bot_audit("BOT_WHO_IS_WORKING_CHECK", team_id=eng[2], entity_type="engineer", entity_id=eng[0])

            sat_workers = [r[1] for r in results if str(r[0]) == str(sat_date)]
            sun_workers = [r[1] for r in results if str(r[0]) == str(sun_date)]
            
            msg = f"👥 **Upcoming Weekend Roster:**\n\n"
            msg += f"**Saturday ({sat_date.strftime('%d %b')}):** {', '.join(sat_workers) if sat_workers else 'No one scheduled.'}\n"
            msg += f"**Sunday ({sun_date.strftime('%d %b')}):** {', '.join(sun_workers) if sun_workers else 'No one scheduled.'}"
            return msg
        except Exception as e:
            return f"❌ DB Error: {e}"

class PendingStatusCommand(Command):
    def __init__(self):
        super().__init__(command_keyword="pending_status", help_message="Admin: Check pending submissions.", card=None)

    def execute(self, message, attachment_actions, activity):
        sender = activity.get("personEmail") or activity.get("actor", {}).get("emailAddress")
        eng = get_engineer_by_email(sender)
        if not eng or not is_user_admin(sender, eng[2]): return "⛔ Access Denied: Admin only."

        _, _, target_month, display_month = get_next_month_info()
        pending = get_pending_engineers(target_month, team_id=eng[2])
        
        bot_audit("BOT_ADMIN_PENDING_CHECK", team_id=eng[2], details={"pending_count": len(pending)})

        if not pending:
            return f"✅ **All Good!** Every engineer in your team has submitted preferences for {display_month}."

        names = [p[1] for p in pending]
        
        # Adaptive Card with the "Send Reminders" button
        card = {
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.2",
                "body": [
                    {"type": "TextBlock", "text": f"⏳ Pending Submissions ({display_month})", "weight": "Bolder", "size": "Medium"},
                    {"type": "TextBlock", "text": f"{len(names)} engineers have not submitted yet:", "wrap": True},
                    {"type": "TextBlock", "text": ", ".join(names), "wrap": True, "color": "Attention"}
                ],
                "actions": [
                    {
                        "type": "Action.Submit", 
                        "title": "🔔 Send Reminders Now", 
                        "data": {"callback_keyword": "send_reminders_now", "target_month": target_month}
                    }
                ]
            }
        }
        r = Response()
        r.text = "Pending Status"
        r.attachments = card
        return r

class SendRemindersNowCommand(Command):
    def __init__(self):
        super().__init__(command_keyword="send_reminders_now", help_message="Admin: Send manual reminders.", card=None)

    def execute(self, message, attachment_actions, activity):
        sender = activity.get("personEmail") or activity.get("actor", {}).get("emailAddress")
        eng = get_engineer_by_email(sender)
        if not eng or not is_user_admin(sender, eng[2]): return "⛔ Access Denied."

        inputs = attachment_actions.inputs if attachment_actions else {}
        target_month = inputs.get("target_month")
        _, _, _, display_month = get_next_month_info()
        
        pending = get_pending_engineers(target_month, team_id=eng[2])
        if not pending: return "✅ No pending engineers to remind."

        team_name = get_team_name(eng[2])
        card = build_step1_card(display_month, team_name=team_name)
        
        sent_count = 0
        for _, name, email, _ in pending:
            if email and bot_instance:
                try:
                    bot_instance.teams.messages.create(
                        toPersonEmail=email,
                        text="Reminder: Please submit your weekend shift preferences.",
                        attachments=[card]
                    )
                    sent_count += 1
                except Exception:
                    pass

        bot_audit("BOT_ADMIN_MANUAL_REMINDERS", team_id=eng[2], details={"sent_count": sent_count})
        return f"✅ Successfully sent reminders to **{sent_count}** engineers."

class SavePreferencesCommand(Command):
    def __init__(self):
        super().__init__(command_keyword="save_preferences", help_message="Save preferences", card=None)
        self.card_callback_keyword = "save_preferences"

    def execute(self, message, attachment_actions, activity):
        sender = activity.get("personEmail") or activity.get("actor", {}).get("emailAddress")
        eng = get_engineer_by_email(sender)
        if not eng:
            bot_audit(
                "BOT_PREF_SUBMIT_FAILED",
                status="failed",
                details={"reason": "not_registered", "email": sender},
                error_message="access_denied"
            )
            return "⛔ Access Denied."

        engineer_id, _, team_id = eng
        inputs = attachment_actions.inputs if attachment_actions else {}

        target_month = inputs.get("target_month")
        preferred_count = int(inputs.get("preferred_count", 2))
        min_required = preferred_count + 2

        selected = []
        for k, v in sorted(inputs.items()):
            if k.startswith("priority_") and v and v.strip():
                selected.append(v.strip())

        seen = set()
        unique = []
        duplicates = []
        for d in selected:
            if d in seen:
                duplicates.append(d)
            else:
                seen.add(d)
                unique.append(d)

        if duplicates:
            bot_audit(
                "BOT_PREF_SUBMIT_FAILED",
                status="failed",
                team_id=team_id,
                target_month=target_month,
                entity_type="preferences",
                entity_id=engineer_id,
                details={"reason": "duplicate_dates", "selected": selected},
                error_message="duplicate_dates"
            )
            return "⚠️ Duplicate dates detected. Please resubmit without duplicates."

        if len(unique) < min_required:
            bot_audit(
                "BOT_PREF_SUBMIT_FAILED",
                status="failed",
                team_id=team_id,
                target_month=target_month,
                entity_type="preferences",
                entity_id=engineer_id,
                details={"reason": "min_not_met", "selected_count": len(unique), "min_required": min_required},
                error_message="min_required_not_met"
            )
            return f"⚠️ You selected {len(unique)} dates, minimum required is {min_required}."

        conn = None
        cur = None
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO preferences (engineer_id, target_month, status, preferred_count, priority_dates, updated_at)
                VALUES (%s, %s, 'submitted', %s, %s::date[], CURRENT_TIMESTAMP)
                ON CONFLICT (engineer_id, target_month)
                DO UPDATE SET
                    status='submitted',
                    preferred_count=EXCLUDED.preferred_count,
                    priority_dates=EXCLUDED.priority_dates,
                    updated_at=CURRENT_TIMESTAMP
            """, (engineer_id, target_month, preferred_count, unique))
            conn.commit()

            bot_audit(
                "BOT_PREF_SUBMIT",
                team_id=team_id,
                target_month=target_month,
                entity_type="preferences",
                entity_id=engineer_id,
                details={"preferred_count": preferred_count, "priority_dates": unique}
            )

            check_all_complete_and_notify(target_month, team_id)

            formatted = []
            for i, d in enumerate(unique, start=1):
                dt = datetime.strptime(d, "%Y-%m-%d")
                formatted.append(f"{i}. {dt.strftime('%d-%m-%Y (%A)')}")
            date_list = "\n".join(formatted)

            return (
                f"✅ Preferences Saved Successfully!\n\n"
                f"📅 Month: {target_month}\n"
                f"🔢 Preferred Shifts: {preferred_count}\n"
                f"📋 Your Priority Dates:\n{date_list}"
            )
        except Exception as ex:
            bot_audit(
                "BOT_PREF_SUBMIT_FAILED",
                status="failed",
                team_id=team_id,
                target_month=target_month,
                entity_type="preferences",
                entity_id=engineer_id,
                details={"preferred_count": preferred_count},
                error_message=str(ex)
            )
            return f"❌ Database Error: {ex}"
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
class OptOutCommand(Command):
    def __init__(self):
        super().__init__(command_keyword="opt_out_preferences", help_message="Opt out", card=None)
        self.card_callback_keyword = "opt_out_preferences"

    def execute(self, message, attachment_actions, activity):
        sender = activity.get("actor", {}).get("emailAddress")
        eng = get_engineer_by_email(sender)
        if not eng:
            return "⛔ Access Denied."

        engineer_id, _, team_id = eng
        _, _, target_month, display = get_next_month_info()

        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO preferences (engineer_id, target_month, status, preferred_count, priority_dates, updated_at)
                VALUES (%s, %s, 'opted_out', 0, NULL, CURRENT_TIMESTAMP)
                ON CONFLICT (engineer_id, target_month)
                DO UPDATE SET
                    status='opted_out',
                    preferred_count=0,
                    priority_dates=NULL,
                    updated_at=CURRENT_TIMESTAMP
            """, (engineer_id, target_month))
            conn.commit()
            bot_audit("BOT_PREF_OPTOUT", team_id=team_id, target_month=target_month,
                entity_type="preferences", entity_id=engineer_id, details={"preferred_count": 0})
            cur.close()
            conn.close()

            check_all_complete_and_notify(target_month, team_id)
            return f"🏖️ Opt-out saved for {display}."
        except Exception as e:
            bot_audit("BOT_PREF_OPTOUT_FAILED", status="failed", team_id=team_id, target_month=target_month,
                entity_type="preferences", entity_id=engineer_id, error_message=str(e))
            return f"❌ Database Error: {e}"

def nag_pending_engineers():
    print("[SCHEDULER] 24-hour pending reminder")
    _, _, target_month, display = get_next_month_info()

    if not bot_instance:
        print("bot_instance not initialized")
        return

    # Only teams where admin triggered broadcast at least once
    team_ids = get_triggered_team_ids(target_month)
    if not team_ids:
        print(f"No triggered teams for {target_month}; skipping reminders.")
        return

    total_sent = 0
    total_failed = 0

    for team_id in team_ids:
        pending = get_pending_engineers(target_month, team_id=team_id)
        if not pending:
            continue

        for eng_id, name, email, _ in pending:
            if not email:
                continue
            try:
                team_name = get_team_name(team_id)
                card = build_step1_card(display, team_name=team_name)
                bot_instance.teams.messages.create(
                    toPersonEmail=email,
                    text="Reminder: Please submit your preferences.",
                    attachments=[card]
                )
                total_sent += 1

                bot_audit(
                    action="BOT_NAG_SENT",
                    status="success",
                    team_id=team_id,
                    target_month=target_month,
                    entity_type="preferences",
                    entity_id=eng_id,
                    details={"email": email, "name": name}
                )
                print(f"Reminder sent to {name} ({email})")
            except Exception as ex:
                total_failed += 1
                bot_audit(
                    action="BOT_NAG_FAILED",
                    status="failed",
                    team_id=team_id,
                    target_month=target_month,
                    entity_type="preferences",
                    entity_id=eng_id,
                    details={"email": email, "name": name},
                    error_message=str(ex)
                )
                print(f"Reminder error for {email}: {ex}")

    bot_audit(
        action="BOT_NAG_RUN",
        status="success",
        target_month=target_month,
        entity_type="notification",
        details={
            "triggered_teams": team_ids,
            "sent": total_sent,
            "failed": total_failed
        }
    )

def send_admin_digest():
    print("[SCHEDULER] 24-hour admin digest")
    _, _, target_month, display = get_next_month_info()

    if not bot_instance:
        print("bot_instance not initialized")
        return

    pending = get_pending_engineers(target_month)
    if not pending:
        print("No pending engineers")
        return

    by_team = {}
    for _, name, _, team_id in pending:
        by_team.setdefault(team_id, []).append(name)

    for team_id, names in by_team.items():
        admins = get_team_admin_emails(team_id)
        if not admins:
            continue

        team_name = get_team_name(team_id)
        msg = f"📋 **Daily Preference Digest — {display}**\n\n"
        msg += f"**{team_name}** pending ({len(names)}):\n"
        for n in names:
            msg += f"- ⏳ {n}\n"

        for a in admins:
            try:
                bot_instance.teams.messages.create(toPersonEmail=a, markdown=msg)
                print(f"Digest sent to {a} for team {team_id}")
            except Exception as e:
                print(f"Digest error for {a}: {e}")

if __name__ == "__main__":
    token = os.getenv("WEBEX_BOT_TOKEN")
    if not token:
        print("WEBEX_BOT_TOKEN missing")
        raise SystemExit(1)

    bot = WebexBot(token)
    bot_instance = bot

    bot.add_command(HelloCommand())
    bot.add_command(StatusCommand())
    bot.add_command(Step1PreferencesCommand())
    bot.add_command(Step2PreferencesCommand())
    bot.add_command(SavePreferencesCommand())
    bot.add_command(OptOutCommand())
    bot.add_command(MyPreferencesCommand())
    bot.add_command(MyShiftsCommand())
    bot.add_command(WhoIsWorkingCommand())
    bot.add_command(PendingStatusCommand())
    bot.add_command(SendRemindersNowCommand())
    bot.add_command(UnableToWorkCommand())
    bot.add_command(SubmitReliefRequestCommand())
    bot.add_command(ReliefResponseCommand())
    scheduler = BackgroundScheduler()
    scheduler.add_job(send_admin_digest, "cron", hour=9, minute=0)
    scheduler.add_job(nag_pending_engineers, "interval", hours=24)
    scheduler.add_job(send_friday_shift_reminders, "interval", minutes=15)
    scheduler.add_job(tick_relief_timers, "interval", minutes=5)
    scheduler.start()

    print("Bot started with scheduler.")
    bot.run()