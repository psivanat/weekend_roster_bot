import os
import calendar
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler
from webex_bot.webex_bot import WebexBot
from webex_bot.models.command import Command
from webex_bot.models.response import Response

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
        super().__init__(command_keyword="hi", help_message="Show menu", card=None)
        self.aliases = ["hello", "help", "menu"]

    def execute(self, message, attachment_actions, activity):
        sender = activity.get("personEmail") or activity.get("actor", {}).get("emailAddress")
        eng = get_engineer_by_email(sender)
        if not eng:
            return "⛔ Access Denied: You are not registered as an active engineer."

        card = {
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.2",
                "body": [
                    {"type": "TextBlock", "text": f"👋 Welcome back, {eng[1]}!", "weight": "Bolder", "size": "Medium"},
                    {"type": "TextBlock", "text": "Choose an option:", "wrap": True}
                ],
                "actions": [
                    {"type": "Action.Submit", "title": "📝 Update Next Month Preferences", "data": {"callback_keyword": "step1_preferences"}},
                    {"type": "Action.Submit", "title": "❓ Bot Status", "data": {"callback_keyword": "status"}}
                ]
            }
        }
        r = Response()
        r.text = "Menu"
        r.attachments = card
        return r

class StatusCommand(Command):
    def __init__(self):
        super().__init__(command_keyword="status", help_message="Bot status", card=None)

    def execute(self, message, attachment_actions, activity):
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

        # Use target_month from card if present (from GUI broadcast), else fallback to next month
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

        r = Response()
        r.text = "Preference Step 2"
        r.attachments = card
        return r

class SavePreferencesCommand(Command):
    def __init__(self):
        super().__init__(command_keyword="save_preferences", help_message="Save preferences", card=None)
        self.card_callback_keyword = "save_preferences"

    def execute(self, message, attachment_actions, activity):
        sender = activity.get("actor", {}).get("emailAddress")
        eng = get_engineer_by_email(sender)
        if not eng:
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
            return "⚠️ Duplicate dates detected. Please resubmit without duplicates."

        if len(unique) < min_required:
            return f"⚠️ You selected {len(unique)} dates, minimum required is {min_required}."

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
            cur.close()
            conn.close()

            check_all_complete_and_notify(target_month, team_id)

            # formatted confirmation list
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
        except Exception as e:
            return f"❌ Database Error: {e}"

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
            cur.close()
            conn.close()

            check_all_complete_and_notify(target_month, team_id)
            return f"🏖️ Opt-out saved for {display}."
        except Exception as e:
            return f"❌ Database Error: {e}"

def nag_pending_engineers():
    print("[SCHEDULER] 48-hour pending reminder")
    _, _, target_month, display = get_next_month_info()
    pending = get_pending_engineers(target_month)

    if not bot_instance:
        print("bot_instance not initialized")
        return

    for _, name, email, team_id in pending:
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
            print(f"Reminder sent to {name} ({email})")
        except Exception as e:
            print(f"Reminder error for {email}: {e}")

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

    scheduler = BackgroundScheduler()
    scheduler.add_job(send_admin_digest, "cron", hour=9, minute=0)
    scheduler.add_job(nag_pending_engineers, "interval", hours=48)
    scheduler.start()

    print("Bot started with scheduler.")
    bot.run()