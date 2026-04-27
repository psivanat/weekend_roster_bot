import os
import calendar
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler
from webex_bot.webex_bot import WebexBot
from webex_bot.models.command import Command
from webex_bot.models.response import Response

# 1. Load Environment & Proxies
load_dotenv()
os.environ["HTTP_PROXY"] = "http://proxy-wsa.esl.cisco.com:80"
os.environ["HTTPS_PROXY"] = "http://proxy-wsa.esl.cisco.com:80"

# ==========================================
# GLOBAL: Bot instance (set during init)
# ==========================================
bot_instance = None

# ==========================================
# DATABASE HELPERS
# ==========================================
def get_db_connection():
    """Returns a new database connection."""
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        database=os.getenv("DB_NAME", "roster_db"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASS", "")
    )

def get_user_from_db(email):
    """Checks if the Webex sender is an active engineer."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id, name, team_id FROM engineers WHERE webex_email = %s AND is_active = true", (email,))
        engineer = cursor.fetchone()
        cursor.close()
        conn.close()
        return engineer
    except Exception as e:
        print(f"DB Error: {e}")
        return None

def get_next_month_info():
    """Returns (next_month_number, next_year, target_month_str, display_str)."""
    now = datetime.now()
    next_m = now.month + 1 if now.month < 12 else 1
    next_y = now.year if now.month < 12 else now.year + 1
    target_month_str = f"{next_y}-{next_m:02d}"
    display_str = datetime(next_y, next_m, 1).strftime("%B %Y")
    return next_m, next_y, target_month_str, display_str

def get_weekend_dates(year, month):
    """Returns list of weekend dates for a given month."""
    cal = calendar.Calendar()
    return [day for day in cal.itermonthdates(year, month) if day.month == month and day.weekday() in [5, 6]]

# ==========================================
# SCHEDULER: Background Notification Tasks
# ==========================================

def build_preference_card(next_month_display):
    """Builds the Step 1 Adaptive Card for preference collection."""
    return {
        "contentType": "application/vnd.microsoft.card.adaptive",
        "content": {
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "type": "AdaptiveCard",
            "version": "1.2",
            "body": [
                {"type": "TextBlock", "text": f"📅 Preferences for {next_month_display}", "weight": "Bolder", "size": "Medium"},
                {"type": "TextBlock", "text": "🔔 **Reminder:** Please submit your weekend shift preferences.", "wrap": True, "color": "Attention"},
                {"type": "TextBlock", "text": "How many weekend shifts do you want to work?", "wrap": True},
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

def get_pending_engineers(target_month_str):
    """Returns list of engineers who haven't submitted preferences yet."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT e.id, e.name, e.webex_email, e.team_id
            FROM engineers e
            WHERE e.is_active = true
            AND e.id NOT IN (
                SELECT p.engineer_id FROM preferences p
                WHERE p.target_month = %s AND p.status IN ('submitted', 'opted_out')
            )
        """, (target_month_str,))
        pending = cursor.fetchall()
        cursor.close()
        conn.close()
        return pending
    except Exception as e:
        print(f"DB Error (get_pending): {e}")
        return []

def get_team_admins():
    """Returns list of (email, team_id, team_name) for all team admins."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT u.email, ut.team_id, t.name
            FROM users u
            JOIN user_teams ut ON u.id = ut.user_id
            JOIN teams t ON ut.team_id = t.id
            WHERE u.role IN ('team_admin', 'super_admin')
            AND u.email IS NOT NULL
        """)
        admins = cursor.fetchall()
        cursor.close()
        conn.close()
        return admins
    except Exception as e:
        print(f"DB Error (get_admins): {e}")
        return []

def check_all_complete_and_notify(target_month_str):
    """If all engineers have submitted, notify all admins immediately."""
    pending = get_pending_engineers(target_month_str)
    if len(pending) == 0:
        admins = get_team_admins()
        seen_emails = set()
        for admin_email, team_id, team_name in admins:
            if admin_email not in seen_emails:
                seen_emails.add(admin_email)
                try:
                    bot_instance.teams.messages.create(
                        toPersonEmail=admin_email,
                        markdown=(
                            f"🎉 **All Preferences Collected!**\n\n"
                            f"Every engineer has submitted (or opted out of) their preferences for **{target_month_str}**.\n\n"
                            f"You can now go to the dashboard and **generate the roster**! 🚀"
                        )
                    )
                    print(f"All-complete notification sent to {admin_email}")
                except Exception as e:
                    print(f"Error sending all-complete to {admin_email}: {e}")

def nag_pending_engineers():
    """48-HOUR TASK: Send reminder to engineers who haven't submitted."""
    print("[SCHEDULER] Running 48-hour nag task...")
    _, next_y, target_month_str, display_str = get_next_month_info()
    pending = get_pending_engineers(target_month_str)

    if not pending:
        print("[SCHEDULER] No pending engineers. Skipping nag.")
        return

    card = build_preference_card(display_str)

    for eng_id, eng_name, eng_email, team_id in pending:
        if not eng_email:
            print(f"[SCHEDULER] Skipping {eng_name} — no Webex email.")
            continue
        try:
            bot_instance.teams.messages.create(
                toPersonEmail=eng_email,
                text="Reminder: Please submit your shift preferences.",
                attachments=[card]
            )
            print(f"[SCHEDULER] Reminder sent to {eng_name} ({eng_email})")
        except Exception as e:
            print(f"[SCHEDULER] Error sending to {eng_email}: {e}")

def send_admin_digest():
    """24-HOUR TASK: Send admin a summary of who hasn't submitted."""
    print("[SCHEDULER] Running 24-hour admin digest...")
    _, next_y, target_month_str, display_str = get_next_month_info()
    pending = get_pending_engineers(target_month_str)
    admins = get_team_admins()

    if not pending:
        print("[SCHEDULER] Everyone has submitted. Skipping digest.")
        return

    # Group pending engineers by team
    pending_by_team = {}
    for eng_id, eng_name, eng_email, team_id in pending:
        if team_id not in pending_by_team:
            pending_by_team[team_id] = []
        pending_by_team[team_id].append(eng_name)

    # Send digest to each admin for their teams
    seen_emails = set()
    for admin_email, team_id, team_name in admins:
        if admin_email in seen_emails:
            continue
        seen_emails.add(admin_email)

        # Build a combined digest for all teams this admin manages
        digest_text = f"📋 **Daily Preference Digest — {display_str}**\n\n"
        has_pending = False

        for a_email, a_team_id, a_team_name in admins:
            if a_email == admin_email and a_team_id in pending_by_team:
                has_pending = True
                names = pending_by_team[a_team_id]
                digest_text += f"**{a_team_name}** ({len(names)} pending):\n"
                for name in names:
                    digest_text += f"  - ⏳ {name}\n"
                digest_text += "\n"

        if not has_pending:
            continue

        total_pending = sum(len(v) for k, v in pending_by_team.items())
        digest_text += f"**Total Pending:** {total_pending} engineer(s)\n"

        try:
            bot_instance.teams.messages.create(
                toPersonEmail=admin_email,
                markdown=digest_text
            )
            print(f"[SCHEDULER] Digest sent to {admin_email}")
        except Exception as e:
            print(f"[SCHEDULER] Error sending digest to {admin_email}: {e}")

# ==========================================
# COMMAND: Hi / Main Menu
# ==========================================
class HelloCommand(Command):
    def __init__(self):
        super().__init__(command_keyword="hi", help_message="Show the main menu.", card=None)
        self.aliases = ["hello", "help", "menu"]

    def execute(self, message, attachment_actions, activity):
        sender_email = activity.get("personEmail") or activity.get("actor", {}).get("emailAddress")
        user = get_user_from_db(sender_email)

        if not user:
            return "⛔ **Access Denied:** You are not registered in any Roster Team."

        menu_card = {
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.2",
                "body": [
                    {"type": "TextBlock", "text": f"👋 Welcome back, {user[1]}!", "weight": "Bolder", "size": "Medium"},
                    {"type": "TextBlock", "text": "What would you like to do today?", "wrap": True}
                ],
                "actions": [
                    {"type": "Action.Submit", "title": "📝 Update Next Month's Preferences", "data": {"callback_keyword": "step1_preferences"}},
                    {"type": "Action.Submit", "title": "❓ Bot Status", "data": {"callback_keyword": "status"}}
                ]
            }
        }
        response = Response()
        response.text = "Menu"
        response.attachments = menu_card
        return response

# ==========================================
# COMMAND: Status
# ==========================================
class StatusCommand(Command):
    def __init__(self):
        super().__init__(command_keyword="status", help_message="Check bot status.", card=None)

    def execute(self, message, attachment_actions, activity):
        return "✅ Roster Bot is online via WebSockets!"

# ==========================================
# COMMAND: Step 1 (Shift Count)
# ==========================================
class Step1PreferencesCommand(Command):
    def __init__(self):
        super().__init__(command_keyword="step1_preferences", help_message="Start preference submission.", card=None)
        self.card_callback_keyword = "step1_preferences"

    def execute(self, message, attachment_actions, activity):
        _, _, _, next_month_str = get_next_month_info()

        step1_card = {
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.2",
                "body": [
                    {"type": "TextBlock", "text": f"📅 Preferences for {next_month_str}", "weight": "Bolder", "size": "Medium"},
                    {"type": "TextBlock", "text": "Step 1: How many weekend shifts do you want to work?", "wrap": True},
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
        response = Response()
        response.text = "Step 1"
        response.attachments = step1_card
        return response

# ==========================================
# COMMAND: Step 2 (Dynamic Dates)
# ==========================================
class Step2PreferencesCommand(Command):
    def __init__(self):
        super().__init__(command_keyword="step2_preferences", help_message="Generate date dropdowns.", card=None)
        self.card_callback_keyword = "step2_preferences"

    def execute(self, message, attachment_actions, activity):
        inputs = attachment_actions.inputs if attachment_actions else {}
        preferred_count = int(inputs.get("preferred_count", 2))
        min_required = preferred_count + 2

        next_m, next_y, target_month_str, _ = get_next_month_info()
        weekend_dates = get_weekend_dates(next_y, next_m)

        if min_required > len(weekend_dates):
            min_required = len(weekend_dates)

        date_choices = [{"title": d.strftime("%A, %b %d"), "value": d.strftime("%Y-%m-%d")} for d in weekend_dates]

        form_body = [
            {"type": "TextBlock", "text": "📅 Step 2: Rank Your Dates", "weight": "Bolder", "size": "Medium"},
            {"type": "TextBlock", "text": f"You chose {preferred_count} shifts. Please rank at least {min_required} dates.", "wrap": True},
            {"type": "TextBlock", "text": "⚠️ Please do not select the same date twice.", "wrap": True, "color": "Attention", "weight": "Bolder"}
        ]

        for i in range(1, len(weekend_dates) + 1):
            is_required = i <= min_required
            req_text = "(Required)" if is_required else "(Optional)"
            form_body.append({"type": "TextBlock", "text": f"Priority {i} {req_text}:", "spacing": "Medium"})
            form_body.append({
                "type": "Input.ChoiceSet",
                "id": f"priority_{i}",
                "choices": date_choices,
                "placeholder": "Select a date...",
                "isRequired": is_required,
                "errorMessage": f"Priority {i} is required."
            })

        step2_card = {
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.2",
                "body": form_body,
                "actions": [
                    {
                        "type": "Action.Submit",
                        "title": "✅ Submit Preferences",
                        "data": {"callback_keyword": "save_preferences", "target_month": target_month_str, "preferred_count": preferred_count}
                    }
                ]
            }
        }
        response = Response()
        response.text = "Step 2"
        response.attachments = step2_card
        return response

# ==========================================
# COMMAND: Save Preferences (With Validation)
# ==========================================
class SavePreferencesCommand(Command):
    def __init__(self):
        super().__init__(command_keyword="save_preferences", help_message="Save to DB.", card=None)
        self.card_callback_keyword = "save_preferences"

    def execute(self, message, attachment_actions, activity):
        sender_email = activity.get("actor", {}).get("emailAddress")
        user = get_user_from_db(sender_email)

        if not user:
            return "⛔ **Access Denied.**"

        engineer_id = user[0]
        inputs = attachment_actions.inputs if attachment_actions else {}

        target_month = inputs.get("target_month")
        preferred_count = int(inputs.get("preferred_count", 2))
        min_required = preferred_count + 2

        # Collect all selected dates
        selected_dates = []
        for key, value in sorted(inputs.items()):
            if key.startswith("priority_") and value:
                selected_dates.append(value)

        # Duplicate check
        seen = set()
        unique_dates = []
        duplicates = []
        for d in selected_dates:
            if d in seen:
                duplicates.append(d)
            else:
                seen.add(d)
                unique_dates.append(d)

        if duplicates:
            dup_list = ", ".join(set(duplicates))
            return (
                f"⚠️ **Duplicate dates detected!**\n\n"
                f"You selected the same date more than once: **{dup_list}**\n\n"
                f"Please type **hi** and re-submit your preferences without duplicates."
            )

        if len(unique_dates) < min_required:
            return (
                f"⚠️ **Not enough dates!**\n\n"
                f"You selected {len(unique_dates)} dates, but the minimum required is **{min_required}**.\n\n"
                f"Please type **hi** and re-submit with at least {min_required} dates."
            )

        # Save to database
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO preferences (engineer_id, target_month, status, preferred_count, priority_dates, updated_at)
                VALUES (%s, %s, 'submitted', %s, %s::date[], CURRENT_TIMESTAMP)
                ON CONFLICT (engineer_id, target_month)
                DO UPDATE SET
                    status = 'submitted',
                    preferred_count = EXCLUDED.preferred_count,
                    priority_dates = EXCLUDED.priority_dates,
                    updated_at = CURRENT_TIMESTAMP
            """, (engineer_id, target_month, preferred_count, unique_dates))
            conn.commit()
            cursor.close()
            conn.close()

            # Check if all engineers are done — notify admin if yes
            check_all_complete_and_notify(target_month)

            date_list = "\n".join([f"  {i+1}. **{d}**" for i, d in enumerate(unique_dates)])
            return (
                f"✅ **Preferences Saved Successfully!**\n\n"
                f"📅 **Month:** {target_month}\n"
                f"🔢 **Preferred Shifts:** {preferred_count}\n"
                f"📋 **Your Priority Dates:**\n{date_list}\n\n"
                f"You can update your preferences anytime by typing **hi**."
            )

        except Exception as e:
            return f"❌ **Database Error:** Could not save preferences.\n`{str(e)}`"

# ==========================================
# COMMAND: Opt Out (With DB Save)
# ==========================================
class OptOutCommand(Command):
    def __init__(self):
        super().__init__(command_keyword="opt_out_preferences", help_message="Opt out of shifts.", card=None)
        self.card_callback_keyword = "opt_out_preferences"

    def execute(self, message, attachment_actions, activity):
        sender_email = activity.get("actor", {}).get("emailAddress")
        user = get_user_from_db(sender_email)

        if not user:
            return "⛔ **Access Denied.**"

        engineer_id = user[0]
        _, _, target_month, display_str = get_next_month_info()

        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO preferences (engineer_id, target_month, status, preferred_count, priority_dates, updated_at)
                VALUES (%s, %s, 'opted_out', 0, NULL, CURRENT_TIMESTAMP)
                ON CONFLICT (engineer_id, target_month)
                DO UPDATE SET
                    status = 'opted_out',
                    preferred_count = 0,
                    priority_dates = NULL,
                    updated_at = CURRENT_TIMESTAMP
            """, (engineer_id, target_month))
            conn.commit()
            cursor.close()
            conn.close()

            # Check if all engineers are done
            check_all_complete_and_notify(target_month)

            return (
                f"🏖️ **Opt-Out Confirmed!**\n\n"
                f"You have been marked as **unavailable** for **{display_str}**.\n"
                f"Changed your mind? Type **hi** and submit your preferences."
            )

        except Exception as e:
            return f"❌ **Database Error:** Could not save opt-out.\n`{str(e)}`"

# ==========================================
# BOT INITIALIZATION + SCHEDULER
# ==========================================
if __name__ == "__main__":
    bot_token = os.getenv("WEBEX_BOT_TOKEN")
    if not bot_token:
        print("Error: WEBEX_BOT_TOKEN not found.")
        exit(1)

    bot = WebexBot(bot_token)
    bot_instance = bot  # Store globally so scheduler functions can send messages

    # Register commands
    bot.add_command(HelloCommand())
    bot.add_command(StatusCommand())
    bot.add_command(Step1PreferencesCommand())
    bot.add_command(Step2PreferencesCommand())
    bot.add_command(SavePreferencesCommand())
    bot.add_command(OptOutCommand())

    # Start the background scheduler
    scheduler = BackgroundScheduler()

    # Run admin digest every 24 hours (at 9:00 AM IST)
    scheduler.add_job(send_admin_digest, 'cron', hour=9, minute=0)

    # Run engineer nag every 48 hours (at 10:00 AM IST)
    scheduler.add_job(nag_pending_engineers, 'interval', hours=48)

    scheduler.start()
    print("[SCHEDULER] Background scheduler started. Admin digest at 9AM, Engineer nag every 48hrs.")

    bot.run()