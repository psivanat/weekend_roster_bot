import os
import calendar
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
from webex_bot.webex_bot import WebexBot
from webex_bot.models.command import Command
from webex_bot.models.response import Response

# 1. Load Environment & Proxies
load_dotenv()
os.environ["HTTP_PROXY"] = "http://proxy-wsa.esl.cisco.com:80"
os.environ["HTTPS_PROXY"] = "http://proxy-wsa.esl.cisco.com:80"

# ==========================================
# DATABASE HELPER
# ==========================================
def get_user_from_db(email):
    """Checks if the Webex sender is an active engineer."""
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            database=os.getenv("DB_NAME", "roster_db"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASS", "")
        )
        cursor = conn.cursor()
        cursor.execute("SELECT id, name, team_id FROM engineers WHERE webex_email = %s AND is_active = true", (email,))
        engineer = cursor.fetchone()
        cursor.close()
        conn.close()
        return engineer
    except Exception as e:
        print(f"DB Error: {e}")
        return None

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
        now = datetime.now()
        next_m = now.month + 1 if now.month < 12 else 1
        next_y = now.year if now.month < 12 else now.year + 1
        next_month_str = datetime(next_y, next_m, 1).strftime("%B %Y")

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

        now = datetime.now()
        next_m = now.month + 1 if now.month < 12 else 1
        next_y = now.year if now.month < 12 else now.year + 1
        target_month_str = f"{next_y}-{next_m:02d}"

        cal = calendar.Calendar()
        weekend_dates = [day for day in cal.itermonthdates(next_y, next_m) if day.month == next_m and day.weekday() in [5, 6]]

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

        # 1. Extract metadata
        target_month = inputs.get("target_month")
        preferred_count = int(inputs.get("preferred_count", 2))
        min_required = preferred_count + 2

        # 2. Collect all selected dates (skip empty optional fields)
        selected_dates = []
        for key, value in sorted(inputs.items()):
            if key.startswith("priority_") and value:
                selected_dates.append(value)

        # 3. DUPLICATE CHECK — Remove duplicates but track them
        seen = set()
        unique_dates = []
        duplicates = []
        for d in selected_dates:
            if d in seen:
                duplicates.append(d)
            else:
                seen.add(d)
                unique_dates.append(d)

        # 4. If duplicates were found, warn the user and ask them to re-submit
        if duplicates:
            dup_list = ", ".join(set(duplicates))
            return (
                f"⚠️ **Duplicate dates detected!**\n\n"
                f"You selected the same date more than once: **{dup_list}**\n\n"
                f"Please type **hi** and re-submit your preferences without duplicates."
            )

        # 5. Check minimum requirement
        if len(unique_dates) < min_required:
            return (
                f"⚠️ **Not enough dates!**\n\n"
                f"You selected {len(unique_dates)} dates, but the minimum required is **{min_required}** "
                f"(your preferred shifts + 2).\n\n"
                f"Please type **hi** and re-submit with at least {min_required} dates."
            )

        # 6. Save to PostgreSQL
        try:
            conn = psycopg2.connect(
                host=os.getenv("DB_HOST", "localhost"),
                database=os.getenv("DB_NAME", "roster_db"),
                user=os.getenv("DB_USER", "postgres"),
                password=os.getenv("DB_PASS", "")
            )
            cursor = conn.cursor()

            # Upsert: Insert or update if preferences already exist for this month
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

            # 7. Build a nice confirmation message
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

        # Calculate next month
        now = datetime.now()
        next_m = now.month + 1 if now.month < 12 else 1
        next_y = now.year if now.month < 12 else now.year + 1
        target_month = f"{next_y}-{next_m:02d}"

        try:
            conn = psycopg2.connect(
                host=os.getenv("DB_HOST", "localhost"),
                database=os.getenv("DB_NAME", "roster_db"),
                user=os.getenv("DB_USER", "postgres"),
                password=os.getenv("DB_PASS", "")
            )
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

            return (
                f"🏖️ **Opt-Out Confirmed!**\n\n"
                f"You have been marked as **unavailable** for **{target_month}**.\n"
                f"The scheduling engine will skip you for this month.\n\n"
                f"Changed your mind? Type **hi** and submit your preferences."
            )

        except Exception as e:
            return f"❌ **Database Error:** Could not save opt-out.\n`{str(e)}`"

# ==========================================
# BOT INITIALIZATION
# ==========================================
if __name__ == "__main__":
    bot_token = os.getenv("WEBEX_BOT_TOKEN")
    if not bot_token:
        print("Error: WEBEX_BOT_TOKEN not found.")
        exit(1)

    bot = WebexBot(bot_token)
    bot.add_command(HelloCommand())
    bot.add_command(StatusCommand())
    bot.add_command(Step1PreferencesCommand())
    bot.add_command(Step2PreferencesCommand())
    bot.add_command(SavePreferencesCommand())
    bot.add_command(OptOutCommand())
    bot.run()