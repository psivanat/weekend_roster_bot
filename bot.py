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
                    # FIXED: Using the strict "command" keyword required by the SDK
                    {"type": "Action.Submit", "title": "📝 Update Next Month's Preferences", "data": {"command": "step1_preferences"}},
                    {"type": "Action.Submit", "title": "❓ Bot Status", "data": {"command": "status"}}
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
                    # FIXED: Using the strict "command" keyword
                    {"type": "Action.Submit", "title": "Next ➡️", "data": {"command": "step2_preferences"}},
                    {"type": "Action.Submit", "title": "🏖️ Opt-Out (Unavailable)", "data": {"command": "opt_out_preferences"}}
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
                        # FIXED: Using the strict "command" keyword
                        "data": {"command": "save_preferences", "target_month": target_month_str, "preferred_count": preferred_count}
                    }
                ]
            }
        }
        response = Response()
        response.text = "Step 2"
        response.attachments = step2_card
        return response

# ==========================================
# COMMAND: Save Preferences (Placeholder)
# ==========================================
class SavePreferencesCommand(Command):
    def __init__(self):
        super().__init__(command_keyword="save_preferences", help_message="Save to DB.", card=None)

    def execute(self, message, attachment_actions, activity):
        return "✅ **Success!** Your preferences have been saved to the database. Thank you!"

# ==========================================
# COMMAND: Opt Out (Placeholder)
# ==========================================
class OptOutCommand(Command):
    def __init__(self):
        super().__init__(command_keyword="opt_out_preferences", help_message="Opt out of shifts.", card=None)

    def execute(self, message, attachment_actions, activity):
        return "🏖️ **Opt-Out Confirmed:** You have been marked as unavailable for next month."

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