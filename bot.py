import os
import psycopg2
import calendar
from datetime import datetime, date
from dotenv import load_dotenv
from webex_bot.webex_bot import WebexBot
from webex_bot.models.command import Command
from webex_bot.models.response import Response

load_dotenv()

os.environ["HTTP_PROXY"] = "http://proxy-wsa.esl.cisco.com:80"
os.environ["HTTPS_PROXY"] = "http://proxy-wsa.esl.cisco.com:80"

# -----------------------------------------
# DATABASE HELPER
# -----------------------------------------
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
        
        # Now querying the ENGINEERS table using webex_email
        cursor.execute("SELECT id, name, team_id FROM engineers WHERE webex_email = %s AND is_active = true", (email,))
        engineer = cursor.fetchone()
        
        cursor.close()
        conn.close()
        return engineer # Returns (id, name, team_id) or None
    except Exception as e:
        print(f"DB Error: {e}")
        return None

# -----------------------------------------
# COMMAND: Step 1 Preferences (Shift Count)
# -----------------------------------------
class Step1PreferencesCommand(Command):
    def __init__(self):
        super().__init__(
            command_keyword="step1_preferences",
            help_message="Start the preference submission process.",
            card=None
        )

    def execute(self, message, attachment_actions, activity):
        # Calculate next month (e.g., "May 2026")
        now = datetime.now()
        next_m = now.month + 1 if now.month < 12 else 1
        next_y = now.year if now.month < 12 else now.year + 1
        next_month_str = datetime(next_y, next_m, 1).strftime("%B %Y")

        # The Step 1 Card: Ask for 'n'
        step1_card = {
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.2",
                "body": [
                    {
                        "type": "TextBlock",
                        "text": f"📅 Preferences for {next_month_str}",
                        "weight": "Bolder",
                        "size": "Medium"
                    },
                    {
                        "type": "TextBlock",
                        "text": "Step 1: How many weekend shifts would you prefer to work next month?",
                        "wrap": True
                    },
                    {
                        "type": "Input.ChoiceSet",
                        "id": "preferred_count",
                        "style": "compact",
                        "value": "2", # Default value
                        "choices": [
                            {"title": "1 Shift", "value": "1"},
                            {"title": "2 Shifts", "value": "2"},
                            {"title": "3 Shifts", "value": "3"},
                            {"title": "4 Shifts", "value": "4"}
                        ]
                    }
                ],
                "actions": [
                    {
                        "type": "Action.Submit",
                        "title": "Next ➡️",
                        "data": {"command": "step2_preferences"} # Routes to the next step!
                    },
                    {
                        "type": "Action.Submit",
                        "title": "🏖️ Opt-Out (Unavailable)",
                        "data": {"command": "opt_out_preferences"}
                    }
                ]
            }
        }

        response = Response()
        response.text = "Please select your preferred shift count."
        response.attachments = step1_card
        return response

# -----------------------------------------
# COMMAND: Step 2 Preferences (Dynamic Form)
# -----------------------------------------
class Step2PreferencesCommand(Command):
    def __init__(self):
        super().__init__(
            command_keyword="step2_preferences",
            help_message="Generate the dynamic preference form.",
            card=None
        )

    def execute(self, message, attachment_actions, activity):
        # 1. Get the 'n' value they selected in Step 1
        inputs = attachment_actions.inputs if attachment_actions else {}
        preferred_count = int(inputs.get("preferred_count", 2))
        
        # Calculate n + 2
        min_required = preferred_count + 2

        # 2. Calculate Next Month's Weekend Dates
        now = datetime.now()
        next_m = now.month + 1 if now.month < 12 else 1
        next_y = now.year if now.month < 12 else now.year + 1
        target_month_str = f"{next_y}-{next_m:02d}"
        target_month_display = datetime(next_y, next_m, 1).strftime("%B %Y")

        cal = calendar.Calendar()
        weekend_dates = []
        for day in cal.itermonthdates(next_y, next_m):
            if day.month == next_m and day.weekday() in [5, 6]: # 5=Sat, 6=Sun
                weekend_dates.append(day)

        # Cap min_required if the month has fewer weekends than n+2 (rare, but safe)
        if min_required > len(weekend_dates):
            min_required = len(weekend_dates)

        # 3. Build the Dropdown Choices
        date_choices = [{"title": d.strftime("%A, %b %d"), "value": d.strftime("%Y-%m-%d")} for d in weekend_dates]
        
        # 4. Generate the Dynamic Form Body
        form_body = [
            {
                "type": "TextBlock",
                "text": f"📅 Select Dates for {target_month_display}",
                "weight": "Bolder",
                "size": "Medium"
            },
            {
                "type": "TextBlock",
                "text": f"You chose to work {preferred_count} shifts. Please rank at least {min_required} dates.",
                "wrap": True,
                "color": "Attention"
            }
        ]

        # Dynamically add the dropdowns
        for i in range(1, len(weekend_dates) + 1):
            is_required = i <= min_required
            req_text = "(Required)" if is_required else "(Optional)"
            
            form_body.append({
                "type": "TextBlock",
                "text": f"Priority {i} {req_text}:",
                "spacing": "Medium"
            })
            form_body.append({
                "type": "Input.ChoiceSet",
                "id": f"priority_{i}",
                "choices": date_choices,
                "placeholder": "Select a date...",
                "isRequired": is_required,
                "errorMessage": f"Priority {i} is required."
            })

        # 5. Build the Final Card
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
                        "data": {
                            "command": "save_preferences",
                            "target_month": target_month_str,
                            "preferred_count": preferred_count
                        }
                    }
                ]
            }
        }

        response = Response()
        response.text = "Please fill out your date preferences."
        response.attachments = step2_card
        return response

# -----------------------------------------
# COMMAND: Status
# -----------------------------------------
class StatusCommand(Command):
    def __init__(self):
        super().__init__(
            command_keyword="status",
            help_message="Check if the bot is online.",
            card=None
        )

    def execute(self, message, attachment_actions, activity):
        return "✅ Roster Bot is online via WebSockets! The firewall has been bypassed. 🚀"

# -----------------------------------------
# COMMAND: Roster (Upcoming Shifts)
# -----------------------------------------
class RosterCommand(Command):
    def __init__(self):
        super().__init__(
            command_keyword="roster",
            help_message="Show who is working the upcoming weekend.",
            card=None
        )
        
    def execute(self, message, attachment_actions, activity):
        try:
            conn = psycopg2.connect(
                host=os.getenv("DB_HOST", "localhost"),
                database=os.getenv("DB_NAME", "roster_db"),
                user=os.getenv("DB_USER", "postgres"),
                password=os.getenv("DB_PASS", "")
            )
            cursor = conn.cursor()
            query = """
                SELECT r.shift_date, e.name 
                FROM roster_assignments r
                JOIN engineers e ON r.engineer_id = e.id
                WHERE r.shift_date >= CURRENT_DATE
                ORDER BY r.shift_date ASC
                LIMIT 10;
            """
            cursor.execute(query)
            records = cursor.fetchall()
            cursor.close()
            conn.close()
            
            if not records:
                return "🗓️ There are no upcoming shifts scheduled in the database."
                
            response = "**🗓️ Upcoming Weekend Shifts:**\n\n"
            current_date = None
            for shift_date, engineer_name in records:
                if shift_date != current_date:
                    response += f"\n**{shift_date.strftime('%A, %b %d, %Y')}**\n"
                    current_date = shift_date
                response += f"- 👤 {engineer_name}\n"
                
            return response

        except Exception as e:
            return f"❌ **Database Error:** Could not fetch the roster.\n`{str(e)}`"

# -----------------------------------------
# COMMAND: Hi / Hello / Help (Interactive Menu)
# -----------------------------------------
class HelloCommand(Command):
    def __init__(self):
        super().__init__(
            command_keyword="hi",
            help_message="Show the main menu.",
            card=None
        )
        # Also trigger on these words
        self.aliases = ["hello", "help", "menu"]

    def execute(self, message, attachment_actions, activity):
        sender_email = activity["actor"]["emailAddress"]
        user = get_user_from_db(sender_email)

        # 1. Access Control Check
        if not user:
            return "⛔ **Access Denied:** You are not registered in any Roster Team. Please contact your Team Admin."

        user_name = user[1]

        # 2. The Adaptive Card JSON (Main Menu)
        menu_card = {
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.2",
                "body": [
                    {
                        "type": "TextBlock",
                        "text": f"👋 Welcome back, {user_name}!",
                        "weight": "Bolder",
                        "size": "Medium"
                    },
                    {
                        "type": "TextBlock",
                        "text": "What would you like to do today?",
                        "wrap": True
                    }
                ],
                "actions": [
                    {
                        "type": "Action.Submit",
                        "title": "📝 Update Next Month's Preferences",
                        "data": {"command": "step1_preferences"} # <--- Changed to "command"
                    },
                    {
                        "type": "Action.Submit",
                        "title": "❓ Bot Status",
                        "data": {"command": "status"} # <--- Reuses your existing status command!
                    }
                ]
            }
        }

        # Return the card as a Response object
        response = Response()
        response.text = "This client does not support Adaptive Cards."
        response.attachments = menu_card 
        return response

# -----------------------------------------
# BOT INITIALIZATION
# -----------------------------------------
if __name__ == "__main__":
    bot_token = os.getenv("WEBEX_BOT_TOKEN")
    if not bot_token:
        print("Error: WEBEX_BOT_TOKEN not found.")
        exit(1)

    print("Starting Webex Bot via WebSockets...")
    bot = WebexBot(bot_token)
    
    # Register commands
    bot.add_command(HelloCommand())
    bot.add_command(StatusCommand())
    bot.add_command(RosterCommand())
    bot.add_command(Step1PreferencesCommand())
    bot.add_command(Step2PreferencesCommand())    
    
    bot.run()