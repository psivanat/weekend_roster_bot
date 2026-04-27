import os
import psycopg2
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
    """Checks if the Webex sender is an authorized team member."""
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            database=os.getenv("DB_NAME", "roster_db"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASS", "")
        )
        cursor = conn.cursor()
        cursor.execute("SELECT id, name, role FROM users WHERE email = %s", (email,))
        user = cursor.fetchone()
        cursor.close()
        conn.close()
        return user # Returns (id, name, role) or None
    except Exception as e:
        print(f"DB Error: {e}")
        return None

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
                        "data": {"action": "start_preferences"}
                    },
                    {
                        "type": "Action.Submit",
                        "title": "📊 View My Upcoming Shifts",
                        "data": {"action": "view_shifts"}
                    },
                    {
                        "type": "Action.Submit",
                        "title": "❓ Bot Status",
                        "data": {"action": "check_status"}
                    }
                ]
            }
        }

        # Return the card as a Response object
        response = Response()
        response.text = "This client does not support Adaptive Cards."
        response.attachments = [menu_card]
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
    
    bot.run()