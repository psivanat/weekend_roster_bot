import os
from dotenv import load_dotenv
from webex_bot.webex_bot import WebexBot
from webex_bot.models.command import Command

# 1. Load environment variables from .env file
load_dotenv()

# 2. Force Python to use the Cisco Proxy for all outbound Webex connections
os.environ["HTTP_PROXY"] = "http://proxy-wsa.esl.cisco.com:80"
os.environ["HTTPS_PROXY"] = "http://proxy-wsa.esl.cisco.com:80"

# 3. Define a custom command for "status"
class StatusCommand(Command):
    def __init__(self):
        super().__init__(
            command_keyword="status",
            help_message="Check if the bot is online.",
            card=None
        )

    def execute(self, message, attachment_actions, activity):
        return "✅ Roster Bot is online via WebSockets! The firewall has been bypassed. 🚀"

# 4. Initialize and run the bot
if __name__ == "__main__":
    bot_token = os.getenv("WEBEX_BOT_TOKEN")
    
    if not bot_token:
        print("Error: WEBEX_BOT_TOKEN not found in environment variables.")
        exit(1)

    print("Starting Webex Bot via WebSockets...")
    
    # Create the bot (this automatically deletes any old HTTP webhooks to prevent conflicts)
    bot = WebexBot(bot_token)
    
    # Add our custom commands
    bot.add_command(StatusCommand())
    
    # Start the WebSocket connection
    bot.run()