import os
import calendar
from datetime import datetime
import psycopg2
from dotenv import load_dotenv
from webexpythonsdk import WebexAPI

load_dotenv()

DB_PARAMS = {
    "host": os.getenv("DB_HOST", "localhost"),
    "database": os.getenv("DB_NAME", "roster_db"),
    "user": os.getenv("DB_USER", "roster_bot"),
    "password": os.getenv("DB_PASS")
}

PROXIES = {
    "http": "http://proxy-wsa.esl.cisco.com:80",
    "https": "http://proxy-wsa.esl.cisco.com:80"
}

def get_db_connection():
    return psycopg2.connect(**DB_PARAMS)

def get_webex_client():
    token = os.getenv("WEBEX_BOT_TOKEN")
    if not token:
        raise ValueError("WEBEX_BOT_TOKEN missing in .env")
    return WebexAPI(access_token=token, proxies=PROXIES)

def build_preference_card(month_display):
    return {
        "contentType": "application/vnd.microsoft.card.adaptive",
        "content": {
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "type": "AdaptiveCard",
            "version": "1.2",
            "body": [
                {"type": "TextBlock", "text": f"📅 Preferences for {month_display}", "weight": "Bolder", "size": "Medium"},
                {"type": "TextBlock", "text": "Please submit your weekend shift preferences.", "wrap": True},
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

def send_preference_broadcast(team_id: int, year: int, month: int):
    api = get_webex_client()
    conn = get_db_connection()
    cur = conn.cursor()

    month_display = datetime(year, month, 1).strftime("%B %Y")
    card = build_preference_card(month_display)

    cur.execute("""
        SELECT name, webex_email
        FROM engineers
        WHERE team_id = %s AND is_active = TRUE AND webex_email IS NOT NULL
        ORDER BY name
    """, (team_id,))
    engineers = cur.fetchall()

    sent, failed = 0, 0
    for name, email in engineers:
        try:
            api.messages.create(
                toPersonEmail=email,
                text=f"Roster preference request for {month_display}",
                attachments=[card]
            )
            sent += 1
        except Exception:
            failed += 1

    cur.close()
    conn.close()
    return {"sent": sent, "failed": failed, "total": len(engineers)}

def publish_roster_for_month(team_id: int, year: int, month: int):
    api = get_webex_client()
    conn = get_db_connection()
    cur = conn.cursor()

    year_month = f"{year}-{month:02d}"
    month_display = datetime(year, month, 1).strftime("%B %Y")

    cur.execute("""
        SELECT e.id, e.name, e.webex_email
        FROM engineers e
        WHERE e.team_id = %s AND e.is_active = TRUE AND e.webex_email IS NOT NULL
        ORDER BY e.name
    """, (team_id,))
    engineers = cur.fetchall()

    sent, failed = 0, 0

    for eng_id, eng_name, eng_email in engineers:
        cur.execute("""
            SELECT shift_date
            FROM roster_assignments
            WHERE team_id = %s
              AND engineer_id = %s
              AND TO_CHAR(shift_date, 'YYYY-MM') = %s
            ORDER BY shift_date
        """, (team_id, eng_id, year_month))
        dates = [r[0] for r in cur.fetchall()]

        if dates:
            lines = "\n".join([f"- {d.strftime('%a, %d %b %Y')}" for d in dates])
            msg = (
                f"✅ **Your published roster for {month_display}**\n\n"
                f"{lines}\n\n"
                f"Please plan accordingly."
            )
        else:
            msg = (
                f"ℹ️ **Roster published for {month_display}**\n\n"
                f"You have no assigned weekend shifts this month."
            )

        try:
            api.messages.create(toPersonEmail=eng_email, markdown=msg)
            sent += 1
        except Exception:
            failed += 1

    cur.close()
    conn.close()
    return {"sent": sent, "failed": failed, "total": len(engineers)}