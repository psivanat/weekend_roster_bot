from flask import request
from flask_login import current_user
from psycopg2.extras import Json

def audit_log(conn, source, action, status="success", team_id=None, target_month=None,
              entity_type=None, entity_id=None, details=None, error_message=None):
    actor_user_id = None
    actor_name = None
    actor_role = None
    ip_address = None

    try:
        if current_user and not current_user.is_anonymous:
            actor_user_id = getattr(current_user, "id", None)
            actor_name = getattr(current_user, "username", None) or getattr(current_user, "name", None)
            actor_role = getattr(current_user, "role", None)
    except Exception:
        pass

    try:
        ip_address = request.headers.get("X-Forwarded-For", request.remote_addr)
    except Exception:
        pass

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO audit_logs
            (source, action, status, actor_user_id, actor_name, actor_role, team_id, target_month,
             entity_type, entity_id, details, error_message, ip_address)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            source, action, status, actor_user_id, actor_name, actor_role, team_id, target_month,
            entity_type, str(entity_id) if entity_id is not None else None,
            Json(details) if details is not None else None,
            error_message, ip_address
        ))