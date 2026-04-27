import os
import io
import calendar
from datetime import datetime, date
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, render_template, request, redirect, url_for, flash, session, send_file, jsonify
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from werkzeug.security import check_password_hash, generate_password_hash
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor
from audit_logger import audit_log
from scheduler_engine import generate_monthly_roster, get_weekend_dates
from webex_notify import send_preference_broadcast, publish_roster_for_month

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET", "super_secure_enterprise_key_change_in_production")

DB_PARAMS = {
    "host": os.getenv("DB_HOST", "localhost"),
    "database": os.getenv("DB_NAME", "roster_db"),
    "user": os.getenv("DB_USER", "roster_bot"),
    "password": os.getenv("DB_PASS")
}

def validate_weekend_days_input(raw_text, year, month):
    """
    raw_text: e.g. '2, 3, 9'
    Returns: (is_valid, cleaned_days, error_message)
    """
    if raw_text is None:
        return False, [], "Dates are required."

    parts = [p.strip() for p in raw_text.split(",") if p.strip()]
    if not parts:
        return False, [], "Please enter at least one date."

    last_day = calendar.monthrange(year, month)[1]
    seen = set()
    cleaned = []

    for p in parts:
        if not p.isdigit():
            return False, [], f"Invalid token '{p}'. Use only numbers separated by commas."
        d = int(p)
        if d < 1 or d > last_day:
            return False, [], f"Day {d} is out of range for {year}-{month:02d}."

        dt = datetime(year, month, d)
        if dt.weekday() not in (5, 6):  # 5=Sat, 6=Sun
            return False, [], f"{year}-{month:02d}-{d:02d} is not a Saturday/Sunday."

        if d not in seen:   # keep order, remove duplicates
            seen.add(d)
            cleaned.append(d)

    return True, cleaned, ""


def get_db_connection():
    return psycopg2.connect(**DB_PARAMS)


# ---------------- Auth ----------------

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login"


class User(UserMixin):
    def __init__(self, id, username, role, is_active_db):
        self.id = id
        self.username = username
        self.role = role
        self._is_active = is_active_db

    @property
    def is_active(self):
        return self._is_active


@login_manager.user_loader
def load_user(user_id):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT id, username, role, is_active FROM users WHERE id = %s", (user_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    if row and row["is_active"]:
        return User(row["id"], row["username"], row["role"], row["is_active"])
    return None


@app.context_processor
def inject_globals():
    now = datetime.now()
    allowed_teams = []
    active_team_id = session.get("active_team_id")
    active_team_name = "No Team Selected"

    if current_user.is_authenticated:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        if current_user.role == "super_admin":
            cur.execute("SELECT id, name FROM teams ORDER BY name")
        else:
            cur.execute("""
                SELECT t.id, t.name
                FROM teams t
                JOIN user_teams ut ON t.id = ut.team_id
                WHERE ut.user_id = %s
                ORDER BY t.name
            """, (current_user.id,))

        allowed_teams = cur.fetchall()

        if not active_team_id and allowed_teams:
            active_team_id = allowed_teams[0]["id"]
            session["active_team_id"] = active_team_id

        if active_team_id:
            for t in allowed_teams:
                if t["id"] == active_team_id:
                    active_team_name = t["name"]
                    break

        cur.close()
        conn.close()

    return dict(
        current_year=now.year,
        current_month=now.month,
        active_team_id=active_team_id,
        active_team_name=active_team_name,
        allowed_teams=allowed_teams
    )


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")

        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT * FROM users WHERE username = %s", (username,))
        row = cur.fetchone()
        cur.close()
        conn.close()

        if row and check_password_hash(row["password_hash"], password):
            if not row["is_active"]:
                flash("Account disabled. Contact administrator.", "error")
                return render_template("login.html")

            user = User(row["id"], row["username"], row["role"], row["is_active"])
            login_user(user)
            session.pop("active_team_id", None)
            return redirect(url_for("dashboard"))

        flash("Invalid username or password", "error")

    return render_template("login.html")


@app.route("/logout")
@login_required
def logout():
    logout_user()
    session.clear()
    return redirect(url_for("login"))


@app.route("/quick_switch_team", methods=["POST"])
@login_required
def quick_switch_team():
    session["active_team_id"] = int(request.form.get("team_id"))
    return redirect(request.referrer or url_for("dashboard"))


# ---------------- Core ----------------

@app.route("/")
@app.route("/dashboard")
@login_required
def dashboard():
    team_id = session.get("active_team_id")
    if not team_id:
        return render_template(
            "dashboard.html",
            engineers=[],
            dates=[],
            matrix={},
            selected_year=datetime.now().year,
            selected_month=datetime.now().month,
            month_name=""
        )

    year = int(request.args.get("year", datetime.now().year))
    month = int(request.args.get("month", datetime.now().month))
    ym = f"{year}-{month:02d}"

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("SELECT id, name FROM engineers WHERE team_id=%s AND is_active=TRUE ORDER BY name", (team_id,))
    engineers = cur.fetchall()

    date_strings = get_weekend_dates(year, month)
    dates = [datetime.strptime(d, "%Y-%m-%d") for d in date_strings]

    cur.execute("""
        SELECT shift_date, engineer_id
        FROM roster_assignments
        WHERE team_id=%s AND TO_CHAR(shift_date, 'YYYY-MM')=%s
    """, (team_id, ym))
    rows = cur.fetchall()

    cur.close()
    conn.close()

    matrix = {d: [] for d in date_strings}
    for r in rows:
        ds = r["shift_date"].strftime("%Y-%m-%d")
        if ds in matrix:
            matrix[ds].append(r["engineer_id"])

    return render_template(
        "dashboard.html",
        engineers=engineers,
        dates=dates,
        matrix=matrix,
        selected_year=year,
        selected_month=month,
        month_name=calendar.month_name[month]
    )


@app.route("/generate", methods=["POST"])
@login_required
def generate_roster():
    if current_user.role == "viewer":
        flash("You do not have permission to run the algorithm.", "error")
        return redirect(url_for("dashboard"))

    year = int(request.form.get("year"))
    month = int(request.form.get("month"))
    team_id = session.get("active_team_id")

    resp = generate_monthly_roster(year, month, team_id)
    flash(resp["message"], "success" if resp.get("success") else "error")
    return redirect(url_for("dashboard", year=year, month=month))

@app.route("/audit-logs")
@login_required
def audit_logs():
    team_id = session.get("active_team_id")
    if not team_id:
        flash("Select a team first.", "warning")
        return redirect(url_for("dashboard"))

    # filters
    source = request.args.get("source", "").strip()
    action = request.args.get("action", "").strip()
    status = request.args.get("status", "").strip()
    target_month = request.args.get("target_month", "").strip()
    limit = min(int(request.args.get("limit", 100)), 500)

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        q = """
            SELECT id, event_time, source, action, status, actor_name, actor_role,
                   team_id, target_month, entity_type, entity_id, details, error_message, ip_address
            FROM audit_logs
            WHERE team_id = %s
        """
        params = [team_id]

        if source:
            q += " AND source = %s"
            params.append(source)
        if action:
            q += " AND action = %s"
            params.append(action)
        if status:
            q += " AND status = %s"
            params.append(status)
        if target_month:
            q += " AND target_month = %s"
            params.append(target_month)

        q += " ORDER BY event_time DESC LIMIT %s"
        params.append(limit)

        cur.execute(q, tuple(params))
        logs = cur.fetchall()

        # dropdown values
        cur.execute("SELECT DISTINCT source FROM audit_logs WHERE team_id=%s ORDER BY source", (team_id,))
        sources = [r["source"] for r in cur.fetchall()]

        cur.execute("SELECT DISTINCT action FROM audit_logs WHERE team_id=%s ORDER BY action", (team_id,))
        actions = [r["action"] for r in cur.fetchall()]

    finally:
        cur.close()
        conn.close()

    return render_template(
        "audit_logs.html",
        logs=logs,
        sources=sources,
        actions=actions,
        filters={
            "source": source,
            "action": action,
            "status": status,
            "target_month": target_month,
            "limit": limit
        }
    )

@app.route("/admin/broadcast_preferences", methods=["POST"])
@login_required
def broadcast_preferences():
    if current_user.role == "viewer":
        flash("Permission denied.", "error")
        return redirect(url_for("dashboard"))

    team_id = session.get("active_team_id")
    year = int(request.form.get("year"))
    month = int(request.form.get("month"))

    try:
        result = send_preference_broadcast(team_id, year, month)
        flash(f"Broadcast sent. Total={result['total']}, Sent={result['sent']}, Failed={result['failed']}", "success")
    except Exception as e:
        flash(f"Broadcast failed: {e}", "error")

    return redirect(url_for("dashboard", year=year, month=month))


@app.route("/admin/publish_roster", methods=["POST"])
@login_required
def publish_roster():
    if current_user.role == "viewer":
        flash("Permission denied.", "error")
        return redirect(url_for("dashboard"))

    team_id = session.get("active_team_id")
    year = int(request.form.get("year"))
    month = int(request.form.get("month"))

    try:
        result = publish_roster_for_month(team_id, year, month)
        flash(f"Roster published. Total={result['total']}, Sent={result['sent']}, Failed={result['failed']}", "success")
    except Exception as e:
        flash(f"Publish failed: {e}", "error")

    return redirect(url_for("dashboard", year=year, month=month))


@app.route("/manual_override", methods=["POST"])
@login_required
def manual_override():
    if current_user.role == "viewer":
        flash("Permission denied.", "error")
        return redirect(url_for("dashboard"))

    team_id = session.get("active_team_id")
    shift_date = request.form.get("shift_date")
    engineer_id = request.form.get("engineer_id")
    action = request.form.get("action")

    dt = datetime.strptime(shift_date, "%Y-%m-%d")
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        if action == "add":
            cur.execute("""
                INSERT INTO roster_assignments (shift_date, team_id, engineer_id)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (shift_date, team_id, engineer_id))
            flash("Engineer added to shift.", "success")
        elif action == "remove":
            cur.execute("""
                DELETE FROM roster_assignments
                WHERE shift_date=%s AND team_id=%s AND engineer_id=%s
            """, (shift_date, team_id, engineer_id))
            flash("Engineer removed from shift.", "success")
        conn.commit()
    except Exception as e:
        conn.rollback()
        flash(f"Database error: {e}", "error")
    finally:
        cur.close()
        conn.close()

    return redirect(url_for("dashboard", year=dt.year, month=dt.month))


@app.route("/api/move_shift", methods=["POST"])
@login_required
def move_shift():
    if current_user.role == "viewer":
        return jsonify({"success": False, "message": "Permission denied"}), 403

    data = request.json
    team_id = session.get("active_team_id")
    old_eng_id = data.get("old_eng_id")
    old_date = data.get("old_date")
    new_eng_id = data.get("new_eng_id")
    new_date = data.get("new_date")

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM roster_assignments WHERE team_id=%s AND engineer_id=%s AND shift_date=%s",
                    (team_id, old_eng_id, old_date))
        cur.execute("""
            INSERT INTO roster_assignments (shift_date, team_id, engineer_id)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (new_date, team_id, new_eng_id))
        conn.commit()
        return jsonify({"success": True})
    except Exception as e:
        conn.rollback()
        return jsonify({"success": False, "message": str(e)}), 500
    finally:
        cur.close()
        conn.close()


@app.route("/export_csv")
@login_required
def export_csv():
    team_id = session.get("active_team_id")
    year = request.args.get("year")
    month = int(request.args.get("month"))
    ym = f"{year}-{month:02d}"

    conn = get_db_connection()
    q = """
        SELECT r.shift_date as "Date", STRING_AGG(e.name, ', ') as "Assigned Engineers"
        FROM roster_assignments r
        JOIN engineers e ON r.engineer_id = e.id
        WHERE r.team_id=%s AND TO_CHAR(r.shift_date, 'YYYY-MM')=%s
        GROUP BY r.shift_date
        ORDER BY r.shift_date
    """
    df = pd.read_sql(q, conn, params=(team_id, ym))
    conn.close()

    out = io.StringIO()
    df.to_csv(out, index=False)
    out.seek(0)

    return send_file(
        io.BytesIO(out.getvalue().encode("utf-8")),
        mimetype="text/csv",
        as_attachment=True,
        download_name=f"Roster_{ym}.csv"
    )


# ---------------- Engineers ----------------

@app.route("/engineers", methods=["GET", "POST"])
@login_required
def manage_engineers():
    team_id = session.get("active_team_id")
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    if request.method == "POST" and current_user.role != "viewer":
        action = request.form.get("action")

        if action == "add":
            name = request.form.get("name")
            email = request.form.get("email")
            max_shifts = request.form.get("max_shifts", 3)
            cur.execute("""
                INSERT INTO engineers (name, webex_email, team_id, max_shifts)
                VALUES (%s, %s, %s, %s)
            """, (name, email, team_id, max_shifts))
            flash("Engineer added.", "success")

        elif action == "edit":
            eng_id = request.form.get("eng_id")
            max_shifts = request.form.get("max_shifts")
            is_active = request.form.get("is_active") == "on"
            cur.execute("""
                UPDATE engineers
                SET max_shifts=%s, is_active=%s
                WHERE id=%s AND team_id=%s
            """, (max_shifts, is_active, eng_id, team_id))
            flash("Engineer updated.", "success")

        elif action == "delete":
            eng_id = request.form.get("eng_id")
            cur.execute("DELETE FROM engineers WHERE id=%s AND team_id=%s", (eng_id, team_id))
            flash("Engineer deleted.", "success")

        conn.commit()

    cur.execute("SELECT * FROM engineers WHERE team_id=%s ORDER BY name", (team_id,))
    engineers = cur.fetchall()
    cur.close()
    conn.close()

    return render_template("engineers.html", engineers=engineers)


# ---------------- Availability (Order-Preserving) ----------------

@app.route("/availability", methods=["GET", "POST"])
@login_required
def manage_availability():
    team_id = session.get("active_team_id")
    year = int(request.args.get("year", datetime.now().year))
    month = int(request.args.get("month", datetime.now().month))

    # preferences.target_month is VARCHAR/TEXT like "2026-05"
    target_month_key = f"{year}-{month:02d}"
    ym_str = target_month_key
    _, last_day = calendar.monthrange(year, month)

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        if request.method == "POST" and current_user.role != "viewer":
            try:
                action = request.form.get("action")
                eng_id = request.form.get("engineer_id")

                if action == "update_prefs":
                    prefs = request.form.get("preferences", "").strip()

                    if prefs:
                        tokens = [x.strip() for x in prefs.split(",") if x.strip()]
                        days_int = []
                        seen = set()
                        invalid_tokens = []
                        non_weekend_days = []

                        for t in tokens:
                            if not t.isdigit():
                                invalid_tokens.append(t)
                                continue

                            d = int(t)
                            if not (1 <= d <= last_day):
                                invalid_tokens.append(t)
                                continue

                            dt = datetime(year, month, d)
                            if dt.weekday() not in (5, 6):  # 5=Saturday, 6=Sunday
                                non_weekend_days.append(d)
                                continue

                            if d not in seen:  # preserve input order, remove duplicates
                                seen.add(d)
                                days_int.append(d)

                        if invalid_tokens:
                            flash(
                                f"Invalid date values: {', '.join(invalid_tokens)}. "
                                f"Enter day numbers between 1 and {last_day}.",
                                "warning"
                            )
                        elif non_weekend_days:
                            bad = ", ".join(str(d) for d in non_weekend_days)
                            flash(
                                f"Only Saturday/Sunday dates are allowed. Not weekend: {bad}.",
                                "warning"
                            )
                        elif not days_int:
                            flash("No valid weekend dates found for selected month.", "warning")
                        else:
                            dates = [f"{year}-{month:02d}-{d:02d}" for d in days_int]  # ranking order preserved
                            preferred_count = max(1, len(dates) - 2)

                            cur.execute("""
                                INSERT INTO preferences (
                                    engineer_id, target_month, status, preferred_count, priority_dates, updated_at
                                )
                                VALUES (%s, %s, 'submitted', %s, %s::date[], CURRENT_TIMESTAMP)
                                ON CONFLICT (engineer_id, target_month)
                                DO UPDATE SET
                                    status='submitted',
                                    preferred_count=EXCLUDED.preferred_count,
                                    priority_dates=EXCLUDED.priority_dates,
                                    updated_at=CURRENT_TIMESTAMP
                            """, (eng_id, target_month_key, preferred_count, dates))
                            audit_log(
                                conn, source="gui", action="UPDATE_PREFS", status="success",
                                team_id=team_id, target_month=target_month_key,
                                entity_type="preferences", entity_id=eng_id,
                                details={"preferences_raw": prefs, "saved_dates": dates, "preferred_count": preferred_count}
                            )
                            conn.commit()                         
                            flash("Preferences updated.", "success")

                    else:
                        cur.execute("""
                            DELETE FROM preferences
                            WHERE engineer_id=%s AND target_month=%s
                        """, (eng_id, target_month_key))
                        audit_log(
                            conn, source="gui", action="CLEAR_PREFS", status="success",
                            team_id=team_id, target_month=target_month_key,
                            entity_type="preferences", entity_id=eng_id
                        )
                        conn.commit()
                        flash("Preferences cleared.", "success")

                elif action == "add_leave":
                    leave_date = request.form.get("leave_date")
                    cur.execute("""
                        INSERT INTO leave_blockouts (engineer_id, block_date)
                        VALUES (%s, %s)
                        ON CONFLICT DO NOTHING
                    """, (eng_id, leave_date))
                    
                    flash("Leave blockout added.", "success")

                elif action == "delete_leave":
                    leave_id = request.form.get("leave_id")
                    cur.execute("DELETE FROM leave_blockouts WHERE id=%s", (leave_id,))
                    flash("Leave blockout removed.", "success")
                audit_log(
                    conn, source="gui", action=action or "UNKNOWN_ACTION", status="failed",
                    team_id=team_id, target_month=target_month_key,
                    entity_type="preferences", entity_id=eng_id,
                    error_message=str(e)
                    conn.commit()
                    
                )
                conn.commit()

            except Exception as ex:
                conn.rollback()
                flash(f"Save failed: {ex}", "error")

        # Engineers for forms
        cur.execute("""
            SELECT id, name
            FROM engineers
            WHERE team_id=%s AND is_active=TRUE
            ORDER BY name
        """, (team_id,))
        engineers = cur.fetchall()

        # Submitted preferences table - preserve stored array order in display
        cur.execute("""
            SELECT
                e.id AS engineer_id,
                e.name,
                COALESCE(
                    array_to_string(
                        ARRAY(
                            SELECT EXTRACT(DAY FROM u.d)::int::text
                            FROM unnest(p.priority_dates) WITH ORDINALITY AS u(d, ord)
                            ORDER BY u.ord
                        ),
                        ', '
                    ),
                    ''
                ) AS preferences
            FROM engineers e
            LEFT JOIN preferences p
                ON e.id = p.engineer_id
               AND p.target_month = %s
               AND p.status = 'submitted'
            WHERE e.team_id=%s AND e.is_active=TRUE
            ORDER BY e.name
        """, (target_month_key, team_id))
        avail_data = cur.fetchall()

        # Leave blockouts
        cur.execute("""
            SELECT l.id, e.name, l.block_date
            FROM leave_blockouts l
            JOIN engineers e ON l.engineer_id = e.id
            WHERE e.team_id=%s AND TO_CHAR(l.block_date, 'YYYY-MM')=%s
            ORDER BY l.block_date
        """, (team_id, ym_str))
        leaves = cur.fetchall()

    finally:
        cur.close()
        conn.close()

    return render_template(
        "availability.html",
        engineers=engineers,
        avail_data=avail_data,
        leaves=leaves,
        selected_year=year,
        selected_month=month
    )

# ---------------- Settings / Analytics / Superadmin ----------------

@app.route("/settings", methods=["GET", "POST"])
@login_required
def settings():
    if current_user.role == "viewer":
        flash("Permission denied.", "error")
        return redirect(url_for("dashboard"))

    team_id = session.get("active_team_id")
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    if request.method == "POST":
        cur.execute("""
            UPDATE teams
            SET sat_coverage=%s, sun_coverage=%s, min_preferences=%s, shift_start_time=%s, shift_end_time=%s
            WHERE id=%s
        """, (
            request.form.get("sat_coverage"),
            request.form.get("sun_coverage"),
            request.form.get("min_preferences"),
            request.form.get("shift_start_time"),
            request.form.get("shift_end_time"),
            team_id
        ))
        conn.commit()
        flash("Team settings updated.", "success")

    cur.execute("SELECT * FROM teams WHERE id=%s", (team_id,))
    team_settings = cur.fetchone()
    cur.close()
    conn.close()

    return render_template("settings.html", settings=team_settings)


@app.route("/analytics")
@login_required
def analytics():
    team_id = session.get("active_team_id")
    year = request.args.get("year", datetime.now().year)

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT e.name, COUNT(r.shift_date) as total_shifts
        FROM engineers e
        LEFT JOIN roster_assignments r
               ON e.id = r.engineer_id
              AND EXTRACT(YEAR FROM r.shift_date) = %s
        WHERE e.team_id=%s AND e.is_active=TRUE
        GROUP BY e.name
        ORDER BY total_shifts DESC
    """, (year, team_id))
    stats = cur.fetchall()
    cur.close()
    conn.close()

    labels = [r["name"] for r in stats]
    data = [r["total_shifts"] for r in stats]

    return render_template("analytics.html", stats=stats, selected_year=year, labels=labels, data=data)


@app.route("/superadmin", methods=["GET", "POST"])
@login_required
def superadmin():
    if current_user.role != "super_admin":
        flash("Super Admin access required.", "error")
        return redirect(url_for("dashboard"))

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    if request.method == "POST":
        action = request.form.get("action")

        if action == "create_team":
            cur.execute("INSERT INTO teams (name) VALUES (%s)", (request.form.get("team_name"),))
            flash("Team created.", "success")

        elif action == "create_user":
            username = request.form.get("username")
            password_hash = generate_password_hash(request.form.get("password"))
            role = request.form.get("role")
            team_ids = request.form.getlist("team_ids")

            cur.execute("""
                INSERT INTO users (username, password_hash, role)
                VALUES (%s, %s, %s)
                RETURNING id
            """, (username, password_hash, role))
            new_user_id = cur.fetchone()["id"]

            if role != "super_admin":
                for t in team_ids:
                    cur.execute("INSERT INTO user_teams (user_id, team_id) VALUES (%s, %s)", (new_user_id, t))
            flash("User created successfully.", "success")

        elif action == "delete_team":
            t_id = request.form.get("team_id")
            cur.execute("DELETE FROM teams WHERE id=%s", (t_id,))
            flash("Team and associated data deleted.", "success")

        elif action == "update_user":
            user_id = request.form.get("user_id")
            new_password = request.form.get("new_password")
            is_active = request.form.get("is_active") == "on"
            team_ids = request.form.getlist("team_ids")

            cur.execute("UPDATE users SET is_active=%s WHERE id=%s", (is_active, user_id))
            if new_password:
                cur.execute("UPDATE users SET password_hash=%s WHERE id=%s",
                            (generate_password_hash(new_password), user_id))

            cur.execute("DELETE FROM user_teams WHERE user_id=%s", (user_id,))
            for t in team_ids:
                cur.execute("INSERT INTO user_teams (user_id, team_id) VALUES (%s, %s)", (user_id, t))

            flash("User updated successfully.", "success")

        conn.commit()

    cur.execute("SELECT * FROM teams ORDER BY name")
    all_teams = cur.fetchall()

    cur.execute("SELECT id, username, role, is_active FROM users ORDER BY id")
    all_users = cur.fetchall()

    for u in all_users:
        cur.execute("SELECT team_id FROM user_teams WHERE user_id=%s", (u["id"],))
        u["team_ids"] = [r["team_id"] for r in cur.fetchall()]

    cur.close()
    conn.close()

    return render_template("superadmin.html", all_teams=all_teams, all_users=all_users)


@app.route("/manual")
@login_required
def manual():
    return render_template("manual.html")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)