import os
import requests
from datetime import datetime
import calendar
import io
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, render_template, request, redirect, url_for, flash, session, send_file
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from werkzeug.security import check_password_hash, generate_password_hash
from dotenv import load_dotenv

# Import the isolated backend engine
from scheduler_engine import generate_monthly_roster, get_weekend_dates

load_dotenv()
app = Flask(__name__)
app.secret_key = "super_secure_enterprise_key_change_in_production"

# ==========================================
# DATABASE CONNECTION
# ==========================================
DB_PARAMS = {
    "host": os.getenv("DB_HOST", "localhost"),
    "database": os.getenv("DB_NAME", "roster_db"),
    "user": os.getenv("DB_USER", "roster_bot"),
    "password": os.getenv("DB_PASS")
}

def get_db_connection():
    return psycopg2.connect(**DB_PARAMS)

# ==========================================
# AUTHENTICATION & FLASK-LOGIN SETUP
# ==========================================
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

class User(UserMixin):
    def __init__(self, id, username, role, is_active_db):
        self.id = id
        self.username = username
        self.role = role
        self._is_active = is_active_db  # Use a private variable name here

    @property
    def is_active(self):
        # Override Flask-Login's default is_active property
        return self._is_active

@login_manager.user_loader
def load_user(user_id):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT id, username, role, is_active FROM users WHERE id = %s", (user_id,))
    user_data = cursor.fetchone()
    cursor.close()
    conn.close()
    if user_data and user_data['is_active']:
        return User(user_data['id'], user_data['username'], user_data['role'], user_data['is_active'])
    return None

@app.context_processor
def inject_globals():
    now = datetime.now()
    allowed_teams = []
    active_team_id = session.get('active_team_id')
    active_team_name = "No Team Selected"
    
    if current_user.is_authenticated:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        if current_user.role == 'super_admin':
            cursor.execute("SELECT id, name FROM teams ORDER BY name")
        else:
            cursor.execute("""
                SELECT t.id, t.name FROM teams t
                JOIN user_teams ut ON t.id = ut.team_id
                WHERE ut.user_id = %s ORDER BY t.name
            """, (current_user.id,))
            
        allowed_teams = cursor.fetchall()
        
        if not active_team_id and allowed_teams:
            active_team_id = allowed_teams[0]['id']
            session['active_team_id'] = active_team_id
            
        if active_team_id:
            for t in allowed_teams:
                if t['id'] == active_team_id:
                    active_team_name = t['name']
                    break
                    
        cursor.close()
        conn.close()

    return dict(
        current_year=now.year, 
        current_month=now.month,
        active_team_id=active_team_id,
        active_team_name=active_team_name,
        allowed_teams=allowed_teams
    )

# ==========================================
# ROUTES: AUTHENTICATION
# ==========================================
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SELECT * FROM users WHERE username = %s", (username,))
        user_data = cursor.fetchone()
        cursor.close()
        conn.close()

        if user_data and check_password_hash(user_data['password_hash'], password):
            if not user_data['is_active']:
                flash('Account disabled. Contact administrator.', 'error')
                return render_template('login.html')
                
            user = User(user_data['id'], user_data['username'], user_data['role'], user_data['is_active'])
            login_user(user)
            session.pop('active_team_id', None)
            return redirect(url_for('dashboard'))
        else:
            flash('Invalid username or password', 'error')
            
    return render_template('login.html')

@app.route('/logout')
@login_required
def logout():
    logout_user()
    session.clear()
    return redirect(url_for('login'))

@app.route('/quick_switch_team', methods=['POST'])
@login_required
def quick_switch_team():
    team_id = int(request.form.get('team_id'))
    session['active_team_id'] = team_id
    return redirect(request.referrer or url_for('dashboard'))

# ==========================================
# WEBEX BOT INTEGRATION
# ==========================================

# Cisco Corporate Proxy (Required for outgoing API calls from the server)
cisco_proxies = {
    "http": "http://proxy-wsa.esl.cisco.com:80",
    "https": "http://proxy-wsa.esl.cisco.com:80"
}

@app.route('/api/webex/webhook', methods=['POST'])
def webex_webhook():
    data = request.json
    
    # 1. Ignore messages sent by the bot itself (prevents infinite loops)
    bot_email = os.getenv('WEBEX_BOT_EMAIL')
    sender_email = data.get('data', {}).get('personEmail')
    if sender_email == bot_email:
        return jsonify({'status': 'ignored'}), 200
        
    # 2. Extract IDs
    message_id = data.get('data', {}).get('id')
    room_id = data.get('data', {}).get('roomId')
    
    if not message_id:
        return jsonify({'status': 'no message id'}), 400
        
    # 3. Webex webhooks don't include the message text for security. 
    # We must fetch the text using the message_id.
    token = os.getenv('WEBEX_BOT_TOKEN')
    headers = {'Authorization': f'Bearer {token}'}
    
    msg_resp = requests.get(
        f"https://webexapis.com/v1/messages/{message_id}", 
        headers=headers, 
        proxies=cisco_proxies # Use proxies for Cisco network
    )
    
    if msg_resp.status_code == 200:
        # Clean up the message text
        message_text = msg_resp.json().get('text', '').strip().lower()
        
        # 4. Command Routing Logic
        reply_text = "I didn't understand that command. Try typing **help**."
        
        if "help" in message_text:
            reply_text = (
                "🤖 **Weekend Roster Bot**\n\n"
                "Available commands:\n"
                "- `help`: Show this menu\n"
                "- `roster`: Show who is working this weekend (Coming soon!)\n"
                "- `status`: Check if the bot is online"
            )
        elif "status" in message_text:
            reply_text = "✅ Roster Bot is online and connected to the database!"
            
        # 5. Send the reply back to the Webex Room
        payload = {
            'roomId': room_id, 
            'markdown': reply_text
        }
        requests.post(
            "https://webexapis.com/v1/messages", 
            headers=headers, 
            json=payload, 
            proxies=cisco_proxies
        )
        
    return jsonify({'status': 'success'}), 200

# ==========================================
# ROUTES: CORE APPLICATION
# ==========================================
@app.route('/')
@app.route('/dashboard')
@login_required
def dashboard():
    team_id = session.get('active_team_id')
    if not team_id:
        return render_template('dashboard.html', engineers=[], dates=[], matrix={}, selected_year=datetime.now().year, selected_month=datetime.now().month, month_name="")

    year = int(request.args.get('year', datetime.now().year))
    month = int(request.args.get('month', datetime.now().month))
    year_month_str = f"{year}-{month:02d}"
    
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    cursor.execute("SELECT id, name FROM engineers WHERE team_id = %s AND is_active = TRUE ORDER BY name", (team_id,))
    engineers = cursor.fetchall()
    
    date_strings = get_weekend_dates(year, month)
    dates = [datetime.strptime(d, "%Y-%m-%d") for d in date_strings]
    
    cursor.execute("""
        SELECT shift_date, engineer_id 
        FROM roster_assignments 
        WHERE team_id = %s AND TO_CHAR(shift_date, 'YYYY-MM') = %s
    """, (team_id, year_month_str))
    
    assignments = cursor.fetchall()
    cursor.close()
    conn.close()
    
    matrix = {d: [] for d in date_strings}
    for row in assignments:
        date_str = row['shift_date'].strftime('%Y-%m-%d')
        if date_str in matrix:
            matrix[date_str].append(row['engineer_id'])

    return render_template('dashboard.html', 
                           engineers=engineers, 
                           dates=dates, 
                           matrix=matrix, 
                           selected_year=year, 
                           selected_month=month,
                           month_name=calendar.month_name[month])

@app.route('/generate', methods=['POST'])
@login_required
def generate_roster():
    if current_user.role == 'viewer':
        flash("You do not have permission to run the algorithm.", "error")
        return redirect(url_for('dashboard'))
        
    year = int(request.form.get('year'))
    month = int(request.form.get('month'))
    team_id = session.get('active_team_id')
    
    response = generate_monthly_roster(year, month, team_id)
    
    if response["success"]:
        flash(f"{response['message']}", "success")
    else:
        flash(response["message"], "error")
        
    return redirect(url_for('dashboard', year=year, month=month))

@app.route('/manual_override', methods=['POST'])
@login_required
def manual_override():
    if current_user.role == 'viewer':
        flash("Permission denied.", "error")
        return redirect(url_for('dashboard'))
        
    team_id = session.get('active_team_id')
    shift_date = request.form.get('shift_date')
    engineer_id = request.form.get('engineer_id')
    action = request.form.get('action')
    
    dt_obj = datetime.strptime(shift_date, "%Y-%m-%d")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        if action == 'add':
            cursor.execute("INSERT INTO roster_assignments (shift_date, team_id, engineer_id) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", (shift_date, team_id, engineer_id))
            flash("Engineer added to shift.", "success")
        elif action == 'remove':
            cursor.execute("DELETE FROM roster_assignments WHERE shift_date = %s AND team_id = %s AND engineer_id = %s", (shift_date, team_id, engineer_id))
            flash("Engineer removed from shift.", "success")
        conn.commit()
    except Exception as e:
        conn.rollback()
        flash(f"Database error: {e}", "error")
    finally:
        cursor.close()
        conn.close()
        
    return redirect(url_for('dashboard', year=dt_obj.year, month=dt_obj.month))

@app.route('/api/move_shift', methods=['POST'])
@login_required
def move_shift():
    """Silent API route for Drag-and-Drop functionality in the UI."""
    if current_user.role == 'viewer':
        return {"success": False, "message": "Permission denied."}, 403

    data = request.json
    team_id = session.get('active_team_id')
    old_eng_id = data.get('old_eng_id')
    old_date = data.get('old_date')
    new_eng_id = data.get('new_eng_id')
    new_date = data.get('new_date')

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Remove from old cell
        cursor.execute("DELETE FROM roster_assignments WHERE team_id=%s AND engineer_id=%s AND shift_date=%s", (team_id, old_eng_id, old_date))
        # Insert into new cell
        cursor.execute("INSERT INTO roster_assignments (shift_date, team_id, engineer_id) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", (new_date, team_id, new_eng_id))
        conn.commit()
        return {"success": True}
    except Exception as e:
        conn.rollback()
        return {"success": False, "message": str(e)}, 500
    finally:
        cursor.close()
        conn.close()

@app.route('/export_csv')
@login_required
def export_csv():
    team_id = session.get('active_team_id')
    year = request.args.get('year')
    month = request.args.get('month')
    year_month_str = f"{year}-{int(month):02d}"
    
    conn = get_db_connection()
    query = """
        SELECT r.shift_date as "Date", STRING_AGG(e.name, ', ') as "Assigned Engineers"
        FROM roster_assignments r
        JOIN engineers e ON r.engineer_id = e.id
        WHERE r.team_id = %s AND TO_CHAR(r.shift_date, 'YYYY-MM') = %s
        GROUP BY r.shift_date ORDER BY r.shift_date ASC
    """
    df = pd.read_sql(query, conn, params=(team_id, year_month_str))
    conn.close()
    
    output = io.StringIO()
    df.to_csv(output, index=False)
    output.seek(0)
    
    return send_file(
        io.BytesIO(output.getvalue().encode('utf-8')),
        mimetype='text/csv',
        as_attachment=True,
        download_name=f"Roster_{year_month_str}.csv"
    )

# ==========================================
# ROUTES: MANAGEMENT PAGES
# ==========================================
@app.route('/engineers', methods=['GET', 'POST'])
@login_required
def manage_engineers():
    team_id = session.get('active_team_id')
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    if request.method == 'POST' and current_user.role != 'viewer':
        action = request.form.get('action')
        if action == 'add':
            name = request.form.get('name')
            email = request.form.get('email')
            max_shifts = request.form.get('max_shifts', 3)
            cursor.execute("INSERT INTO engineers (name, webex_email, team_id, max_shifts) VALUES (%s, %s, %s, %s)", (name, email, team_id, max_shifts))
            flash("Engineer added.", "success")
        elif action == 'edit':
            eng_id = request.form.get('eng_id')
            max_shifts = request.form.get('max_shifts')
            is_active = request.form.get('is_active') == 'on'
            cursor.execute("UPDATE engineers SET max_shifts = %s, is_active = %s WHERE id = %s AND team_id = %s", (max_shifts, is_active, eng_id, team_id))
            flash("Engineer updated.", "success")
        # --- NEW DELETE LOGIC ---
        elif action == 'delete':
            eng_id = request.form.get('eng_id')
            # Because of ON DELETE CASCADE in the DB, this safely removes their roster & availability too!
            cursor.execute("DELETE FROM engineers WHERE id = %s AND team_id = %s", (eng_id, team_id))
            flash("Engineer deleted successfully.", "success")
        conn.commit()

    cursor.execute("SELECT * FROM engineers WHERE team_id = %s ORDER BY name", (team_id,))
    engineers = cursor.fetchall()
    cursor.close()
    conn.close()
    return render_template('engineers.html', engineers=engineers)

@app.route('/availability', methods=['GET', 'POST'])
@login_required
def manage_availability():
    team_id = session.get('active_team_id')
    year = int(request.args.get('year', datetime.now().year))
    month = int(request.args.get('month', datetime.now().month))
    year_month_str = f"{year}-{month:02d}"
    
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    if request.method == 'POST' and current_user.role != 'viewer':
        eng_id = request.form.get('engineer_id')
        action = request.form.get('action')
        
        if action == 'update_prefs':
            prefs = request.form.get('preferences')
            if prefs.strip():
                cursor.execute("""
                    INSERT INTO availability (engineer_id, year_month, preferences) VALUES (%s, %s, %s)
                    ON CONFLICT (engineer_id, year_month) DO UPDATE SET preferences = EXCLUDED.preferences
                """, (eng_id, year_month_str, prefs))
            else:
                cursor.execute("DELETE FROM availability WHERE engineer_id = %s AND year_month = %s", (eng_id, year_month_str))
            flash("Preferences updated.", "success")
            
        elif action == 'add_leave':
            leave_date = request.form.get('leave_date')
            cursor.execute("INSERT INTO leave_blockouts (engineer_id, block_date) VALUES (%s, %s) ON CONFLICT DO NOTHING", (eng_id, leave_date))
            flash("Leave blockout added.", "success")
            
        elif action == 'delete_leave':
            leave_id = request.form.get('leave_id')
            cursor.execute("DELETE FROM leave_blockouts WHERE id = %s", (leave_id,))
            flash("Leave blockout removed.", "success")
            
        conn.commit()

    cursor.execute("SELECT id, name FROM engineers WHERE team_id = %s AND is_active = TRUE ORDER BY name", (team_id,))
    engineers = cursor.fetchall()
    
    cursor.execute("""
        SELECT e.id as engineer_id, e.name, a.preferences 
        FROM engineers e 
        LEFT JOIN availability a ON e.id = a.engineer_id AND a.year_month = %s 
        WHERE e.team_id = %s AND e.is_active = TRUE
        ORDER BY e.name
    """, (year_month_str, team_id))
    avail_data = cursor.fetchall()
    
    cursor.execute("""
        SELECT l.id, e.name, l.block_date FROM leave_blockouts l
        JOIN engineers e ON l.engineer_id = e.id
        WHERE e.team_id = %s AND TO_CHAR(l.block_date, 'YYYY-MM') = %s
        ORDER BY l.block_date
    """, (team_id, year_month_str))
    leaves = cursor.fetchall()
    
    cursor.close()
    conn.close()
    return render_template('availability.html', engineers=engineers, avail_data=avail_data, leaves=leaves, selected_year=year, selected_month=month)

@app.route('/settings', methods=['GET', 'POST'])
@login_required
def settings():
    if current_user.role == 'viewer':
        flash("Permission denied.", "error")
        return redirect(url_for('dashboard'))
        
    team_id = session.get('active_team_id')
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    if request.method == 'POST':
        sat_cov = request.form.get('sat_coverage')
        sun_cov = request.form.get('sun_coverage')
        min_pref = request.form.get('min_preferences')
        start_time = request.form.get('shift_start_time')
        end_time = request.form.get('shift_end_time')
        
        cursor.execute("""
            UPDATE teams SET sat_coverage=%s, sun_coverage=%s, min_preferences=%s, shift_start_time=%s, shift_end_time=%s WHERE id=%s
        """, (sat_cov, sun_cov, min_pref, start_time, end_time, team_id))
        conn.commit()
        flash("Team settings updated.", "success")
        
    cursor.execute("SELECT * FROM teams WHERE id = %s", (team_id,))
    team_settings = cursor.fetchone()
    cursor.close()
    conn.close()
    
    return render_template('settings.html', settings=team_settings)

@app.route('/analytics')
@login_required
def analytics():
    team_id = session.get('active_team_id')
    year = request.args.get('year', datetime.now().year)
    
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("""
        SELECT e.name, COUNT(r.shift_date) as total_shifts
        FROM engineers e
        LEFT JOIN roster_assignments r ON e.id = r.engineer_id AND EXTRACT(YEAR FROM r.shift_date) = %s
        WHERE e.team_id = %s AND e.is_active = TRUE
        GROUP BY e.name ORDER BY total_shifts DESC
    """, (year, team_id))
    stats = cursor.fetchall()
    cursor.close()
    conn.close()
    
    # Format data for Chart.js
    labels = [row['name'] for row in stats]
    data = [row['total_shifts'] for row in stats]
    
    return render_template('analytics.html', stats=stats, selected_year=year, labels=labels, data=data)

@app.route('/superadmin', methods=['GET', 'POST'])
@login_required
def superadmin():
    if current_user.role != 'super_admin':
        flash("Super Admin access required.", "error")
        return redirect(url_for('dashboard'))
        
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    if request.method == 'POST':
        action = request.form.get('action')
        
        if action == 'create_team':
            team_name = request.form.get('team_name')
            cursor.execute("INSERT INTO teams (name) VALUES (%s)", (team_name,))
            flash("Team created.", "success")
            
        elif action == 'create_user':
            username = request.form.get('username')
            password = generate_password_hash(request.form.get('password'))
            role = request.form.get('role')
            team_ids = request.form.getlist('team_ids')
        
        elif action == 'delete_team':
            t_id = request.form.get('team_id')
            cursor.execute("DELETE FROM teams WHERE id = %s", (t_id,))
            flash("Team and all associated data deleted.", "success")
            
            cursor.execute("INSERT INTO users (username, password_hash, role) VALUES (%s, %s, %s) RETURNING id", (username, password, role))
            new_user_id = cursor.fetchone()['id']
            
            if role != 'super_admin':
                for t_id in team_ids:
                    cursor.execute("INSERT INTO user_teams (user_id, team_id) VALUES (%s, %s)", (new_user_id, t_id))
            flash("User created successfully.", "success")
            
        elif action == 'update_user':
            user_id = request.form.get('user_id')
            new_password = request.form.get('new_password')
            is_active = request.form.get('is_active') == 'on'
            team_ids = request.form.getlist('team_ids')
            
            cursor.execute("UPDATE users SET is_active = %s WHERE id = %s", (is_active, user_id))
            if new_password:
                cursor.execute("UPDATE users SET password_hash = %s WHERE id = %s", (generate_password_hash(new_password), user_id))
            
            cursor.execute("DELETE FROM user_teams WHERE user_id = %s", (user_id,))
            for t_id in team_ids:
                cursor.execute("INSERT INTO user_teams (user_id, team_id) VALUES (%s, %s)", (user_id, t_id))
                
            flash("User updated successfully.", "success")
            
        conn.commit()

    cursor.execute("SELECT * FROM teams ORDER BY name")
    all_teams = cursor.fetchall()
    
    cursor.execute("SELECT id, username, role, is_active FROM users ORDER BY id")
    all_users = cursor.fetchall()
    
    for u in all_users:
        cursor.execute("SELECT team_id FROM user_teams WHERE user_id = %s", (u['id'],))
        u['team_ids'] = [row['team_id'] for row in cursor.fetchall()]
    
    cursor.close()
    conn.close()
    
    return render_template('superadmin.html', all_teams=all_teams, all_users=all_users)

@app.route('/manual')
@login_required
def manual():
    return render_template('manual.html')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
