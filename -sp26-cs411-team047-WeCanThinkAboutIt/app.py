"""
GradPath - Flask Web Application
Helps students make informed career decisions by connecting their academic
profile to real job market data stored in a MySQL database on GCP.
"""

from flask import Flask, render_template, request, redirect, url_for, jsonify, session, flash
import mysql.connector
from mysql.connector import Error

app = Flask(__name__)
app.secret_key = "gradpath_secret_key_2024"

# ---------------------------------------------------------------------------
# Database Configuration
# ---------------------------------------------------------------------------
DB_CONFIG = {
    "host": "34.61.85.176",
    "database": "gradpath",
    "user": "root",
    "password": "root123456",
    "connect_timeout": 10,
}


def get_db_connection():
    """Open and return a new MySQL database connection."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except Error as e:
        print(f"Database connection error: {e}")
        return None


def query_db(sql, params=None, fetch_one=False):
    """
    Execute a SELECT query and return results.
    fetch_one=True returns a single row dict; False returns a list of dicts.
    """
    conn = get_db_connection()
    if not conn:
        return None
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(sql, params or ())
        result = cursor.fetchone() if fetch_one else cursor.fetchall()
        return result
    except Error as e:
        print(f"Query error: {e}")
        return None
    finally:
        cursor.close()
        conn.close()


def execute_db(sql, params=None):
    """
    Execute an INSERT / UPDATE / DELETE statement.
    Returns the lastrowid on success, None on failure.
    """
    conn = get_db_connection()
    if not conn:
        return None
    try:
        cursor = conn.cursor()
        cursor.execute(sql, params or ())
        conn.commit()
        return cursor.lastrowid
    except Error as e:
        print(f"Execute error: {e}")
        conn.rollback()
        return None
    finally:
        cursor.close()
        conn.close()


# ---------------------------------------------------------------------------
# Helper – fetch dropdown option lists used across multiple pages
# ---------------------------------------------------------------------------

def get_majors():
    return query_db("SELECT major_ID, major_name FROM MAJOR ORDER BY major_name") or []


def get_universities():
    return query_db("SELECT university_ID, university_name FROM UNIVERSITY ORDER BY university_name") or []


def get_industries():
    return query_db("SELECT industry_ID, industry_name FROM INDUSTRY ORDER BY industry_name") or []


def get_states():
    return query_db("SELECT state_ID, state_name FROM LOCATION ORDER BY state_name") or []


def get_categories():
    """Fetch distinct major_category values from the allagesRAW table."""
    rows = query_db(
        "SELECT DISTINCT major_category FROM allagesRAW "
        "WHERE major_category IS NOT NULL "
        "  AND major_category != '' "
        "  AND major_category != 'Major_category' "
        "ORDER BY major_category"
    ) or []
    return [r["major_category"] for r in rows]


# ---------------------------------------------------------------------------
# Route: Home / Dashboard
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    """
    Main dashboard.  Loads the active user profile (from session), the top
    job recommendations, and passes lookup lists for search filters.
    """
    user_id = session.get("user_id")
    profile = None
    major_name = None
    university_name = None
    location_name = None

    if user_id:
        # Fetch the profile along with joined display names
        profile = query_db(
            """
            SELECT up.user_profile_ID, up.email, up.grad_year, up.degree_level,
                   m.major_name, m.category,
                   u.university_name,
                   up.major_ID, up.university_ID
            FROM   USER_PROFILE up
            LEFT JOIN MAJOR      m ON up.major_ID      = m.major_ID
            LEFT JOIN UNIVERSITY u ON up.university_ID = u.university_ID
            WHERE  up.user_profile_ID = %s
            """,
            (user_id,),
            fetch_one=True,
        )

        # Fetch preferred location from the most recent preference preset
        if profile:
            pref = query_db(
                """
                SELECT l.state_name
                FROM   PREFERENCE_PRESET pp
                LEFT JOIN LOCATION l ON pp.state_ID = l.state_ID
                WHERE  pp.user_profile_ID = %s
                ORDER  BY pp.preset_ID DESC
                LIMIT  1
                """,
                (user_id,),
                fetch_one=True,
            )
            location_name = pref["state_name"] if pref else "Not set"

    search_query = request.args.get("q", "").strip()
    major_filter = request.args.get("major", "").strip()
    year_filter = request.args.get("year", "").strip()
    category_filter = request.args.get("category", "").strip()
    degree_filter = request.args.get("degree", "").strip()

    # Query allagesRAW for career-outcome data (clean salary & employment stats).
    # The JOB_ROLE table's salary data is largely unusable (99 999 999.99),
    # while allagesRAW has verified median salaries by major ($35K–$125K).
    sql = """
        SELECT a.major,
               a.major_category,
               a.median_salary,
               a.unemployment_rate,
               a.total_students,
               a.employed_count,
               a.unemployed_count
        FROM   allagesRAW a
        WHERE  a.major != 'Major'
          AND  a.median_salary > 0
    """
    params = []

    if search_query:
        sql += " AND (a.major LIKE %s OR a.major_category LIKE %s)"
        like = f"%{search_query}%"
        params.extend([like, like])

    if major_filter:
        sql += " AND a.major LIKE %s"
        params.append(f"%{major_filter}%")

    if category_filter:
        sql += " AND a.major_category = %s"
        params.append(category_filter)

    sql += " ORDER BY a.median_salary DESC"

    jobs = query_db(sql, params) or []

    for idx, job in enumerate(jobs, start=1):
        job["rank"] = idx
        if job.get("unemployment_rate") is not None:
            job["unemployment_rate"] = float(job["unemployment_rate"])

    majors = get_majors()
    industries = get_industries()
    states = get_states()
    categories = get_categories()

    return render_template(
        "index.html",
        profile=profile,
        location_name=location_name,
        jobs=jobs,
        majors=majors,
        industries=industries,
        states=states,
        categories=categories,
        search_query=search_query,
        major_filter=major_filter,
        year_filter=year_filter,
        category_filter=category_filter,
        degree_filter=degree_filter,
    )


# ---------------------------------------------------------------------------
# Route: Profile – Create / Read / Update / Delete
# ---------------------------------------------------------------------------

@app.route("/profile", methods=["GET"])
def profile():
    """Render the profile creation / edit page."""
    user_id = session.get("user_id")
    user_profile = None
    presets = []

    if user_id:
        user_profile = query_db(
            """
            SELECT up.user_profile_ID, up.email, up.grad_year, up.degree_level,
                   up.major_ID, up.university_ID,
                   m.major_name, u.university_name
            FROM   USER_PROFILE up
            LEFT JOIN MAJOR      m ON up.major_ID      = m.major_ID
            LEFT JOIN UNIVERSITY u ON up.university_ID = u.university_ID
            WHERE  up.user_profile_ID = %s
            """,
            (user_id,),
            fetch_one=True,
        )

        presets = query_db(
            """
            SELECT pp.preset_ID, pp.expected_salary, pp.max_unemployment,
                   i.industry_name, l.state_name
            FROM   PREFERENCE_PRESET pp
            LEFT JOIN INDUSTRY  i ON pp.industry_ID = i.industry_ID
            LEFT JOIN LOCATION  l ON pp.state_ID    = l.state_ID
            WHERE  pp.user_profile_ID = %s
            ORDER  BY pp.preset_ID DESC
            """,
            (user_id,),
        ) or []

    majors = get_majors()
    universities = get_universities()
    industries = get_industries()
    states = get_states()

    return render_template(
        "profile.html",
        user_profile=user_profile,
        presets=presets,
        majors=majors,
        universities=universities,
        industries=industries,
        states=states,
    )


@app.route("/profile/create", methods=["POST"])
def create_profile():
    """Insert a new USER_PROFILE record."""
    email = request.form.get("email", "").strip()
    major_id = request.form.get("major_id") or None
    university_id = request.form.get("university_id") or None
    grad_year = request.form.get("grad_year") or None
    degree_level = request.form.get("degree_level", "").strip()

    if not email:
        flash("Email is required.", "error")
        return redirect(url_for("profile"))

    # Check for duplicate email
    existing = query_db(
        "SELECT user_profile_ID FROM USER_PROFILE WHERE email = %s",
        (email,),
        fetch_one=True,
    )
    if existing:
        # Log in as that user instead of creating a duplicate
        session["user_id"] = existing["user_profile_ID"]
        flash("Profile already exists – you are now logged in.", "info")
        return redirect(url_for("index"))

    new_id = execute_db(
        """
        INSERT INTO USER_PROFILE (major_ID, university_ID, grad_year, email, degree_level)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (major_id, university_id, grad_year, email, degree_level),
    )

    if new_id:
        session["user_id"] = new_id
        flash("Profile created successfully!", "success")
    else:
        flash("Failed to create profile. Please try again.", "error")

    return redirect(url_for("index"))


@app.route("/profile/update", methods=["POST"])
def update_profile():
    """Update the currently logged-in user's profile."""
    user_id = session.get("user_id")
    if not user_id:
        flash("Please create a profile first.", "error")
        return redirect(url_for("profile"))

    email = request.form.get("email", "").strip()
    major_id = request.form.get("major_id") or None
    university_id = request.form.get("university_id") or None
    grad_year = request.form.get("grad_year") or None
    degree_level = request.form.get("degree_level", "").strip()

    result = execute_db(
        """
        UPDATE USER_PROFILE
        SET    email = %s, major_ID = %s, university_ID = %s,
               grad_year = %s, degree_level = %s
        WHERE  user_profile_ID = %s
        """,
        (email, major_id, university_id, grad_year, degree_level, user_id),
    )

    if result is not None:
        flash("Profile updated successfully!", "success")
    else:
        flash("Failed to update profile.", "error")

    return redirect(url_for("profile"))


@app.route("/profile/delete", methods=["POST"])
def delete_profile():
    """Delete the current user's profile and all associated presets."""
    user_id = session.get("user_id")
    if not user_id:
        flash("No profile to delete.", "error")
        return redirect(url_for("profile"))

    # Remove presets first (foreign key constraint)
    execute_db(
        "DELETE FROM PREFERENCE_PRESET WHERE user_profile_ID = %s", (user_id,)
    )

    result = execute_db(
        "DELETE FROM USER_PROFILE WHERE user_profile_ID = %s", (user_id,)
    )

    if result is not None:
        session.pop("user_id", None)
        flash("Profile deleted successfully.", "info")
    else:
        flash("Failed to delete profile.", "error")

    return redirect(url_for("profile"))


# ---------------------------------------------------------------------------
# Route: Preference Presets – Create / Delete
# ---------------------------------------------------------------------------

@app.route("/preset/create", methods=["POST"])
def create_preset():
    """Create a new preference preset for the current user."""
    user_id = session.get("user_id")
    if not user_id:
        flash("Please create a profile first.", "error")
        return redirect(url_for("profile"))

    expected_salary = request.form.get("expected_salary") or None
    max_unemployment = request.form.get("max_unemployment") or None
    industry_id = request.form.get("industry_id") or None
    state_id = request.form.get("state_id") or None

    result = execute_db(
        """
        INSERT INTO PREFERENCE_PRESET
               (user_profile_ID, industry_ID, state_ID, expected_salary, max_unemployment)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (user_id, industry_id, state_id, expected_salary, max_unemployment),
    )

    if result:
        flash("Preference preset saved!", "success")
    else:
        flash("Failed to save preset.", "error")

    return redirect(url_for("profile"))


@app.route("/preset/delete/<int:preset_id>", methods=["POST"])
def delete_preset(preset_id):
    """Delete a specific preference preset."""
    user_id = session.get("user_id")
    if not user_id:
        flash("Unauthorized.", "error")
        return redirect(url_for("profile"))

    execute_db(
        "DELETE FROM PREFERENCE_PRESET WHERE preset_ID = %s AND user_profile_ID = %s",
        (preset_id, user_id),
    )
    flash("Preset deleted.", "info")
    return redirect(url_for("profile"))


# ---------------------------------------------------------------------------
# Route: Session helpers – switch / logout
# ---------------------------------------------------------------------------

@app.route("/login", methods=["POST"])
def login():
    """Look up a user by email and store their ID in session."""
    email = request.form.get("email", "").strip()
    user = query_db(
        "SELECT user_profile_ID FROM USER_PROFILE WHERE email = %s",
        (email,),
        fetch_one=True,
    )
    if user:
        session["user_id"] = user["user_profile_ID"]
        flash("Logged in successfully.", "success")
    else:
        flash("No account found with that email.", "error")
    return redirect(url_for("index"))


@app.route("/logout")
def logout():
    """Clear the session."""
    session.pop("user_id", None)
    flash("Logged out.", "info")
    return redirect(url_for("index"))


# ---------------------------------------------------------------------------
# API: JSON endpoint for live keyword search (used by front-end JS)
# ---------------------------------------------------------------------------

@app.route("/api/search")
def api_search():
    """
    Returns JSON list of majors/careers matching a keyword.
    Searches major name and major_category from allagesRAW.
    """
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify([])

    like = f"%{q}%"
    rows = query_db(
        """
        SELECT a.major,
               a.major_category,
               a.median_salary,
               a.unemployment_rate
        FROM   allagesRAW a
        WHERE  a.major != 'Major'
          AND  a.median_salary > 0
          AND  (a.major LIKE %s OR a.major_category LIKE %s)
        ORDER  BY a.median_salary DESC
        LIMIT  15
        """,
        (like, like),
    )
    results = []
    for r in rows or []:
        if r.get("unemployment_rate") is not None:
            r["unemployment_rate"] = float(r["unemployment_rate"])
        results.append(r)
    return jsonify(results)


# ---------------------------------------------------------------------------
# API: Unemployment data for a state
# ---------------------------------------------------------------------------

@app.route("/api/unemployment/<int:state_id>")
def api_unemployment(state_id):
    """Returns the most recent unemployment rate for a given state."""
    row = query_db(
        """
        SELECT u.unemployment_rate, u.year, l.state_name
        FROM   UNEMPLOYMENT u
        JOIN   LOCATION     l ON u.state_ID = l.state_ID
        WHERE  u.state_ID = %s
        ORDER  BY u.year DESC
        LIMIT  1
        """,
        (state_id,),
        fetch_one=True,
    )
    return jsonify(row or {})


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
