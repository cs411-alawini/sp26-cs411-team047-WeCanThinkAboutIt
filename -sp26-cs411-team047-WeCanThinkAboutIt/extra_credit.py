"""
jack_function.py
Dashboard-specific database queries for GradPath Analytics.
DB utilities (query_db, get_db_connection, etc.) are reused from app.py via lazy import.
"""

import json
from decimal import Decimal


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_native(rows):
    """Convert Decimal / non-JSON-serialisable types to plain Python."""
    result = []
    for row in rows:
        result.append({
            k: float(v) if isinstance(v, Decimal) else v
            for k, v in row.items()
        })
    return result


# ---------------------------------------------------------------------------
# Dashboard chart queries
# ---------------------------------------------------------------------------

def get_category_salary():
    """Chart 1: average median salary and unemployment rate by major category."""
    from app import query_db
    rows = query_db(
        """
        SELECT major_category,
               ROUND(AVG(median_salary), 0)           AS avg_salary,
               ROUND(AVG(unemployment_rate) * 100, 2) AS avg_unemp
        FROM   allagesRAW
        WHERE  major_category IS NOT NULL
          AND  major_category != ''
          AND  major_category != 'Major_category'
          AND  median_salary > 0
        GROUP  BY major_category
        ORDER  BY avg_salary DESC
        """
    ) or []
    return _to_native(rows)


def get_unemp_trend():
    """Chart 2: national average unemployment rate per year, 2000-2025."""
    from app import query_db
    rows = query_db(
    """
        SELECT year,
               ROUND(AVG(unemployment_rate), 2) AS avg_rate
        FROM   UNEMPLOYMENT
        WHERE  year BETWEEN 2000 AND 2025
        GROUP  BY year
        ORDER  BY year
        """
    ) or []
    return _to_native(rows)


def get_state_unemployment():
    """Chart 3 (map): 2024 average unemployment rate per US state."""
    from app import query_db
    rows = query_db(
        """
        SELECT l.state_name,
               ROUND(AVG(u.unemployment_rate), 2) AS avg_rate
        FROM   LOCATION l
        JOIN   UNEMPLOYMENT u ON l.state_ID = u.state_ID
        WHERE  u.year = 2024
          AND  u.unemployment_rate > 0
          AND  l.state_ID BETWEEN 1 AND 56
          AND  l.state_name NOT IN ('State/Area', 'Los Angeles County')
        GROUP  BY l.state_ID, l.state_name
        ORDER  BY l.state_name
        """
    ) or []
    return _to_native(rows)


def get_salary_distribution():
    """Chart 4: count of majors in each salary bracket."""
    from app import query_db
    rows = query_db(
        """
        SELECT
          CASE
            WHEN median_salary < 40000  THEN 'Under $40K'
            WHEN median_salary < 60000  THEN '$40K - $60K'
            WHEN median_salary < 80000  THEN '$60K - $80K'
            WHEN median_salary < 100000 THEN '$80K - $100K'
            ELSE 'Over $100K'
          END AS bucket,
          COUNT(*) AS cnt
        FROM   allagesRAW
        WHERE  median_salary > 0
          AND  major != 'Major'
        GROUP  BY bucket
        ORDER  BY MIN(median_salary)
        """
    ) or []
    return _to_native(rows)


def get_all_dashboard_data():
    """Return all four chart datasets as JSON strings ready for the template."""
    return {
        "chart_categories":  json.dumps(get_category_salary()),
        "chart_unemp_trend": json.dumps(get_unemp_trend()),
        "chart_state_unemp": json.dumps(get_state_unemployment()),
        "chart_salary_dist": json.dumps(get_salary_distribution()),
    }


# ---------------------------------------------------------------------------
# Transaction: save target state + career recommendations
# ---------------------------------------------------------------------------

def save_state_preference(user_id, state_name, year_adjustment):
    """
    REPEATABLE READ transaction - atomically:
      1. UPDATE USER_PROFILE.grad_year by year_adjustment (-1 / 0 / +1)
      2. DELETE existing preset for the same state  [subquery: state_name -> state_ID]
      3. INSERT new preset carrying over previous salary/unemployment preferences
      4. SELECT matching career recommendations
         [JOIN + GROUP BY aggregation + nested subquery for target unemployment year]

    Returns dict: { success, state, grad_year, state_unemp, matches[] }
    """
    from app import get_db_connection

    conn = get_db_connection()
    if not conn:
        return {"error": "DB connection failed"}

    try:
        conn.start_transaction(isolation_level="REPEATABLE READ")
        cursor = conn.cursor(dictionary=True)

        # ── WRITE 1: adjust graduation year ─────────────────────────────
        if year_adjustment != 0:
            cursor.execute(
                """
                UPDATE USER_PROFILE
                SET    grad_year = grad_year + %s
                WHERE  user_profile_ID = %s
                """,
                (year_adjustment, user_id),
            )

        # ── Snapshot previous preset so we can carry values forward ─────
        cursor.execute(
            """
            SELECT expected_salary, max_unemployment, industry_ID
            FROM   PREFERENCE_PRESET
            WHERE  user_profile_ID = %s
            ORDER  BY last_updated DESC
            LIMIT  1
            """,
            (user_id,),
        )
        prev        = cursor.fetchone()
        exp_salary  = prev["expected_salary"]  if prev else None
        max_unemp   = prev["max_unemployment"] if prev else None
        industry_id = prev["industry_ID"]      if prev else None

        # ── WRITE 2: delete old preset for this state (subquery) ─────────
        # Subquery translates state_name -> state_ID from LOCATION
        cursor.execute(
            """
            DELETE FROM PREFERENCE_PRESET
            WHERE  user_profile_ID = %s
              AND  state_ID = (
                       SELECT state_ID
                       FROM   LOCATION
                       WHERE  state_name = %s
                   )
            """,
            (user_id, state_name),
        )

        # ── WRITE 3: insert new preset for the selected state ────────────
        cursor.execute(
            """
            INSERT INTO PREFERENCE_PRESET
                   (user_profile_ID, state_ID, industry_ID,
                    expected_salary, max_unemployment)
            VALUES (%s,
                    (SELECT state_ID FROM LOCATION WHERE state_name = %s),
                    %s, %s, %s)
            """,
            (user_id, state_name, industry_id, exp_salary, max_unemp),
        )

        # ── READ 4: career recommendations ───────────────────────────────
        # Advanced concepts used:
        #   • JOIN multiple relations (allagesRAW, MAJOR, LOCATION, UNEMPLOYMENT, USER_PROFILE)
        #   • GROUP BY aggregation (AVG unemployment per state)
        #   • Nested subquery that cannot be replaced by a JOIN:
        #       find the latest unemployment year <= user's grad_year
        cursor.execute(
            """
            SELECT a.major,
                   a.major_category,
                   a.median_salary,
                   ROUND(a.unemployment_rate * 100, 2)              AS field_unemp_pct,
                   su.avg_state_unemp,
                   a.median_salary - COALESCE(pp.expected_salary, 0) AS salary_gap
            FROM   allagesRAW a
            JOIN   MAJOR m ON a.major = m.Major_name
            LEFT JOIN (
                -- user's major category (NULL if no major set -> return all)
                SELECT um.Category AS user_category
                FROM   USER_PROFILE up
                JOIN   MAJOR um ON up.major_ID = um.major_ID
                WHERE  up.user_profile_ID = %s
            ) uc ON 1 = 1
            JOIN   LOCATION l ON l.state_name = %s
            JOIN   (
                -- aggregate monthly unemployment -> single avg per state
                -- for the most recent year that does not exceed grad_year
                SELECT state_ID,
                       ROUND(AVG(unemployment_rate), 2) AS avg_state_unemp
                FROM   UNEMPLOYMENT
                WHERE  year = (
                           SELECT MAX(yr.year)
                           FROM   UNEMPLOYMENT yr
                           WHERE  yr.year <= (
                                      SELECT grad_year
                                      FROM   USER_PROFILE
                                      WHERE  user_profile_ID = %s
                                  )
                             AND  yr.year > 0
                       )
                  AND  unemployment_rate > 0
                GROUP  BY state_ID
            ) su ON su.state_ID = l.state_ID
            LEFT JOIN (
                SELECT expected_salary
                FROM   PREFERENCE_PRESET
                WHERE  user_profile_ID = %s
                ORDER  BY last_updated DESC
                LIMIT  1
            ) pp ON 1 = 1
            WHERE  (uc.user_category IS NULL OR m.Category = uc.user_category)
              AND  a.median_salary > 0
              AND  a.major != 'Major'
              AND  (pp.expected_salary IS NULL OR a.median_salary >= pp.expected_salary)
            ORDER  BY a.median_salary DESC
            LIMIT  10
            """,
            (user_id, state_name, user_id, user_id),
        )
        matches = cursor.fetchall()

        # get updated grad_year to send back
        cursor.execute(
            "SELECT grad_year FROM USER_PROFILE WHERE user_profile_ID = %s",
            (user_id,),
        )
        up = cursor.fetchone()

        conn.commit()

        return {
            "success":     True,
            "state":       state_name,
            "grad_year":   up["grad_year"] if up else None,
            "state_unemp": float(matches[0]["avg_state_unemp"])
                           if matches and matches[0]["avg_state_unemp"] is not None
                           else None,
            "matches":     _to_native(matches),
        }

    except Exception as e:
        conn.rollback()
        print(f"Transaction error: {e}")
        return {"error": str(e)}
    finally:
        cursor.close()
        conn.close()


# ---------------------------------------------------------------------------
# Stored Procedure: GetCareerRecommendations
# ---------------------------------------------------------------------------

def call_career_recommendations(user_id, state_name):
    """
    Call the stored procedure GetCareerRecommendations(user_id, state_name).
    Returns { matches: [...], category_stats: [...] }
    """
    from app import get_db_connection

    conn = get_db_connection()
    if not conn:
        return {"error": "DB connection failed"}

    cursor = conn.cursor(dictionary=True)
    try:
        cursor.callproc("GetCareerRecommendations", (user_id, state_name))

        result_sets = [rs.fetchall() for rs in cursor.stored_results()]
        matches        = result_sets[0] if len(result_sets) > 0 else []
        category_stats = result_sets[1] if len(result_sets) > 1 else []

        return {
            "matches":        _to_native(matches),
            "category_stats": _to_native(category_stats),
        }
    except Exception as e:
        print(f"SP call error: {e}")
        return {"error": str(e)}
    finally:
        cursor.close()
        conn.close()


# ---------------------------------------------------------------------------
# One-time DB setup: constraints, trigger, stored procedure
# ---------------------------------------------------------------------------

def setup_db_objects():
    """Create constraints, trigger, and stored procedure if not present."""
    _setup_constraints()
    _setup_trigger()
    _setup_stored_procedure()


def _setup_constraints():
    from app import get_db_connection
    conn = get_db_connection()
    if not conn:
        return
    cursor = conn.cursor()
    ddl_list = [
        # UNIQUE email prevents duplicate accounts at the DB level
        "ALTER TABLE USER_PROFILE ADD CONSTRAINT uq_email UNIQUE (email)",
        # CHECK: salary must be non-negative
        ("ALTER TABLE PREFERENCE_PRESET ADD CONSTRAINT chk_salary "
         "CHECK (expected_salary IS NULL OR expected_salary >= 0)"),
        # CHECK: unemployment rate percentage must be in valid range
        ("ALTER TABLE PREFERENCE_PRESET ADD CONSTRAINT chk_unemployment "
         "CHECK (max_unemployment IS NULL OR max_unemployment BETWEEN 0 AND 100)"),
    ]
    for ddl in ddl_list:
        try:
            cursor.execute(ddl)
            conn.commit()
        except Exception:
            conn.rollback()  # already exists – ignore
    cursor.close()
    conn.close()


def _setup_trigger():
    """
    Trigger: after_major_change
    Event    : AFTER UPDATE ON USER_PROFILE
    Condition: IF major_ID actually changed
    Action   : DELETE all PREFERENCE_PRESET rows for that user
               (old presets based on the old major are no longer relevant)
    """
    from app import get_db_connection
    conn = get_db_connection()
    if not conn:
        return
    cursor = conn.cursor()
    try:
        cursor.execute("DROP TRIGGER IF EXISTS after_major_change")
        cursor.execute(
            """
            CREATE TRIGGER after_major_change
            AFTER UPDATE ON USER_PROFILE
            FOR EACH ROW
            BEGIN
                IF NEW.major_ID IS NOT NULL
                   AND OLD.major_ID IS NOT NULL
                   AND NEW.major_ID != OLD.major_ID
                THEN
                    DELETE FROM PREFERENCE_PRESET
                    WHERE  user_profile_ID = NEW.user_profile_ID;
                END IF;
            END
            """
        )
        conn.commit()
        print("Trigger after_major_change created.")
    except Exception as e:
        print(f"Trigger setup error: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def _setup_stored_procedure():

    from app import get_db_connection
    conn = get_db_connection()
    if not conn:
        return
    cursor = conn.cursor()
    try:
        cursor.execute("DROP PROCEDURE IF EXISTS GetCareerRecommendations")
        cursor.execute(
            """
            CREATE PROCEDURE GetCareerRecommendations(
                IN  p_user_id    INT,
                IN  p_state_name VARCHAR(30)
            )
            BEGIN
                DECLARE v_major_id    INT          DEFAULT NULL;
                DECLARE v_grad_year   INT          DEFAULT 2024;
                DECLARE v_max_year    INT          DEFAULT 2024;
                DECLARE v_target_year INT          DEFAULT 2024;
                DECLARE v_exp_salary  DOUBLE       DEFAULT NULL;
                DECLARE v_category    VARCHAR(100) DEFAULT NULL;

                SELECT major_ID, COALESCE(grad_year, 2024)
                INTO   v_major_id, v_grad_year
                FROM   USER_PROFILE
                WHERE  user_profile_ID = p_user_id;

                IF v_major_id IS NOT NULL THEN
                    SELECT Category INTO v_category
                    FROM   MAJOR
                    WHERE  major_ID = v_major_id;
                END IF;

                SELECT expected_salary INTO v_exp_salary
                FROM   PREFERENCE_PRESET
                WHERE  user_profile_ID = p_user_id
                ORDER  BY last_updated DESC
                LIMIT  1;

                SELECT MAX(year) INTO v_max_year
                FROM   UNEMPLOYMENT
                WHERE  year > 0;

                IF v_grad_year > v_max_year THEN
                    SET v_target_year = v_max_year;
                ELSE
                    SET v_target_year = v_grad_year;
                END IF;

                IF v_category IS NULL THEN
                   SELECT a.major,
                           a.major_category,
                           a.median_salary,
                           ROUND(a.unemployment_rate * 100, 2) AS field_unemp_pct,
                           su.avg_state_unemp,
                           a.median_salary - COALESCE(v_exp_salary, 0) AS salary_gap
                    FROM   allagesRAW a
                    JOIN   LOCATION l ON l.state_name = p_state_name
                    JOIN   (
                               SELECT state_ID,
                                      ROUND(AVG(unemployment_rate), 2) AS avg_state_unemp
                               FROM   UNEMPLOYMENT
                               WHERE  year = v_target_year
                                 AND  unemployment_rate > 0
                               GROUP  BY state_ID
                           ) su ON su.state_ID = l.state_ID
                    WHERE  a.median_salary > 0
                      AND  a.major != 'Major'
                      AND  (v_exp_salary IS NULL OR a.median_salary >= v_exp_salary)
                    ORDER  BY a.median_salary DESC
                    LIMIT  10;
                ELSE
                    
                    SELECT a.major,
                           a.major_category,
                           a.median_salary,
                           ROUND(a.unemployment_rate * 100, 2) AS field_unemp_pct,
                           su.avg_state_unemp,
                           a.median_salary - COALESCE(v_exp_salary, 0) AS salary_gap
                    FROM   allagesRAW a
                    JOIN   MAJOR m ON a.major = m.Major_name
                    JOIN   LOCATION l ON l.state_name = p_state_name
                    JOIN   (
                               SELECT state_ID,
                                      ROUND(AVG(unemployment_rate), 2) AS avg_state_unemp
                               FROM   UNEMPLOYMENT
                               WHERE  year = v_target_year
                                 AND  unemployment_rate > 0
                               GROUP  BY state_ID
                           ) su ON su.state_ID = l.state_ID
                    WHERE  m.Category = v_category
                      AND  a.median_salary > 0
                      AND  a.major != 'Major'
                      AND  (v_exp_salary IS NULL OR a.median_salary >= v_exp_salary)
                    ORDER  BY a.median_salary DESC
                    LIMIT  10;
                END IF;

                SELECT major_category,
                       ROUND(AVG(median_salary), 0) AS avg_salary,
                       COUNT(*)                     AS major_count
                FROM   allagesRAW
                WHERE  median_salary > 0
                  AND  major != 'Major'
                GROUP  BY major_category
                ORDER  BY avg_salary DESC;
            END
            """
        )
        conn.commit()
        print("Stored procedure GetCareerRecommendations created.")
    except Exception as e:
        print(f"SP setup error: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
