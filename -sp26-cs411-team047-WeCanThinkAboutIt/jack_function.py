"""
jack_function.py
Dashboard-specific database queries for GradPath Analytics.
DB utilities (query_db, etc.) are reused from app.py via lazy import.
"""

import json
from decimal import Decimal


def _to_native(rows):
    """Convert Decimal/non-JSON-serialisable types to plain Python for json.dumps."""
    result = []
    for row in rows:
        result.append({
            k: float(v) if isinstance(v, Decimal) else v
            for k, v in row.items()
        })
    return result


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
    """Chart 2: national average unemployment rate per year, 2000–2025."""
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
            WHEN median_salary < 60000  THEN '$40K – $60K'
            WHEN median_salary < 80000  THEN '$60K – $80K'
            WHEN median_salary < 100000 THEN '$80K – $100K'
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
        "chart_categories":   json.dumps(get_category_salary()),
        "chart_unemp_trend":  json.dumps(get_unemp_trend()),
        "chart_state_unemp":  json.dumps(get_state_unemployment()),
        "chart_salary_dist":  json.dumps(get_salary_distribution()),
    }
