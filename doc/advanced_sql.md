# GradPath – Advanced SQL Reference

## 1. Transaction

**Isolation Level:** `REPEATABLE READ`

**Application utility:** When a logged-in user selects a US state on the Analytics
Dashboard and chooses a graduation timeline adjustment (-1 / 0 / +1 year), this
transaction atomically updates the user's graduation year, removes any stale preset
for that state, saves a new preference preset, and returns a personalized list of
matching careers – all in a single consistent unit of work.

**Advanced SQL concepts used:**
- Join multiple relations (allagesRAW ⋈ MAJOR ⋈ LOCATION ⋈ UNEMPLOYMENT ⋈ USER_PROFILE)
- Aggregation via GROUP BY (AVG unemployment grouped by state_ID)
- Subquery that cannot be replaced by a JOIN
  (MAX year ≤ grad_year inside the UNEMPLOYMENT subquery)

```sql
-- Step 1: adjust graduation year
UPDATE USER_PROFILE
SET    grad_year = grad_year + :year_adjustment   -- -1 / 0 / +1
WHERE  user_profile_ID = :user_id;

-- Step 2: delete existing preset for the selected state
--         uses a subquery to translate state_name -> state_ID
DELETE FROM PREFERENCE_PRESET
WHERE  user_profile_ID = :user_id
  AND  state_ID = (
           SELECT state_ID
           FROM   LOCATION
           WHERE  state_name = :state_name
       );

-- Step 3: insert new preset (carries over previous salary / unemployment preferences)
INSERT INTO PREFERENCE_PRESET
       (user_profile_ID, state_ID, industry_ID, expected_salary, max_unemployment)
VALUES (:user_id,
        (SELECT state_ID FROM LOCATION WHERE state_name = :state_name),
        :industry_id, :expected_salary, :max_unemployment);

-- Step 4: retrieve personalized career recommendations
--   • JOIN: allagesRAW ⋈ MAJOR ⋈ LOCATION ⋈ UNEMPLOYMENT (subquery) ⋈ USER_PROFILE (subquery)
--   • GROUP BY aggregation: AVG(unemployment_rate) per state
--   • Non-replaceable subquery: find latest unemployment year <= user's grad_year
SELECT a.major,
       a.major_category,
       a.median_salary,
       ROUND(a.unemployment_rate * 100, 2)               AS field_unemp_pct,
       su.avg_state_unemp,
       a.median_salary - COALESCE(pp.expected_salary, 0) AS salary_gap
FROM   allagesRAW a
JOIN   MAJOR m  ON a.major = m.Major_name
LEFT JOIN (
    SELECT um.Category AS user_category
    FROM   USER_PROFILE up
    JOIN   MAJOR um ON up.major_ID = um.major_ID
    WHERE  up.user_profile_ID = :user_id
) uc ON 1 = 1
JOIN   LOCATION l ON l.state_name = :state_name
JOIN   (
    SELECT state_ID,
           ROUND(AVG(unemployment_rate), 2) AS avg_state_unemp
    FROM   UNEMPLOYMENT
    WHERE  year = (
               SELECT MAX(yr.year)
               FROM   UNEMPLOYMENT yr
               WHERE  yr.year <= (
                          SELECT grad_year
                          FROM   USER_PROFILE
                          WHERE  user_profile_ID = :user_id
                      )
                 AND  yr.year > 0
           )
      AND  unemployment_rate > 0
    GROUP  BY state_ID
) su ON su.state_ID = l.state_ID
LEFT JOIN (
    SELECT expected_salary
    FROM   PREFERENCE_PRESET
    WHERE  user_profile_ID = :user_id
    ORDER  BY last_updated DESC
    LIMIT  1
) pp ON 1 = 1
WHERE  (uc.user_category IS NULL OR m.Category = uc.user_category)
  AND  a.median_salary > 0
  AND  a.major != 'Major'
  AND  (pp.expected_salary IS NULL OR a.median_salary >= pp.expected_salary)
ORDER  BY a.median_salary DESC
LIMIT  10;
```

---

## 2. Stored Procedure

**Name:** `GetCareerRecommendations(p_user_id INT, p_state_name VARCHAR(30))`

**Application utility:** Reusable procedure that returns personalized career
recommendations for any user/state combination. Called independently of the
transaction when only a read is needed (no preference save).

**Control structures:**
- `IF v_major_id IS NULL … ELSE` — skips category filter when user has no major
- `IF v_grad_year > v_max_year … ELSE` — caps target year to latest available data

**Advanced Query 1:** Career recommendations with JOIN + GROUP BY + subquery
**Advanced Query 2:** Per-category salary statistics (GROUP BY aggregation)

```sql
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

    -- Load user profile fields
    SELECT major_ID, COALESCE(grad_year, 2024)
    INTO   v_major_id, v_grad_year
    FROM   USER_PROFILE
    WHERE  user_profile_ID = p_user_id;

    -- Resolve major category from major_ID
    IF v_major_id IS NOT NULL THEN
        SELECT Category INTO v_category
        FROM   MAJOR
        WHERE  major_ID = v_major_id;
    END IF;

    -- Latest expected salary from any saved preset
    SELECT expected_salary INTO v_exp_salary
    FROM   PREFERENCE_PRESET
    WHERE  user_profile_ID = p_user_id
    ORDER  BY last_updated DESC
    LIMIT  1;

    -- Determine target unemployment year (cap to latest available)
    SELECT MAX(year) INTO v_max_year
    FROM   UNEMPLOYMENT
    WHERE  year > 0;

    IF v_grad_year > v_max_year THEN
        SET v_target_year = v_max_year;
    ELSE
        SET v_target_year = v_grad_year;
    END IF;

    -- Advanced Query 1: career recommendations
    IF v_category IS NULL THEN
        SELECT a.major, a.major_category, a.median_salary,
               ROUND(a.unemployment_rate * 100, 2) AS field_unemp_pct,
               su.avg_state_unemp,
               a.median_salary - COALESCE(v_exp_salary, 0) AS salary_gap
        FROM   allagesRAW a
        JOIN   LOCATION l ON l.state_name = p_state_name
        JOIN   (
                   SELECT state_ID,
                          ROUND(AVG(unemployment_rate), 2) AS avg_state_unemp
                   FROM   UNEMPLOYMENT
                   WHERE  year = v_target_year AND unemployment_rate > 0
                   GROUP  BY state_ID
               ) su ON su.state_ID = l.state_ID
        WHERE  a.median_salary > 0 AND a.major != 'Major'
          AND  (v_exp_salary IS NULL OR a.median_salary >= v_exp_salary)
        ORDER  BY a.median_salary DESC LIMIT 10;
    ELSE
        SELECT a.major, a.major_category, a.median_salary,
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
                   WHERE  year = v_target_year AND unemployment_rate > 0
                   GROUP  BY state_ID
               ) su ON su.state_ID = l.state_ID
        WHERE  m.Category = v_category
          AND  a.median_salary > 0 AND a.major != 'Major'
          AND  (v_exp_salary IS NULL OR a.median_salary >= v_exp_salary)
        ORDER  BY a.median_salary DESC LIMIT 10;
    END IF;

    -- Advanced Query 2: category salary comparison
    SELECT major_category,
           ROUND(AVG(median_salary), 0) AS avg_salary,
           COUNT(*)                     AS major_count
    FROM   allagesRAW
    WHERE  median_salary > 0 AND major != 'Major'
    GROUP  BY major_category
    ORDER  BY avg_salary DESC;
END
```

---

## 3. Trigger

**Name:** `after_major_change`

**Application utility:** When a user updates their major on the Profile page, all
existing preference presets become irrelevant (they were based on the old major's
career context). This trigger automatically clears them, preventing stale
recommendations.

- **Event:** `AFTER UPDATE ON USER_PROFILE`
- **Condition:** `IF NEW.major_ID != OLD.major_ID`
- **Action:** `DELETE FROM PREFERENCE_PRESET`

```sql
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
```

---

## 4. Constraints

```sql
-- Prevent duplicate accounts at the database level
ALTER TABLE USER_PROFILE
ADD CONSTRAINT uq_email UNIQUE (email);

-- Expected salary must be non-negative
ALTER TABLE PREFERENCE_PRESET
ADD CONSTRAINT chk_salary
CHECK (expected_salary IS NULL OR expected_salary >= 0);

-- Unemployment rate percentage must be within a valid range
ALTER TABLE PREFERENCE_PRESET
ADD CONSTRAINT chk_unemployment
CHECK (max_unemployment IS NULL OR max_unemployment BETWEEN 0 AND 100);
```
