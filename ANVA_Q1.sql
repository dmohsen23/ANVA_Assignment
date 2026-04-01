-- ASSIGNMENT ASSUMPTIONS:
-- 1. Data arrives nightly as a JSON file at 12:01 AM
-- 2. JSON format provided by ANVA:
--    { "person_id":"123", "address_id":"456",
--      "name":"John Doe", "date_of_birth":"1990-01-01",
--      "hobbies":["soccer","playing guitar","travel"] }
-- 3. Solution written in standard SQL (MySQL 8.0)
-- raw_json is stored as NVARCHAR(MAX) text (due to having international characters)
-- 4. ETL flows: STAGING -> DWH -> HIST -> DATAMART
-- 5. IDs kept as VARCHAR for pipeline resilience (source system may change ID format in future)
-- ================================================================================================

CREATE DATABASE IF NOT EXISTS anva;
USE anva;

-- Temporarily disables safe update mode to allow
-- DELETE and UPDATE using non-primary key columns
SET SQL_SAFE_UPDATES = 0;

-- Clear all data but keep table structure
DROP TABLE IF EXISTS dwh.person;
DROP TABLE IF EXISTS dwh.person_hobby;
DROP TABLE IF EXISTS hist.person;
DROP TABLE IF EXISTS staging.person_rejected;
DROP TABLE IF EXISTS staging.person_raw;

-- CREATE SCHEMAS
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dwh;
CREATE SCHEMA IF NOT EXISTS hist;
CREATE SCHEMA IF NOT EXISTS datamart;

-- CREATE TABLES
-- ---------------------------------------------------------------------------------------
-- 1. STAGING LAYER
-- Purpose: Raw landing zone. Store JSON as is.
--          No transformation, no validation.
--          Temporary: cleared on each full load.
-- ---------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.person_raw( 
      load_id        INTEGER AUTO_INCREMENT  PRIMARY KEY					-- AUTO_INCREMENT: Ensures every load can be uniquely identified and traced.
    , raw_json       MEDIUMTEXT              NOT NULL						-- MEDIUMTEXT: Handles up to 16MB whic is safe for large JSON payloads.
    , loaded_at      TIMESTAMP               DEFAULT CURRENT_TIMESTAMP		-- Exact moment the file landed in staging
    , source_file    VARCHAR(255)											-- Name of the source JSON file this record came from.
);

-- Quarantine table: holds rejected records with reason, helping nothing is ever lost, and all failures are auditable.
CREATE TABLE IF NOT EXISTS staging.person_rejected(
	  rejected_id        INTEGER AUTO_INCREMENT  PRIMARY KEY
    , raw_json           MEDIUMTEXT
    , rejected_at        TIMESTAMP               DEFAULT CURRENT_TIMESTAMP
    , source_file        VARCHAR(255)
    , rejection_reason   VARCHAR(500)            NOT NULL					-- Enables to debug failed loads and fix source data.
);

-- ---------------------------------------------------------------------------------------
-- 2. DWH LAYER
-- Purpose: Trusted, validated, typed data.
--          Single source of truth for current records.
--          Deduplicated using MD5 hash (to take any text input and produces a fixed 32-character output ).
-- ---------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.person(
      dwh_person_id   INTEGER AUTO_INCREMENT  PRIMARY KEY		-- Surrogate key is internal stable identity (two different source systems both send data).
    , person_id       VARCHAR(50)             UNIQUE NOT NULL
    , address_id      VARCHAR(50)             NOT NULL
    , name            NVARCHAR(255)           NOT NULL
    , date_of_birth   DATE                    NOT NULL
    , record_hash     CHAR(32)                NOT NULL  		-- MD5 fingerprint (To skip duplication and detecting genuine changes).
    -- Audit trail: which file this record came from
    , source_file     VARCHAR(255)
    , loaded_at       TIMESTAMP               DEFAULT CURRENT_TIMESTAMP
    , updated_at      TIMESTAMP               DEFAULT CURRENT_TIMESTAMP
);

-- One row per hobby per person.
-- Normalised to avoid repeating person data for each hobby
CREATE TABLE IF NOT EXISTS dwh.person_hobby(
      hobby_id        INTEGER AUTO_INCREMENT  PRIMARY KEY
    , person_id       VARCHAR(50)             NOT NULL
    , hobby           NVARCHAR(255)           NOT NULL
    , loaded_at       TIMESTAMP               DEFAULT CURRENT_TIMESTAMP
	-- Ensures hobbies cannot be assigned to a non-existing person.
    , CONSTRAINT fk_person_hobby
        FOREIGN KEY (person_id)
        REFERENCES dwh.person(person_id)
);

-- ---------------------------------------------------------------------------------------
-- 3. HIST LAYER
-- Purpose: Immutable history. Every version preserved forever.
--          Never updated or deleted (insert only).
--          Supports fraud detection, regulatory audit, and point-in-time queries.
-- ---------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hist.person(
      hist_id         INTEGER AUTO_INCREMENT  PRIMARY KEY  	-- unique per version.
    -- Business columns (copy from DWH before update)
    , person_id       VARCHAR(50)            NOT NULL
    , address_id      VARCHAR(50)            NOT NULL
    , name            NVARCHAR(255)          NOT NULL
    , date_of_birth   DATE                   NOT NULL
    , record_hash     CHAR(32)               NOT NULL
    , valid_from      DATE                   NOT NULL		-- valid_from: when this version became active.
    , valid_to        DATE                   NOT NULL		-- valid_to: day before next version started (9999-12-31 means currently active).
    , is_current      BOOLEAN                NOT NULL		-- is_current: TRUE only for latest version (fast filtering)
    -- Audit columns
    , loaded_by       VARCHAR(100)           NOT NULL  		-- system/process name
    , loaded_at       TIMESTAMP              DEFAULT CURRENT_TIMESTAMP
    , source_file     VARCHAR(255)
);

-- ---------------------------------------------------------------------------------------
-- 4. DATAMART LAYER
-- Purpose: Business-ready reporting layer.
--          Clean, flat, denormalised view for analysts and Power BI. 
--          No history complexity. VIEW not TABLE, and no storage duplication.
-- ---------------------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS datamart;

CREATE OR REPLACE VIEW datamart.person_view AS
SELECT
      p.person_id
    , p.address_id
    , p.name
    , p.date_of_birth
    , h.hobby           		-- NULL if person has no hobbies
    , p.loaded_at
    , p.updated_at
    
FROM dwh.person p
LEFT JOIN dwh.person_hobby h   -- LEFT JOIN: keep persons with no hobbies
    ON p.person_id = h.person_id;

-- ============================================================
-- ETL LOGIC:

-- STEP 1 → Load STAGING
-- STEP 2 → Validate and reject
-- STEP 3 → Save to HIST before updating
-- STEP 4 → Merge into DWH
-- STEP 5 → Insert new version into HIST
-- STEP 6 → Load hobbies
-- ============================================================

-- ---------------------------------------------------------------------------------------
-- STEP 1: LOAD STAGING
-- Raw JSON lands here untouched.
-- In standard SQL: INSERT raw JSON as NVARCHAR text
-- ---------------------------------------------------------------------------------------
INSERT INTO staging.person_raw(
      raw_json
    , source_file				-- Name of the source JSON file this record came from.
)
VALUES (
    '{"person_id":"123","address_id":"456",
      "name":"John Doe","date_of_birth":"1990-01-01",
      "hobbies":["soccer","playing guitar","travel"]}',
    'person_20251007.json'
);

-- ---------------------------------------------------------------------------------------
-- STEP 2: VALIDATE AND REJECT
-- Parse each field from JSON.
-- Apply three validation rules:
--   Rule 1: Format validation (date must be valid date)
--   Rule 2: NULL validation (key fields cannot be NULL)
--   Rule 3: Empty validation (name/hobbies cannot be empty)
-- Invalid records go to quarantine with rejection reason.
-- ---------------------------------------------------------------------------------------
INSERT INTO staging.person_rejected(
      raw_json
    , source_file					-- Name of the source JSON file this record came from.
    , rejection_reason
)

SELECT
      r.raw_json
    , v.source_file
    , v.validation_status
    
FROM (
    -- Parsed and validated subquery
    SELECT
          p.load_id
        , p.source_file
        , p.person_id
        , p.name
        , p.date_of_birth
        , p.hobbies
        , CASE
            -- Rule 2: NULL checks
            WHEN p.person_id     IS NULL THEN 'person_id is NULL'
            WHEN p.name          IS NULL THEN 'name is NULL'
            WHEN p.date_of_birth IS NULL THEN 'date_of_birth is NULL'
            WHEN p.address_id    IS NULL THEN 'address_id is NULL'
            -- Rule 3: Empty checks
            WHEN TRIM(p.name)    = ''    THEN 'name is empty'
            WHEN p.hobbies       = '[]'  THEN 'hobbies array is empty'
            -- Rule 1: Format validation
            WHEN STR_TO_DATE(p.date_of_birth, '%Y-%m-%d')
                                 IS NULL THEN 'date_of_birth invalid format'
            ELSE 'VALID'
        END AS validation_status
    FROM (
        -- Inner subquery: extract fields from raw JSON
        SELECT
              load_id
            , source_file
            , JSON_VALUE(raw_json, '$.person_id')      AS person_id
            , JSON_VALUE(raw_json, '$.address_id')     AS address_id
            , JSON_VALUE(raw_json, '$.name')           AS name
            , JSON_VALUE(raw_json, '$.date_of_birth')  AS date_of_birth
            , JSON_EXTRACT(raw_json, '$.hobbies')        AS hobbies
        FROM staging.person_raw
    ) AS p
) AS v
JOIN staging.person_raw r ON v.load_id = r.load_id
WHERE v.validation_status != 'VALID';

-- ---------------------------------------------------------------------------------------
-- STEP 3: SAVE CURRENT VERSION TO HIST
-- BEFORE updating DWH save old version first.
-- If system crashes after this step but before DWH update:
-- old version is safe in HIST, DWH retry is safe.
-- Only save records that are about to change (hash differs).
-- ---------------------------------------------------------------------------------------
-- Save current version to HIST before updating DWH
-- Only for records that are about to change (hash differs)
INSERT INTO hist.person(
      person_id
    , address_id
    , name
    , date_of_birth
    , record_hash
    , valid_from
    , valid_to
    , is_current
    , loaded_by
    , source_file
)
SELECT
      p.person_id
    , p.address_id
    , p.name
    , p.date_of_birth
    , p.record_hash
    , DATE(p.loaded_at)                        AS valid_from
    , DATE_ADD(CURDATE(), INTERVAL -1 DAY)     AS valid_to
    , FALSE                                    AS is_current
    , 'ETL_PROCESS'                            AS loaded_by
    , p.source_file

FROM dwh.person p
-- Join staging to find records that genuinely changed
JOIN staging.person_raw s
    ON p.person_id = JSON_VALUE(s.raw_json, '$.person_id')
-- Only save to HIST if hash is different
WHERE p.record_hash != MD5(
    CONCAT(
          COALESCE(JSON_VALUE(s.raw_json, '$.person_id'),  '')
        , COALESCE(JSON_VALUE(s.raw_json, '$.address_id'), '')
        , COALESCE(JSON_VALUE(s.raw_json, '$.name'),       '')
        , COALESCE(JSON_VALUE(s.raw_json, '$.date_of_birth'), '')
    )
);

-- ---------------------------------------------------------------------------------------
-- STEP 4: MERGE INTO DWH
-- Three scenarios handled automatically:
--   Scenario 1: person_id not in DWH       -> INSERT
--   Scenario 2: person_id in DWH, changed  -> UPDATE
--   Scenario 3: person_id in DWH, same     -> skip (do nothing)
-- Only valid records (not in rejected table) are processed.
-- ---------------------------------------------------------------------------------------
-- Scenarios 1 and 2: Insert new or update changed records
-- ON DUPLICATE KEY handles both in one statement
-- Triggers when person_id (UNIQUE key) already exists
INSERT INTO dwh.person(
      person_id
    , address_id
    , name
    , date_of_birth
    , record_hash
    , source_file
    , loaded_at
    , updated_at
)
SELECT
      JSON_VALUE(r.raw_json, '$.person_id')         AS person_id
    , JSON_VALUE(r.raw_json, '$.address_id')        AS address_id
    , JSON_VALUE(r.raw_json, '$.name')              AS name
    , STR_TO_DATE(
        JSON_VALUE(r.raw_json, '$.date_of_birth'),
        '%Y-%m-%d')                               AS date_of_birth
    , MD5(CONCAT(
        COALESCE(JSON_VALUE(r.raw_json, '$.person_id'),      '')
        , COALESCE(JSON_VALUE(r.raw_json, '$.address_id'),     '')
        , COALESCE(JSON_VALUE(r.raw_json, '$.name'),           '')
        , COALESCE(JSON_VALUE(r.raw_json, '$.date_of_birth'),  '')
    ))                                            AS record_hash
    , r.source_file
    , CURRENT_TIMESTAMP
    , CURRENT_TIMESTAMP
FROM staging.person_raw r
WHERE r.load_id NOT IN (
    SELECT load_id FROM staging.person_rejected
)
ON DUPLICATE KEY UPDATE
      address_id    = IF(
        dwh.person.record_hash != VALUES(record_hash),
        VALUES(address_id),    dwh.person.address_id)
    , name          = IF(
        dwh.person.record_hash != VALUES(record_hash),
        VALUES(name),          dwh.person.name)
    , date_of_birth = IF(
        dwh.person.record_hash != VALUES(record_hash),
        VALUES(date_of_birth), dwh.person.date_of_birth)
    , record_hash   = IF(
        dwh.person.record_hash != VALUES(record_hash),
        VALUES(record_hash),   dwh.person.record_hash)
    , source_file   = IF(
        dwh.person.record_hash != VALUES(record_hash),
        VALUES(source_file),   dwh.person.source_file)
    , updated_at    = IF(
        dwh.person.record_hash != VALUES(record_hash),
        CURRENT_TIMESTAMP,     dwh.person.updated_at);

-- Scenario 3: hash same -> ON DUPLICATE KEY does nothing because all IF conditions return existing values

-- ---------------------------------------------------------------------------------------
-- STEP 5: INSERT NEW VERSION INTO HIST
-- After DWH is updated, record the new version in HIST.
-- valid_to = 9999-12-31 means currently active.
-- is_current = TRUE for latest version.
-- ---------------------------------------------------------------------------------------
INSERT INTO hist.person(
      person_id
    , address_id
    , name
    , date_of_birth
    , record_hash
    , valid_from
    , valid_to
    , is_current
    , loaded_by
    , source_file
)
SELECT
      person_id
    , address_id
    , name
    , date_of_birth
    , record_hash
    , CAST(CURRENT_TIMESTAMP AS DATE) AS valid_from
    , CAST('9999-12-31' AS DATE)      AS valid_to
    , TRUE                            AS is_current
    , 'ETL_PROCESS'                   AS loaded_by
    , source_file
FROM dwh.person
WHERE person_id IN (
    SELECT JSON_VALUE(raw_json, '$.person_id')
    FROM staging.person_raw
);

-- Also update previous HIST version is_current to FALSE
UPDATE hist.person 
SET is_current = FALSE 
WHERE is_current = TRUE 
AND valid_to != CAST('9999-12-31' AS DATE)
AND hist_id > 0;  -- hist_id is primary key, satisfies safe mode

-- ---------------------------------------------------------------------------------------
-- STEP 6: LOAD HOBBIES
-- Wrapped in transaction for atomicity.
-- If crash between DELETE and INSERT:
--   ROLLBACK restores original hobbies.
-- Delete and reload is simpler than smart diff
-- (assignment says: keep it as simple as possible).
-- ---------------------------------------------------------------------------------------
START TRANSACTION;
    -- Delete existing hobbies for persons being processed
    DELETE FROM dwh.person_hobby
	WHERE hobby_id > 0
	AND person_id IN (
		SELECT JSON_VALUE(raw_json, '$.person_id')
		FROM staging.person_raw
		WHERE load_id NOT IN (
			SELECT load_id FROM staging.person_rejected
		)
	);

    -- Insert each hobby individually
    -- JSON_EXTRACT extracts element by index from array
    -- hobby[0], hobby[1], hobby[2] etc.
    INSERT INTO dwh.person_hobby (person_id, hobby)
    SELECT
        JSON_VALUE(raw_json, '$.person_id'),
        JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.hobbies[0]'))
    FROM staging.person_raw
    WHERE JSON_EXTRACT(raw_json, '$.hobbies[0]') IS NOT NULL
    AND load_id NOT IN (
        SELECT load_id FROM staging.person_rejected
    );

    INSERT INTO dwh.person_hobby (person_id, hobby)
    SELECT
        JSON_VALUE(raw_json, '$.person_id'),
        JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.hobbies[1]'))
    FROM staging.person_raw
    WHERE JSON_EXTRACT(raw_json, '$.hobbies[1]') IS NOT NULL
    AND load_id NOT IN (
        SELECT load_id FROM staging.person_rejected
    );

    INSERT INTO dwh.person_hobby (person_id, hobby)
    SELECT
        JSON_VALUE(raw_json, '$.person_id'),
        JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.hobbies[2]'))
    FROM staging.person_raw
    WHERE JSON_EXTRACT(raw_json, '$.hobbies[2]') IS NOT NULL
    AND load_id NOT IN (
        SELECT load_id FROM staging.person_rejected
    );

COMMIT;

-- ============================================================
-- SCHEDULING LOGIC
-- ============================================================
-- Full load:        Every Sunday at 12:01 AM
--                   TRUNCATE staging + reload ALL records
-- Incremental load: Every other day at 12:01 AM
--                   Load only new/changed records
-- DWH and HIST use same MERGE logic either way.
-- Difference is only what goes into staging.
-- ============================================================

DELIMITER $$

DROP PROCEDURE IF EXISTS run_etl$$

CREATE PROCEDURE run_etl()
BEGIN
    -- DAYOFWEEK() returns 1 = Sunday in MySQL
    IF DAYOFWEEK(CURDATE()) = 1 THEN

        -- FULL LOAD: every Sunday at 12:01 AM
        -- DELETE clears staging to reload all records
        DELETE FROM staging.person_raw;
    ELSE

        -- INCREMENTAL LOAD: Mon-Sat at 12:01 AM
        -- Staging receives only new/changed records
        -- INSERT ON DUPLICATE KEY handles all scenarios
        SELECT 'incremental load running';

    END IF;

END$$

DELIMITER ;

SET SQL_SAFE_UPDATES = 1;