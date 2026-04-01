-- ASSIGNMENT ASSUMPTIONS:
-- 1. Address data arrives as JSON file nightly at 12:01 AM
-- 2. JSON format designed for this assignment:
--    {
--        "address_id":   "456",
--        "postcode":     "5616SC",
--        "street":       "Cederlaan",
--        "house_number": "254",
--        "city":         "Eindhoven",
--        "country":      "NL"
--    }
-- 3. Solution written in standard SQL (MySQL 8.0)
-- 4. One person can have many addresses (holiday, work, previous)
-- 5. One address can belong to many persons (family members)
-- 6. Many-to-many relationship handled via person_address link table
-- 7. ETL flows: STAGING -> DWH -> HIST -> DATAMART
-- 8. IDs kept as VARCHAR for pipeline resilience
-- ================================================================================================
 
CREATE DATABASE IF NOT EXISTS anva;
USE anva;

-- Temporarily disables safe update mode to allow
-- DELETE and UPDATE using non-primary key columns
SET SQL_SAFE_UPDATES = 0;

-- Clear all data but keep table structure
DROP TABLE IF EXISTS dwh.person_address;
DROP TABLE IF EXISTS dwh.address;
DROP TABLE IF EXISTS hist.address;
DROP TABLE IF EXISTS staging.address_rejected;
DROP TABLE IF EXISTS staging.address_raw;

-- CREATE SCHEMAS
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dwh;
CREATE SCHEMA IF NOT EXISTS hist;
CREATE SCHEMA IF NOT EXISTS datamart;

SET SQL_SAFE_UPDATES = 1;
 
-- CREATE TABLES
-- ---------------------------------------------------------------------------------------
-- 1. STAGING LAYER
-- Purpose: Raw landing zone. Store JSON as is.
--          No transformation, no validation.
--          Temporary: cleared on each full load.
-- ---------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.address_raw(
      load_id        INTEGER AUTO_INCREMENT  PRIMARY KEY						-- AUTO_INCREMENT: Ensures every load can be uniquely identified and traced.
    , raw_json       MEDIUMTEXT              NOT NULL							-- MEDIUMTEXT: Handles up to 16MB whic is safe for large JSON payloads.
    , loaded_at      TIMESTAMP               DEFAULT CURRENT_TIMESTAMP			-- Exact moment the file landed in staging
    , source_file    VARCHAR(255)												-- Name of the source JSON file this record came from.
);
 
-- Quarantine table: holds rejected records with reason, helping nothing is ever lost, and all failures are auditable.
CREATE TABLE IF NOT EXISTS staging.address_rejected(
      rejected_id        INTEGER AUTO_INCREMENT  PRIMARY KEY
    , raw_json           MEDIUMTEXT
    , rejected_at        TIMESTAMP               DEFAULT CURRENT_TIMESTAMP
    , source_file        VARCHAR(255)
    , rejection_reason   VARCHAR(500)            NOT NULL						-- Enables to debug failed loads and fix source data.
);
 
-- ---------------------------------------------------------------------------------------
-- 2. DWH LAYER
-- Purpose: Trusted, validated, typed data.
--          Single source of truth for current records.
--          Deduplicated using MD5 hash (to take any text input and produces a fixed 32-character output ).
-- ---------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.address(
      dwh_address_id  INTEGER AUTO_INCREMENT   PRIMARY KEY						-- Surrogate key is internal stable identity (two different source systems both send data).
    , address_id      VARCHAR(50)              UNIQUE NOT NULL
    , postcode        VARCHAR(10)              NOT NULL
    , street          VARCHAR(255)             NOT NULL
    , house_number    VARCHAR(20)              NOT NULL
    , city            VARCHAR(255)             NOT NULL
    , country         VARCHAR(10)              NOT NULL
    , record_hash     CHAR(32)                 NOT NULL							-- MD5 fingerprint (To skip duplication and detecting genuine changes).
    -- Audit trail: which file this record came from
    , source_file     VARCHAR(255)
    , loaded_at       TIMESTAMP                DEFAULT CURRENT_TIMESTAMP
    , updated_at      TIMESTAMP                DEFAULT CURRENT_TIMESTAMP
);
 
-- Link table: handles many-to-many between person and address
-- One person can have multiple addresses (primary, work, previous)
-- One address can belong to multiple persons (family members)
CREATE TABLE IF NOT EXISTS dwh.person_address(
      person_id       VARCHAR(50)   NOT NULL									    -- Composite primary key: one person can have same address only once per address type						
    , address_id      VARCHAR(50)   NOT NULL
    , address_type    VARCHAR(50)   NOT NULL										-- Type of address: primary, work, previous
    , is_primary      BOOLEAN       NOT NULL									    -- Identifies main address for correspondence
    , valid_from      DATE          NOT NULL										-- When this address relationship became active
    , valid_to        DATE          NOT NULL										-- 9999-12-31 means currently active
    , loaded_at       TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
    , PRIMARY KEY (person_id, address_id, address_type)								-- Composite primary key: unique per person + address + type
 
    -- Referential integrity: person must exist in dwh.person
    -- Cannot link to a person that does not exist
    , CONSTRAINT fk_person_address_person
        FOREIGN KEY (person_id)
        REFERENCES dwh.person(person_id)
 
    -- Referential integrity: address must exist in dwh.address
    -- Cannot link to an address that does not exist
    , CONSTRAINT fk_person_address_address
        FOREIGN KEY (address_id)
        REFERENCES dwh.address(address_id)
);
 
-- ---------------------------------------------------------------------------------------
-- 3. HIST LAYER
-- Purpose: Immutable history. Every version preserved forever.
--          Never updated or deleted (insert only).
--          Supports fraud detection, regulatory audit, and point-in-time queries.
-- ---------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hist.address (
      hist_id         INTEGER AUTO_INCREMENT  PRIMARY KEY							-- Unique identifier for each historical version
    , address_id      VARCHAR(50)             NOT NULL
    , postcode        VARCHAR(10)             NOT NULL
    , street          VARCHAR(255)            NOT NULL
    , house_number    VARCHAR(20)             NOT NULL
    , city            VARCHAR(255)            NOT NULL
    , country         VARCHAR(10)             NOT NULL
    , record_hash     CHAR(32)               NOT NULL
    -- SCD Type 2 columns
    , valid_from      DATE                   NOT NULL
    , valid_to        DATE                   NOT NULL
    , is_current      BOOLEAN                NOT NULL								-- is_current: TRUE only for latest version for fast filtering
    -- Audit columns
    , loaded_by       VARCHAR(100)           NOT NULL
    , loaded_at       TIMESTAMP              DEFAULT CURRENT_TIMESTAMP
    , source_file     VARCHAR(255)
);
 
-- ---------------------------------------------------------------------------------------
-- 4. DATAMART LAYER
-- Purpose: Business-ready reporting layer.
--          Clean, flat, denormalised view for analysts and Power BI. 
--          No history complexity. VIEW not TABLE, and no storage duplication.
-- ---------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW datamart.address_view AS
SELECT
      p.person_id
    , p.name
    , pa.address_type
    , pa.is_primary
    , pa.valid_from
    , pa.valid_to
    , a.address_id
    , a.postcode
    , a.street
    , a.house_number
    , a.city
    , a.country
    , a.loaded_at
    
FROM dwh.person p
LEFT JOIN dwh.person_address pa								-- LEFT JOIN: keep persons even if they have no address yet
    ON pa.person_id = p.person_id
LEFT JOIN dwh.address a										-- LEFT JOIN: keep persons even if address record is missing
    ON a.address_id = pa.address_id;
    
-- ETL logic for ADDRESS follows identical pattern to PERSON
-- Steps 1-6 applied to address_raw, dwh.address, hist.address
-- Validation rules:
--   address_id, postcode, street, house_number, city, country
--   cannot be NULL or empty
-- MD5 hash: MD5(CONCAT(address_id, postcode, street, 
--                      house_number, city, country))