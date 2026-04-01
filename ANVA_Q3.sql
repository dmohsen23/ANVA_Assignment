-- ASSIGNMENT ASSUMPTIONS:
-- Some fields exist for some customers but not others.
-- Fields can be created infinitely.
-- Fields apply at different entity levels (PERSON or ADDRESS).
-- Cannot add infinite columns to PERSON or ADDRESS tables.

-- SOLUTION: Entity Attribute Value (EAV) Pattern
-- Store flexible fields as rows in a separate table.
-- Each row represents one field for one entity.
-- A definitions table controls what fields exist per tenant.

-- TRADEOFFS:
-- Pros:
--   Infinite fields without schema changes
--   Different customers use different fields
--   One table handles all flexible data
-- Cons:
--   All values stored as VARCHAR (no type enforcement)
--   Complex queries require CASE WHEN pivoting
--   Sparse data (many NULLs for unused fields)
--   No referential integrity on field_value

-- Despite cons, EAV is my preferred pattern here because
-- the assignment requires infinite flexible fields that
-- differ per customer and cannot be added as fixed columns.
-- ================================================================================================
 
CREATE DATABASE IF NOT EXISTS anva;
USE anva;
 
CREATE SCHEMA IF NOT EXISTS dwh;
 
SET SQL_SAFE_UPDATES = 0;
 
-- Drop existing tables if rerunning
DROP TABLE IF EXISTS dwh.free_fields;
DROP TABLE IF EXISTS dwh.free_field_definitions;
 
SET SQL_SAFE_UPDATES = 1;

-- ---------------------------------------------------------------------------------------
-- TABLE 1: FREE FIELD DEFINITIONS
-- Controls what free fields exist per tenant (customer).
-- Acts as a registry (fields must be defined here)
-- before values can be stored in dwh.free_fields.
-- This enforces governance over free field creation.
-- ---------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.free_field_definitions(
      definition_id   INTEGER AUTO_INCREMENT  PRIMARY KEY					-- Surrogate key: internal stable identity
    , tenant_id       VARCHAR(50)             NOT NULL						-- Each tenant has their own set of free fields
    , field_name      VARCHAR(255)            NOT NULL						-- Name of the flexible field, e.g., car_registration, roof_type, house_surface_area
	, entity_type     VARCHAR(50)             NOT NULL					    -- Which entity this field belongs to either PERSON or ADDRESS
    , data_type       VARCHAR(50)             NOT NULL					    -- Expected data type for validation purposes (VARCHAR, DECIMAL, DATE, BOOLEAN): Stored as metadata since EAV stores all values as text
    , is_required     BOOLEAN                 NOT NULL						-- Whether this field is required for this tenant
                                              DEFAULT FALSE
    -- Audit trail
    , loaded_at       TIMESTAMP               DEFAULT CURRENT_TIMESTAMP
    -- Prevents duplicate field definitions
    , UNIQUE KEY uq_tenant_field_entity
        (tenant_id, field_name, entity_type)
);
 
-- ---------------------------------------------------------------------------------------
-- TABLE 2: FREE FIELDS (EAV TABLE)
-- Stores actual values for each flexible field.
-- One row = one field value for one entity.
-- Linked to definitions table for governance.
-- ---------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwh.free_fields(
      free_field_id   INTEGER AUTO_INCREMENT  PRIMARY KEY					-- Surrogate key: uniquely identifies each field value row
    , tenant_id       VARCHAR(50)             NOT NULL						
    , entity_id       VARCHAR(50)             NOT NULL						
    , entity_type     VARCHAR(50)             NOT NULL
    , field_name      VARCHAR(255)            NOT NULL
    , field_value     VARCHAR(255)											-- The actual value stored as VARCHAR
    -- Audit trail
    , loaded_at       TIMESTAMP               DEFAULT CURRENT_TIMESTAMP
    , updated_at      TIMESTAMP               DEFAULT CURRENT_TIMESTAMP
    -- Prevents duplicate entries for same field
    , UNIQUE KEY uq_entity_field
        (tenant_id, entity_id, entity_type, field_name)
    -- Cannot store a value for an undefined field
    , CONSTRAINT fk_free_fields_definition
        FOREIGN KEY (tenant_id, field_name, entity_type)
        REFERENCES dwh.free_field_definitions
            (tenant_id, field_name, entity_type)
);
 
-- ---------------------------------------------------------------------------------------
-- EXAMPLE DATA
-- Demonstrates how different tenants use different fields
-- ---------------------------------------------------------------------------------------
-- Define free fields per tenant
INSERT INTO dwh.free_field_definitions
    (tenant_id, field_name, entity_type, data_type, is_required)
VALUES
    -- Customer A: insurance company needing car and roof data
      ('CUSTOMER_A', 'car_registration',   'PERSON',  'VARCHAR', FALSE)
    , ('CUSTOMER_A', 'roof_type',          'ADDRESS', 'VARCHAR', FALSE)
    , ('CUSTOMER_A', 'house_surface_area', 'ADDRESS', 'DECIMAL', FALSE)
    -- Customer B: insurance company needing lifestyle data
    , ('CUSTOMER_B', 'clothing_store',     'PERSON',  'VARCHAR', FALSE)
    , ('CUSTOMER_B', 'house_surface_area', 'ADDRESS', 'DECIMAL', FALSE);
-- Store actual free field values
INSERT INTO dwh.free_fields
    (tenant_id, entity_id, entity_type, field_name, field_value)
VALUES
    -- Customer A: person 123 has a car
      ('CUSTOMER_A', '123', 'PERSON',  'car_registration',   'AB-123-CD')
    -- Customer A: address 456 has roof and surface data
    , ('CUSTOMER_A', '456', 'ADDRESS', 'roof_type',          'flat')
    , ('CUSTOMER_A', '456', 'ADDRESS', 'house_surface_area', '120')
    -- Customer B: person 123 has clothing store preference
    , ('CUSTOMER_B', '123', 'PERSON',  'clothing_store',     'Zara')
    -- Customer B: address 456 has surface area
    , ('CUSTOMER_B', '456', 'ADDRESS', 'house_surface_area', '95');
 
-- ---------------------------------------------------------------------------------------
-- EXAMPLE QUERIES
-- Shows how to retrieve free fields joined to main entities
-- ---------------------------------------------------------------------------------------
-- Query 1: Get all free fields for a specific person
-- Shows how EAV joins back to main entity tables
SELECT
      p.person_id
    , p.name
    , f.field_name
    , f.field_value
    , f.tenant_id
    
FROM dwh.person p
LEFT JOIN dwh.free_fields f
    ON  f.entity_id   = p.person_id
    AND f.entity_type = 'PERSON'
WHERE p.person_id = '123';
 
-- Query 2: Pivot free fields into columns for reporting
-- CASE WHEN pattern required because EAV stores rows not columns
SELECT
      p.person_id
    , p.name
    , MAX(CASE WHEN f.field_name = 'car_registration'
        THEN f.field_value END) AS car_registration
    , MAX(CASE WHEN f.field_name = 'clothing_store'
        THEN f.field_value END) AS clothing_store
        
FROM dwh.person p
LEFT JOIN dwh.free_fields f
    ON  f.entity_id   = p.person_id
    AND f.entity_type = 'PERSON'
GROUP BY p.person_id, p.name;
 
-- Query 3: Get all free fields for an address
SELECT
      a.address_id
    , a.street
    , a.city
    , f.field_name
    , f.field_value
    , f.tenant_id
    
FROM dwh.address a
LEFT JOIN dwh.free_fields f
    ON  f.entity_id   = a.address_id
    AND f.entity_type = 'ADDRESS'
WHERE a.address_id = '456';
 
-- Query 4: See all field definitions per tenant
-- Shows which fields each customer has configured
SELECT
      tenant_id
    , field_name
    , entity_type
    , data_type
    , is_required
    
FROM dwh.free_field_definitions
ORDER BY tenant_id, entity_type, field_name;