-- Business Rules Schema Definitions
-- Azure Databricks SQL for Silver Pipeline Business Rules

-- Create business_rules schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS business_rules
COMMENT 'Schema for Silver pipeline business rules and configuration tables';

-- Schema Rules Table
-- Contains business rules for field selection in Silver_A stage
CREATE TABLE IF NOT EXISTS business_rules.schema_definitions (
  source STRING COMMENT 'Source database identifier',
  lob STRING COMMENT 'Line of business identifier', 
  domain STRING COMMENT 'Domain: pharmacy, medical, or member',
  field_name STRING COMMENT 'Name of the field to include in Silver_A',
  data_type STRING COMMENT 'Expected data type of the field',
  is_required BOOLEAN COMMENT 'Whether the field is required (not null)',
  created_date TIMESTAMP COMMENT 'When this rule was created',
  updated_date TIMESTAMP COMMENT 'When this rule was last updated'
)
USING DELTA
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
COMMENT 'Schema definitions for Silver_A stage field selection';

-- Transform Rules Table  
-- Contains transformation logic definitions for Silver_B stage
CREATE TABLE IF NOT EXISTS business_rules.transform_definitions (
  source STRING COMMENT 'Source database identifier',
  lob STRING COMMENT 'Line of business identifier',
  domain STRING COMMENT 'Domain: pharmacy, medical, or member', 
  field_name STRING COMMENT 'Name of the field to transform',
  transform STRING COMMENT 'SQL transformation expression to apply',
  transform_type STRING COMMENT 'Type of transformation: CAST, CASE, FUNCTION, etc.',
  priority INTEGER COMMENT 'Priority order for applying transformations',
  created_date TIMESTAMP COMMENT 'When this rule was created',
  updated_date TIMESTAMP COMMENT 'When this rule was last updated'
)
USING DELTA
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true', 
  'delta.autoOptimize.autoCompact' = 'true'
)
COMMENT 'Transform definitions for Silver_B stage data transformations';

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_schema_source_lob_domain 
ON business_rules.schema_definitions (source, lob, domain);

CREATE INDEX IF NOT EXISTS idx_transform_source_lob_domain
ON business_rules.transform_definitions (source, lob, domain);

-- Sample data for schema_definitions
INSERT INTO business_rules.schema_definitions VALUES
('source1', 'lob1', 'pharmacy', 'patient_id', 'STRING', true, current_timestamp(), current_timestamp()),
('source1', 'lob1', 'pharmacy', 'prescription_date', 'DATE', true, current_timestamp(), current_timestamp()),
('source1', 'lob1', 'pharmacy', 'drug_name', 'STRING', true, current_timestamp(), current_timestamp()),
('source1', 'lob1', 'pharmacy', 'quantity', 'INTEGER', false, current_timestamp(), current_timestamp()),
('source1', 'lob1', 'medical', 'patient_id', 'STRING', true, current_timestamp(), current_timestamp()),
('source1', 'lob1', 'medical', 'service_date', 'DATE', true, current_timestamp(), current_timestamp()),
('source1', 'lob1', 'medical', 'procedure_code', 'STRING', true, current_timestamp(), current_timestamp()),
('source1', 'lob1', 'member', 'member_id', 'STRING', true, current_timestamp(), current_timestamp()),
('source1', 'lob1', 'member', 'enrollment_date', 'DATE', true, current_timestamp(), current_timestamp());

-- Sample data for transform_definitions  
INSERT INTO business_rules.transform_definitions VALUES
('source1', 'lob1', 'pharmacy', 'patient_id', 'UPPER(patient_id)', 'FUNCTION', 1, current_timestamp(), current_timestamp()),
('source1', 'lob1', 'pharmacy', 'drug_name', 'TRIM(drug_name)', 'FUNCTION', 1, current_timestamp(), current_timestamp()),
('source1', 'lob1', 'pharmacy', 'quantity', 'CAST(quantity AS INTEGER)', 'CAST', 1, current_timestamp(), current_timestamp()),
('source1', 'lob1', 'medical', 'patient_id', 'UPPER(patient_id)', 'FUNCTION', 1, current_timestamp(), current_timestamp()),
('source1', 'lob1', 'medical', 'procedure_code', 'UPPER(TRIM(procedure_code))', 'FUNCTION', 1, current_timestamp(), current_timestamp()),
('source1', 'lob1', 'member', 'member_id', 'UPPER(member_id)', 'FUNCTION', 1, current_timestamp(), current_timestamp());