-- ==========================================
-- Snowflake Setup Script: Cricket Live Score
-- ==========================================
-- This script documents the database, storage integration, external stage,
-- file format, and table configurations used to store raw cricket scores.

USE DATABASE CRICKET_DB;
USE SCHEMA CRICKET_SCHEMA;

-- ------------------------------------------
-- 1. Create Storage Integration (AWS S3)
-- ------------------------------------------
-- Grants Snowflake secure access to the S3 bucket without exposing credentials.
-- Note: Replace the Role ARN and Bucket name with your specific configurations.
CREATE OR REPLACE STORAGE INTEGRATION s3_cricket_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('s3://airflowdemo1817/')
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/SnowflakeS3AccessRole';

-- Execute this description to retrieve:
--   a. STORAGE_AWS_IAM_USER_ARN (Snowflake service user ARN)
--   b. STORAGE_AWS_EXTERNAL_ID (Unique ID for AWS trust policy mapping)
-- Use these values to configure the Trust Relationship in your AWS IAM Role.
DESCRIBE INTEGRATION s3_cricket_integration;


-- ------------------------------------------
-- 2. Create File Format (Parquet)
-- ------------------------------------------
-- The Databricks Spark cleaning job outputs Parquet files, so we configure
-- Snowflake to read Parquet directly.
CREATE OR REPLACE FILE FORMAT parquet_format
  TYPE = 'PARQUET'
  COMPRESSION = 'SNAPPY';


-- ------------------------------------------
-- 3. Create External Stage
-- ------------------------------------------
-- A stage mapping directly to the processed bucket directory using our secure integration.
CREATE OR REPLACE STAGE CRICKET_STAGE
  URL = 's3://airflowdemo1817/cricket/'
  STORAGE_INTEGRATION = s3_cricket_integration
  FILE_FORMAT = parquet_format;


-- ------------------------------------------
-- 4. Target Tables Setup
-- ------------------------------------------

-- Matches Table
CREATE OR REPLACE TABLE CRICKET_MATCHES (
    MATCH_ID VARCHAR(100),
    TEAM_1 VARCHAR(100),
    TEAM_2 VARCHAR(100),
    MATCH_NAME VARCHAR(255),
    MATCHTYPE VARCHAR(50),
    STATUS VARCHAR(255),
    MATCH_DATE DATE,
    VENUE_NAME VARCHAR(255),
    CITY VARCHAR(100),
    DATETIMEGMT TIMESTAMP_NTZ,
    MATCHSTARTED BOOLEAN,
    MATCHENDED BOOLEAN,
    FETCH_DATE DATE
);

-- Score Table
CREATE OR REPLACE TABLE CRICKET_SCORE (
    MATCH_ID VARCHAR(100),
    INNINGS_ORDER INT,
    TEAM_NAME VARCHAR(100),
    RUNS INT,
    WICKETS INT,
    OVERS FLOAT
);
