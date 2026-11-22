-- Run once in a Snowflake worksheet (role with CREATE DATABASE rights, e.g. SYSADMIN).
CREATE DATABASE IF NOT EXISTS JOBMARKET;
CREATE SCHEMA IF NOT EXISTS JOBMARKET.PUBLIC;
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
  WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;

-- The RAW_JOBS table is created automatically at the first load.
-- dbt creates the PUBLIC_STAGING and PUBLIC_MARTS schemas and loads the seeds.
