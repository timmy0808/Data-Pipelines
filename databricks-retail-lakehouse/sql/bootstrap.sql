-- Run once before the first bundle deployment.
-- Change the principal to your user, group, or service principal as appropriate.

CREATE CATALOG IF NOT EXISTS retail_dev;
CREATE SCHEMA IF NOT EXISTS retail_dev.retail_sources;
CREATE SCHEMA IF NOT EXISTS retail_dev.retail_lakehouse;
CREATE VOLUME IF NOT EXISTS retail_dev.retail_sources.landing;

-- Example grants. Replace `account users` with a narrower principal in production.
GRANT USE CATALOG ON CATALOG retail_dev TO `account users`;
GRANT USE SCHEMA, CREATE TABLE, CREATE VOLUME
ON SCHEMA retail_dev.retail_sources TO `account users`;
GRANT USE SCHEMA, CREATE TABLE
ON SCHEMA retail_dev.retail_lakehouse TO `account users`;
