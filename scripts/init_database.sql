/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'legacy_sybase7_dw' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'legacy_sybase7_dw' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

-- Connect to the default database (if not already connected)
\c postgres

-- Close DB conexions
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = 'legacy_sybase7_dw'
  AND pid <> pg_backend_pid();

-- Drop and recreate the 'legacy_sybase7_dw' database
DROP DATABASE IF EXISTS "legacy_sybase7_dw";


-- Create the 'legacy_sybase7_dw' database
CREATE DATABASE "legacy_sybase7_dw";

-- Now connect to the new database.
\c "legacy_sybase7_dw"

-- Create Schemas
CREATE SCHEMA IF NOT EXISTS bronze;

CREATE SCHEMA IF NOT EXISTS silver;

CREATE SCHEMA IF NOT EXISTS gold;
