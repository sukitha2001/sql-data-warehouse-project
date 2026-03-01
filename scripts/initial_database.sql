/*
Create Database and Schemas
Script Purpose:
This script creates a new database named 'DataWarehouse' after checking if it already exists.
If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas within the database: 'bronze', 'silver', and 'gold'.
WARNING:
Running this script will drop the entire 'Datawarehouse' database if it exists.
All data in the database will be permanently deleted. Proceed with caution and ensure you have proper backups before running this script.
*/

-- Drop the database if it exists, forcing disconnection of other users (Requires Postgres 13+)
DROP DATABASE IF EXISTS "DataWarehouse" WITH (FORCE);

-- Create the database
CREATE DATABASE "DataWarehouse";

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO




