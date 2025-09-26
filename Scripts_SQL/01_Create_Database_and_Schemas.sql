-- 01_Create_Database_and_Schemas.sql
-- Run as admin in SSMS

IF DB_ID('FinanceBI') IS NULL
    CREATE DATABASE FinanceBI;
GO

USE FinanceBI;
GO

-- Création des schémas (si non existants)
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'staging') EXEC('CREATE SCHEMA staging');
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'reporting') EXEC('CREATE SCHEMA reporting');
-- dbo exists by default
GO

