-- 02_Create_Dimensions_and_Facts.sql
USE FinanceBI;
GO

-- DROP tables if exist (pour full refresh structure, utile en dev)

IF OBJECT_ID('staging.Budget_raw','U') IS NOT NULL DROP TABLE staging.Budget_raw;
IF OBJECT_ID('staging.Depenses_raw','U') IS NOT NULL DROP TABLE staging.Depenses_raw;
IF OBJECT_ID('staging.DimTemps_raw','U') IS NOT NULL DROP TABLE staging.DimDate_raw;
IF OBJECT_ID('staging.DimDepartement_raw','U') IS NOT NULL DROP TABLE staging.DimDepartement_raw;
IF OBJECT_ID('staging.DimRegion_raw','U') IS NOT NULL DROP TABLE staging.DimRegion_raw;
IF OBJECT_ID('staging.DimTypeDepense_raw','U') IS NOT NULL DROP TABLE staging.DimTypeDepense_raw;


IF OBJECT_ID('dbo.DimDepartement','U') IS NOT NULL DROP TABLE dbo.DimDepartement;
IF OBJECT_ID('dbo.DimRegion','U') IS NOT NULL DROP TABLE dbo.DimRegion;
IF OBJECT_ID('dbo.DimTypeDepense','U') IS NOT NULL DROP TABLE dbo.DimTypeDepense;
IF OBJECT_ID('dbo.DimDate','U') IS NOT NULL DROP TABLE dbo.DimDate;
IF OBJECT_ID('dbo.Budget','U') IS NOT NULL DROP TABLE dbo.Budget;
IF OBJECT_ID('dbo.Depenses','U') IS NOT NULL DROP TABLE dbo.Depenses;

IF OBJECT_ID('dbo.Log_ETL','U') IS NOT NULL DROP TABLE dbo.Log_ETL;

GO


-- === Staging (raw files) ===
CREATE TABLE staging.DimDepartement_raw (
    ID_Departement INT,
    Nom_Departement NVARCHAR(200)
);

CREATE TABLE staging.DimRegion_raw (
    ID_Region INT,
    Nom_Region NVARCHAR(200),
    Pays NVARCHAR(100)
);

CREATE TABLE staging.DimTypeDepense_raw (
    ID_TypeDepense INT,
    Nom_TypeDepense NVARCHAR(200)
);

CREATE TABLE staging.DimDate_raw (
    DateID INT PRIMARY KEY,
    Annee INT NOT NULL,
    Mois INT NOT NULL,
    NomMois NVARCHAR(20) NOT NULL,
    Trimestre INT NOT NULL
);

CREATE TABLE staging.Budget_raw (
    DateID INT NOT NULL,
    Year INT NOT NULL,
    Month INT NOT NULL,
    ID_Departement INT NOT NULL,
    ID_Region INT NOT NULL,
    ID_TypeDepense INT NOT NULL,
    MontantBudget DECIMAL(18,2) NOT NULL
  );

CREATE TABLE staging.Depenses_raw (
    DateID INT NOT NULL,
    Year INT NOT NULL,
    Month INT NOT NULL,
    ID_Departement INT NOT NULL,
    ID_Region INT NOT NULL,
    ID_TypeDepense INT NOT NULL,
    MontantReel DECIMAL(18,2) NOT NULL
);
GO



---  Schéma DBO ( Gold Layer) ---
-- Dimensions

CREATE TABLE dbo.DimDepartement (
    ID_Departement INT PRIMARY KEY,
    Nom_Departement NVARCHAR(100) NOT NULL
);


CREATE TABLE dbo.DimRegion (
    ID_Region INT PRIMARY KEY,
    Nom_Region NVARCHAR(100) NOT NULL,
    Pays NVARCHAR(50) NOT NULL
);

CREATE TABLE dbo.DimTypeDepense (
    ID_TypeDepense INT PRIMARY KEY,
    Nom_TypeDepense NVARCHAR(100) NOT NULL
);

CREATE TABLE dbo.DimDate (
    DateID INT PRIMARY KEY,
    Annee INT NOT NULL,
    Mois INT NOT NULL,
    NomMois NVARCHAR(20) NOT NULL,
    Trimestre INT NOT NULL
);

-- Faits
CREATE TABLE dbo.Budget (
    DateID INT NOT NULL,
    Year INT NOT NULL,
    Month INT NOT NULL,
    ID_Departement INT NOT NULL,
    ID_Region INT NOT NULL,
    ID_TypeDepense INT NOT NULL,
    MontantBudget DECIMAL(18,2) NOT NULL
    FOREIGN KEY (DateID) REFERENCES DimDate(DateID),
    FOREIGN KEY (ID_Departement) REFERENCES DimDepartement(ID_Departement),
    FOREIGN KEY (ID_Region) REFERENCES DimRegion(ID_Region),
    FOREIGN KEY (ID_TypeDepense) REFERENCES DimTypeDepense(ID_TypeDepense)
);

CREATE TABLE dbo.Depenses (
    DateID INT NOT NULL,
    Year INT NOT NULL,
    Month INT NOT NULL,
    ID_Departement INT NOT NULL,
    ID_Region INT NOT NULL,
    ID_TypeDepense INT NOT NULL,
    MontantReel DECIMAL(18,2) NOT NULL
    FOREIGN KEY (DateID) REFERENCES DimDate(DateID),
    FOREIGN KEY (ID_Departement) REFERENCES DimDepartement(ID_Departement),
    FOREIGN KEY (ID_Region) REFERENCES DimRegion(ID_Region),
    FOREIGN KEY (ID_TypeDepense) REFERENCES DimTypeDepense(ID_TypeDepense)
);



-- Logging ETL

CREATE TABLE dbo.Log_ETL (

    LogID INT IDENTITY(1,1) PRIMARY KEY,
    RunDateTime DATETIME NOT NULL DEFAULT GETDATE(),
    JobName NVARCHAR(100) NOT NULL,
    ObjectName NVARCHAR(100) NULL, -- table or file loaded
    RowsStaging INT NULL,
    RowsInserted INT NULL,
    DQ_Errors INT NULL,
    ErrorDetails NVARCHAR(MAX) NULL,
    Status NVARCHAR(20) NULL -- 'SUCCESS' / 'FAIL' 
);

GO