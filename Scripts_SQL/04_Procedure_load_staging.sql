USE FinanceBI;
GO

-- =============================================
-- Procédure : Load_Staging
-- Description : Vide les tables staging et recharge les CSV
-- =============================================
CREATE OR ALTER PROCEDURE staging.Load_Staging
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        BEGIN TRANSACTION; 

        -- Vider les tables staging
        TRUNCATE TABLE staging.DimDepartement_raw;
        TRUNCATE TABLE staging.DimRegion_raw;
        TRUNCATE TABLE staging.DimTypeDepense_raw;
        TRUNCATE TABLE staging.DimDate_raw;
        TRUNCATE TABLE staging.Budget_raw;
        TRUNCATE TABLE staging.Depenses_raw;

        DECLARE @Count1 INT, @Count2 INT, @Count3 INT, @Count4 INT, @Count5 INT, @Count6 INT, @Total INT; 

        -- génération et chargement de la table de dimension Date
        DECLARE @StartDate DATE = '2024-01-01';
        DECLARE @EndDate DATE   = '2025-12-01';
        SET @Count1 = 0;
        WHILE (@StartDate <= @EndDate)
        BEGIN
            INSERT INTO staging.DimDate_raw(DateID, Annee, Mois, NomMois, Trimestre)
            VALUES (
                YEAR(@StartDate) * 100 + MONTH(@StartDate),    -- Clé AAAAMM
                YEAR(@StartDate),
                MONTH(@StartDate),
                FORMAT(@StartDate, 'MMMM', 'en-US'),           -- Mois en anglais
                ((MONTH(@StartDate) - 1) / 3) + 1              -- Trimestre
            );
            SET @Count1 = @@ROWCOUNT + @Count1;
            SET @StartDate = DATEADD(MONTH, 1, @StartDate);
            
        END;
       

        -- Charger les CSV
        BULK INSERT staging.DimDepartement_raw
        FROM 'C:\SqlData\FinanceBI\DimDepartement.csv'
        WITH (FORMAT='CSV', FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='\n', CODEPAGE='65001');
        SET @Count2 = @@ROWCOUNT;

        BULK INSERT staging.DimRegion_raw
        FROM 'C:\SqlData\FinanceBI\DimRegion.csv'
        WITH (FORMAT='CSV', FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='\n', CODEPAGE='65001');
        SET @Count3 = @@ROWCOUNT;

        BULK INSERT staging.DimTypeDepense_raw
        FROM 'C:\SqlData\FinanceBI\DimTypeDepense.csv'
        WITH (FORMAT='CSV', FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='\n', CODEPAGE='65001');
        SET @Count4 = @@ROWCOUNT;

        BULK INSERT staging.Budget_raw
        FROM 'C:\SqlData\FinanceBI\Budget.csv'
        WITH (FORMAT='CSV', FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='\n', CODEPAGE='65001');
        SET @Count5 = @@ROWCOUNT;

        BULK INSERT staging.Depenses_raw
        FROM 'C:\SqlData\FinanceBI\Depenses.csv'
        WITH (FORMAT='CSV', FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='\n', CODEPAGE='65001');
        SET @Count6 = @@ROWCOUNT;

        PRINT 'Chargement des fichiers CSV terminé avec succès et génération de la table de date.';

        -- Log succès
        SET @Total = @Count1 + @Count2 + @Count3 + @Count4 + @Count5 + @Count6;  
        INSERT INTO dbo.Log_ETL(RunDateTime, JobName, ObjectName, RowsStaging, RowsInserted, DQ_Errors, ErrorDetails, Status)
        VALUES (GETDATE(), 'Load_Staging', 'schema Staging', @Total, @Total, NULL, NULL, 'SUCCESS');

        COMMIT TRANSACTION; 
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION; 

        -- Log erreur
        INSERT INTO dbo.Log_ETL(RunDateTime, JobName, ObjectName, RowsStaging, RowsInserted, DQ_Errors, ErrorDetails, Status)
        VALUES (GETDATE(), 'Load_Staging', 'schema Staging', NULL, NULL, NULL, ERROR_MESSAGE(), 'FAIL');

        THROW; -- Renvoie l’erreur pour debug
    END CATCH
END;
GO


exec staging.Load_Staging;
 
