-- 06Load_From_Staging.sql


-- =============================================
-- Procédure : Load_From_Staging_DQ
-- Description : Charge les données de staging vers dbo avec des règles de Data Quality
-- =============================================

USE FinanceBI;
GO

IF OBJECT_ID('dbo.Load_From_Staging_DQ','P') IS NOT NULL
    DROP PROCEDURE dbo.Load_From_Staging_DQ;
GO

CREATE PROCEDURE dbo.Load_From_Staging_DQ
AS
BEGIN
    SET NOCOUNT ON;

    
    DECLARE @start DATETIME = GETDATE();

    ----------------------------------------------------------------
    -- 0) Full refresh prep: delete target tables in safe order
    ----------------------------------------------------------------
    BEGIN TRY
        BEGIN TRAN;
            DELETE FROM dbo.Depenses;
            DELETE FROM dbo.Budget;

            DELETE FROM dbo.DimTypeDepense;
            DELETE FROM dbo.DimRegion;
            DELETE FROM dbo.DimDepartement;
            DELETE FROM dbo.DimDate;
        COMMIT TRAN;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK TRAN;
        INSERT INTO dbo.Log_ETL(RunDateTime,JobName, ObjectName, Status, ErrorDetails)
        VALUES(GETDATE(),'Load_From_Staging','PreDelete','FAIL', ERROR_MESSAGE());
        RETURN; 
    END CATCH;

    ----------------------------------------------------------------
    -- 1) Load DimDepartement
    ----------------------------------------------------------------
    BEGIN TRY
        BEGIN TRAN;

        DECLARE @st_total INT = (SELECT COUNT(*) FROM staging.DimDepartement_raw);
        DECLARE @null_name INT = (SELECT COUNT(*) FROM staging.DimDepartement_raw WHERE Nom_Departement IS NULL OR LTRIM(RTRIM(Nom_Departement)) = '');

        INSERT INTO dbo.DimDepartement (ID_Departement, Nom_Departement)
        SELECT DISTINCT ID_Departement, LTRIM(RTRIM(Nom_Departement))
        FROM staging.DimDepartement_raw 
        WHERE Nom_Departement IS NOT NULL AND LTRIM(RTRIM(Nom_Departement)) <> '';

        DECLARE @inserted INT = @@ROWCOUNT;
        DECLARE @dq INT = @st_total - @inserted;

        INSERT INTO dbo.Log_ETL(RunDateTime,JobName, ObjectName, RowsStaging, RowsInserted, DQ_Errors, ErrorDetails, Status)
        VALUES (GETDATE(),'Load_Dim','DimDepartement', @st_total, @inserted, @dq,
                CASE WHEN @dq > 0 THEN CONCAT('Missing names: ', @null_name) ELSE NULL END, 'SUCCESS');

        COMMIT TRAN;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK TRAN;
        INSERT INTO dbo.Log_ETL(RunDateTime,JobName, ObjectName, Status, ErrorDetails)
        VALUES(GETDATE(),'Load_Dim','DimDepartement','FAIL', ERROR_MESSAGE());
    END CATCH;

    ----------------------------------------------------------------
    -- 2) Load DimRegion
    ----------------------------------------------------------------
    BEGIN TRY
        BEGIN TRAN;

        DECLARE @st_total2 INT = (SELECT COUNT(*) FROM staging.DimRegion_raw);
        DECLARE @null_reg INT = (SELECT COUNT(*) FROM staging.DimRegion_raw WHERE Nom_Region IS NULL OR LTRIM(RTRIM(Nom_Region)) = '');

        INSERT INTO dbo.DimRegion (ID_Region, Nom_Region, Pays)
        SELECT DISTINCT ID_Region, LTRIM(RTRIM(Nom_Region)), ISNULL(LTRIM(RTRIM(Pays)),'France')
        FROM staging.DimRegion_raw
        WHERE Nom_Region IS NOT NULL AND LTRIM(RTRIM(Nom_Region)) <> '';

        DECLARE @ins2 INT = @@ROWCOUNT;
        DECLARE @dq2 INT = @st_total2 - @ins2;

        INSERT INTO dbo.Log_ETL(RunDateTime, JobName, ObjectName, RowsStaging, RowsInserted, DQ_Errors, ErrorDetails, Status)
        VALUES(GETDATE(),'Load_Dim','DimRegion', @st_total2, @ins2, @dq2,
                CASE WHEN @dq2 > 0 THEN CONCAT('Missing names: ', @null_reg) ELSE NULL END, 'SUCCESS');

        COMMIT TRAN;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK TRAN;
        INSERT INTO dbo.Log_ETL(RunDateTime, JobName, ObjectName, Status, ErrorDetails)
        VALUES(GETDATE(),'Load_Dim','DimRegion','FAIL', ERROR_MESSAGE());
    END CATCH;

    ----------------------------------------------------------------
    -- 3) Load DimTypeDepense
    ----------------------------------------------------------------
    BEGIN TRY
        BEGIN TRAN;

        DECLARE @st_total3 INT = (SELECT COUNT(*) FROM staging.DimTypeDepense_raw);
        DECLARE @null_t INT = (SELECT COUNT(*) FROM staging.DimTypeDepense_raw WHERE Nom_TypeDepense IS NULL OR LTRIM(RTRIM(Nom_TypeDepense)) = '');

        INSERT INTO dbo.DimTypeDepense (ID_TypeDepense, Nom_TypeDepense)
        SELECT DISTINCT ID_TypeDepense, LTRIM(RTRIM(Nom_TypeDepense))
        FROM staging.DimTypeDepense_raw
        WHERE Nom_TypeDepense IS NOT NULL AND LTRIM(RTRIM(Nom_TypeDepense)) <> '';

        DECLARE @ins3 INT = @@ROWCOUNT;
        DECLARE @dq3 INT = @st_total3 - @ins3;

        INSERT INTO dbo.Log_ETL(RunDateTime,JobName, ObjectName, RowsStaging, RowsInserted, DQ_Errors, ErrorDetails, Status)
        VALUES(GETDATE(), 'Load_Dim','DimTypeDepense', @st_total3, @ins3, @dq3,
                CASE WHEN @dq3 > 0 THEN CONCAT('Missing names: ', @null_t) ELSE NULL END, 'SUCCESS');

        COMMIT TRAN;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK TRAN;
        INSERT INTO dbo.Log_ETL(RunDateTime, JobName, ObjectName, Status, ErrorDetails)
        VALUES(GETDATE(),'Load_Dim','DimTypeDepense','FAIL', ERROR_MESSAGE());
    END CATCH;

   
    ----------------------------------------------------------------
    -- 5) Load DimDate
    ----------------------------------------------------------------
    BEGIN TRY
        BEGIN TRAN;

        DECLARE @st_total5 INT = (SELECT COUNT(*) FROM staging.DimDate_raw);
        DECLARE @invalid_months INT = (SELECT COUNT(*) FROM staging.DimDate_raw WHERE Mois NOT BETWEEN 1 AND 12 OR Mois IS NULL);

        INSERT INTO dbo.DimDate (DateID, Annee, Mois, NomMois, Trimestre)
        SELECT DISTINCT DateID, Annee, Mois, NomMois, Trimestre
        FROM staging.DimDate_raw
        WHERE Mois BETWEEN 1 AND 12 AND Annee IS NOT NULL;

        DECLARE @ins5 INT = @@ROWCOUNT;
        DECLARE @dq5 INT = @st_total5 - @ins5;

        INSERT INTO dbo.Log_ETL(RunDateTime, JobName, ObjectName, RowsStaging, RowsInserted, DQ_Errors, ErrorDetails, Status)
        VALUES(GETDATE(),'Load_Dim','DimDate', @st_total5, @ins5, @dq5,
                CASE WHEN @invalid_months > 0 THEN CONCAT('Invalid months: ', @invalid_months) ELSE NULL END, 'SUCCESS');

        COMMIT TRAN;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK TRAN;
        INSERT INTO dbo.Log_ETL(RunDateTime,JobName, ObjectName, Status, ErrorDetails)
        VALUES(GETDATE(),'Load_Dim','DimTemps','FAIL', ERROR_MESSAGE());
    END CATCH;

    ----------------------------------------------------------------
    -- 6) Load Budget (facts) with DQ checks: MontantBudget > 0, month in 1..12, FK existence. Aggregate to avoid dupes.
    ----------------------------------------------------------------
    BEGIN TRY
        BEGIN TRAN;

        DECLARE @st_totalB INT = (SELECT COUNT(*) FROM staging.Budget_raw);
        DECLARE @invalid_amounts INT = (SELECT COUNT(*) FROM staging.Budget_raw WHERE MontantBudget IS NULL OR MontantBudget <= 0);
        DECLARE @invalid_months_b INT = (SELECT COUNT(*) FROM staging.Budget_raw WHERE Month NOT BETWEEN 1 AND 12 OR Month IS NULL);

        -- Insert aggregated to ensure unique key
        INSERT INTO dbo.Budget (DateID ,Year, Month, ID_Departement, ID_Region, ID_TypeDepense, MontantBudget)
        SELECT DateID,Year, Month, ID_Departement, ID_Region, ID_TypeDepense, SUM(MontantBudget) AS MontantBudget
        FROM staging.Budget_raw s
        WHERE MontantBudget IS NOT NULL AND MontantBudget > 0
          AND Month BETWEEN 1 AND 12
          AND EXISTS (SELECT 1 FROM dbo.DimDate d WHERE d.DateID = s.DateID)
          AND EXISTS (SELECT 1 FROM dbo.DimDepartement d WHERE d.ID_Departement = s.ID_Departement)
          AND EXISTS (SELECT 1 FROM dbo.DimRegion r WHERE r.ID_Region = s.ID_Region)
          AND EXISTS (SELECT 1 FROM dbo.DimTypeDepense t WHERE t.ID_TypeDepense = s.ID_TypeDepense)
        GROUP BY DateID, Year, Month, ID_Departement, ID_Region, ID_TypeDepense;

        DECLARE @insB INT = @@ROWCOUNT;
        DECLARE @dqB INT = @st_totalB - @insB;

        INSERT INTO dbo.Log_ETL(RunDateTime,JobName, ObjectName, RowsStaging, RowsInserted, DQ_Errors, ErrorDetails, Status)
        VALUES(GETDATE(),'Load_Fact','Budget', @st_totalB, @insB, @dqB,
                CASE WHEN (@invalid_amounts + @invalid_months_b) > 0 THEN CONCAT('Invalid amounts: ', @invalid_amounts, '; Invalid months: ', @invalid_months_b) ELSE NULL END, 'SUCCESS');

        COMMIT TRAN;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK TRAN;
        INSERT INTO dbo.Log_ETL(RunDateTime,JobName, ObjectName, Status, ErrorDetails)
        VALUES(GETDATE(),'Load_Fact','Budget','FAIL', ERROR_MESSAGE());
    END CATCH;

    ----------------------------------------------------------------
    -- 7) Load Depenses (facts) with DQ rules (same approach)
    ----------------------------------------------------------------
    BEGIN TRY
        BEGIN TRAN;

        DECLARE @st_totalD INT = (SELECT COUNT(*) FROM staging.Depenses_raw);
        DECLARE @invalid_amounts_d INT = (SELECT COUNT(*) FROM staging.Depenses_raw WHERE MontantReel IS NULL OR MontantReel < 0);
        DECLARE @invalid_months_d INT = (SELECT COUNT(*) FROM staging.Depenses_raw WHERE Month NOT BETWEEN 1 AND 12 OR Month IS NULL);

        INSERT INTO dbo.Depenses (DateID,Year, Month, ID_Departement, ID_Region, ID_TypeDepense, MontantReel)
        SELECT DateID,Year, Month, ID_Departement, ID_Region, ID_TypeDepense, SUM(MontantReel) AS MontantReel
        FROM staging.Depenses_raw s
        WHERE MontantReel IS NOT NULL AND MontantReel >= 0
          AND Month BETWEEN 1 AND 12
          AND EXISTS (SELECT 1 FROM dbo.DimDate d WHERE d.DateID = s.DateID)
          AND EXISTS (SELECT 1 FROM dbo.DimDepartement d WHERE d.ID_Departement = s.ID_Departement)
          AND EXISTS (SELECT 1 FROM dbo.DimRegion r WHERE r.ID_Region = s.ID_Region)
          AND EXISTS (SELECT 1 FROM dbo.DimTypeDepense t WHERE t.ID_TypeDepense = s.ID_TypeDepense)
        GROUP BY DateID,Year, Month, ID_Departement, ID_Region, ID_TypeDepense;

        DECLARE @insD INT = @@ROWCOUNT;
        DECLARE @dqD INT = @st_totalD - @insD;

        INSERT INTO dbo.Log_ETL(RunDateTime,JobName, ObjectName, RowsStaging, RowsInserted, DQ_Errors, ErrorDetails, Status)
        VALUES(GETDATE(),'Load_Fact','Depenses', @st_totalD, @insD, @dqD,
                CASE WHEN (@invalid_amounts_d + @invalid_months_d) > 0 THEN CONCAT('Invalid amounts: ', @invalid_amounts_d, '; Invalid months: ', @invalid_months_d) ELSE NULL END, 'SUCCESS');

        COMMIT TRAN;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK TRAN;
        INSERT INTO dbo.Log_ETL(RunDateTime,JobName, ObjectName, Status, ErrorDetails)
        VALUES(GETDATE(),'Load_Fact','Depenses','FAIL', ERROR_MESSAGE());
    END CATCH;

    ----------------------------------------------------------------
    -- End: summary log
    ----------------------------------------------------------------
    DECLARE @totalRowLoaded INT;
    SET @totalRowLoaded = @inserted + @ins2+@st_total3+@st_total5+@insB+@insD;
    INSERT INTO dbo.Log_ETL(RunDateTime,JobName, ObjectName, RowsStaging, RowsInserted, DQ_Errors, ErrorDetails, Status)
    VALUES(GETDATE(),'Load_Summary','All Table', @totalRowLoaded, @totalRowLoaded, 0, 'ETL Completed', 'SUCCESS');

END
GO

exec dbo.Load_From_Staging_DQ;

