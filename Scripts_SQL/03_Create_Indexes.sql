-- 03_Create_Indexes.sql
USE FinanceBI;
GO

-- Index temporels / filtres fréquents
CREATE  INDEX IX_Budget_Date ON dbo.Budget(Year, Month);
CREATE  INDEX IX_Depenses_Date ON dbo.Depenses(Year, Month);

-- Index sur clés de jointure
CREATE NONCLUSTERED INDEX IX_Budget_Keys ON dbo.Budget(DateID,ID_Departement, ID_Region, ID_TypeDepense);
CREATE NONCLUSTERED INDEX IX_Depenses_Keys ON dbo.Depenses(DateID,ID_Departement, ID_Region, ID_TypeDepense);
GO

