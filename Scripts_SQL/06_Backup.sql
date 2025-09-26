-- 08_Backup_Script.sql
USE master;
GO

-- Ajuste le chemin où SQL Server peut écrire (ex: disque local, accessible)
BACKUP DATABASE FinanceBI
TO DISK = 'C:\SqlData\FinanceBI\FinanceBI_backup.bak'
WITH INIT;
GO
