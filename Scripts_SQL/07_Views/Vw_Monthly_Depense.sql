use FinanceBI ;
GO 

CREATE OR ALTER VIEW reporting.vw_DepensesMensuelles AS
WITH Monthly AS (
    SELECT 
        dt.Annee,
        dt.Mois,
        dt.NomMois,
        SUM(dep.MontantReel) AS DepenseTotale
    FROM dbo.Depenses dep 
    LEFT JOIN dbo.DimDate dt on dt.DateID = dep.DateID
    GROUP BY dt.Annee,dt.Mois, dt.NomMois
)
SELECT *,
       SUM(DepenseTotale) OVER (PARTITION BY Annee ORDER BY Mois) AS Cumul_Annuel 

FROM Monthly;
GO
