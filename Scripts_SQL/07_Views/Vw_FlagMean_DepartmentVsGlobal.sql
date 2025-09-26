use FinanceBI ;
GO 

CREATE OR ALTER VIEW reporting.vw_DepartementFlagDepense AS
WITH DepAgg AS (
    SELECT 
        d.Nom_Departement,
        AVG(dep.MontantReel) AS DepenseMoyenne
    FROM dbo.Depenses dep
    INNER JOIN dbo.DimDepartement d ON dep.ID_Departement = d.ID_Departement
    GROUP BY d.Nom_Departement
),
GlobalAvg AS (
    SELECT AVG(MontantReel) AS MoyenneGlobale FROM dbo.Depenses
)
SELECT 
    da.Nom_Departement,
    da.DepenseMoyenne,
    g.MoyenneGlobale,
    CASE WHEN da.DepenseMoyenne > g.MoyenneGlobale THEN 1 ELSE 0 END AS Depense_Haute_Flag
FROM DepAgg da CROSS JOIN GlobalAvg g;
GO
