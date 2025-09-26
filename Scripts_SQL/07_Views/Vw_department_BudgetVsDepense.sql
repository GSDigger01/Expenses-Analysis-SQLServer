use FinanceBI ;
GO 

CREATE OR ALTER VIEW reporting.vw_BudgetVsDepenses AS
WITH BudgetDepenses AS (
    SELECT 
        --b.DateID,
        b.Year,
        --d.Nom_Departement,
        d.Nom_Departement,
        --r.Nom_Region,
        SUM(b.MontantBudget) AS TotalBudget,
        SUM(dep.MontantReel ) AS TotalDepense
    FROM dbo.Budget b
    INNER JOIN dbo.DimDepartement d ON b.ID_Departement = d.Id_Departement
    INNER JOIN dbo.DimRegion r ON b.ID_Region = r.Id_Region
    LEFT JOIN dbo.Depenses dep 
        ON b.ID_Departement = dep.ID_Departement
        AND b.DateID = dep.DateID
    --LEFT JOIN dbo.DimDepartement td ON td.ID_Departement = b.ID_TypeDepense
    --GROUP BY b.Year, b.Month, d.Nom_Departement, r.Nom_Region
    GROUP BY  b.Year ,d.Nom_Departement
    


)
SELECT *,
       CASE WHEN TotalBudget = 0 THEN NULL 
            ELSE CONCAT( CAST (ROUND((TotalDepense/TotalBudget) * 100, 1) as decimal (10,1)), '%')
       END AS PercentDepense,
       CASE WHEN TotalBudget - TotalDepense < 0 THEN N'Bad management🚫'
       ELSE  N'Good management ✔'
       END AS flag
FROM BudgetDepenses bg
ORDER BY Year DESC
--GROUP by bg.Nom_Departement
GO
