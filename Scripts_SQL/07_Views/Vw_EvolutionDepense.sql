USE FinanceBI;
GO

CREATE OR ALTER VIEW reporting.vw_EvolutionTypeDepense AS
WITH DepensesParType AS (
    SELECT 
        td.Nom_TypeDepense,
        d.Year,
        SUM(d.MontantReel) AS TotalDepense
    FROM dbo.Depenses d
    INNER JOIN dbo.DimTypeDepense td ON d.ID_TypeDepense = td.ID_TypeDepense
    GROUP BY td.Nom_TypeDepense, d.Year
),
Pivoted AS (
    SELECT 
        Nom_TypeDepense,
        MAX(CASE WHEN Year = 2024 THEN TotalDepense END) AS Depense2024,
        MAX(CASE WHEN Year = 2025 THEN TotalDepense END) AS Depense2025
    FROM DepensesParType
    GROUP BY Nom_TypeDepense
)
SELECT 
    Nom_TypeDepense,
    ISNULL(Depense2024,0) AS Depense2024,
    ISNULL(Depense2025,0) AS Depense2025,
    (ISNULL(Depense2025,0) - ISNULL(Depense2024,0)) AS Ecart_Absolu,
    CASE 
        WHEN ISNULL(Depense2024,0) = 0 THEN NULL
        ELSE CONCAT (CAST(((ISNULL(Depense2025,0) - ISNULL(Depense2024,0)) / Depense2024) * 100 as DECIMAL (10,2)),'%')
    END AS Taux_Evolution_Pourcent,
    RANK() OVER (ORDER BY (ISNULL(Depense2025,0) - ISNULL(Depense2024,0)) DESC) AS Rang_Croissance
FROM Pivoted;
GO
