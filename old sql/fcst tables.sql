DECLARE @mthwitdh AS INT
SELECT @mthwitdh = 7;

WITH T4fcst
    -- Table to make fcst table. FROM this month to upcoming 5 monthl
AS (
    SELECT T1.material, SUM(eship) AS eship, FORMAT(act_date, 'MMyyyy') AS MMYYYY, plant FROM [ivy.mm.dim.bo] T1
    LEFT JOIN [ivy.mm.dim.mtrl] T2 on T1.material =T2.material
    LEFT JOIN [ivy.mm.dim.factfcst] T3 on T1.material= T3.material
    WHERE bo_bf_pdt='yes' and locn='total' and BOdays_bf_pdt>14 and bo_seq=1 and T2.ms='01' and bo_qty>0
    and act_date BETWEEN DATEADD(DD, - DAY(GETDATE()), GETDATE()) AND DATEADD(MM, @mthwitdh + 1, DATEADD(DD, - DAY(GETDATE()), GETDATE()))
    GROUP BY T1.material, FORMAT(act_date, 'MMyyyy'), plant
    ), fcst
AS (
    SELECT T1.TheDate, T1.accumWDs, T1.MMYYYY, T1.IsKissHoliday, (1 - T3.IsKissHoliday) * (CONVERT(FLOAT, T2.eship) / T3.workdaysInMonth) AS fcstPerWDs, T2.plant, T2.material
    FROM (
        SELECT TheDate, workdaysInMonth AS WDs, workdaysInMonth - workdaysLeftInMonth AS accumWDs, MMYYYY, IsKissHoliday
        FROM [ivy.mm.dim.date]
        WHERE thedate BETWEEN DATEADD(DD, - DAY(GETDATE()), GETDATE()) AND DATEADD(MM, @mthwitdh + 1, DATEADD(DD, - DAY(GETDATE()), GETDATE()))
        ) T1
    LEFT JOIN T4fcst T2 ON T1.MMYYYY = T2.MMYYYY
    LEFT JOIN [ivy.mm.dim.date] T3 on T1.TheDate=T3.TheDate
    WHERE T1.thedate BETWEEN DATEADD(DAY, - 6, GETDATE()) AND DATEADD(MONTH, @mthwitdh, GETDATE())
    )

SELECT material,TheDate, sum(fcstPerWDs) as fcstPerWDs FROM fcst
GROUP BY TheDate, material
ORDER BY material, TheDate