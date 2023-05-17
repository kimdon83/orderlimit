DECLARE @mthwitdh AS INT
DECLARE @3Mwds AS FLOAT

SELECT @mthwitdh = 7;

SELECT @3Mwds = (
        SELECT COUNT(*) AS WDs
        FROM [ivy.mm.dim.date]
        WHERE IsKissHoliday != 1 AND thedate BETWEEN DATEADD(MM, - 3, DATEADD(DD, - 1, GETDATE())) AND DATEADD(DD, - 1, GETDATE())
        GROUP BY IsKissHoliday
        );

WITH pppDailyThisMonth
AS (
    SELECT SUM(qty) AS thisMthReOdqty, material, plant
    FROM [ivy.sd.fact.bill_ppp]
    WHERE act_date BETWEEN DATEADD(DD, 1, EOMONTH(GETDATE(), - 1)) AND GETDATE() AND ordsqc > 1
    GROUP BY material, plant
    ), ppp
    --avgMreorder within 3month, material, plant FROM [ivy.sd.fact.bill_ppp]
AS (
    SELECT SUM(qty) AS reorder3M, material, plant
    FROM [ivy.sd.fact.bill_ppp]
    WHERE act_date BETWEEN DATEADD(MM, - 3, DATEADD(DD, - 1, GETDATE())) AND DATEADD(DD, - 1, GETDATE()) AND ordsqc > 1
    GROUP BY material, plant
    ), backOrder
    -- avgMbo within 3month, material, plant FROM [ivy.sd.fact.bo] 
AS (
    SELECT SUM(bo_qty) AS bo3M, material, plant
    FROM [ivy.sd.fact.bo]
    WHERE (act_date BETWEEN DATEADD(MM, - 3, DATEADD(DD, - 1, GETDATE())) AND DATEADD(DD, - 1, GETDATE()))
    GROUP BY material, plant
    ), pppbo
AS (
    SELECT cast(reorder3M AS FLOAT) / @3Mwds AS reorderPerWDs, T1.material, T1.plant, cast(bo3M AS FLOAT) / @3Mwds AS boPerWDs
    FROM ppp T1
    LEFT JOIN backOrder T2 ON T1.material = T2.material AND T1.plant = T2.plant
        --ORDER BY plant, material
    ), T4fcst
    -- Table to make fcst table. FROM this month to upcoming 5 monthl
AS (
    SELECT material, SUM(eship) AS eship, FORMAT(act_date, 'MMyyyy') AS MMYYYY, plant
    FROM [ivy.mm.dim.factfcst]
    WHERE act_date BETWEEN DATEADD(DD, - DAY(GETDATE()), GETDATE()) AND DATEADD(MM, @mthwitdh + 1, DATEADD(DD, - DAY(GETDATE()), GETDATE()))
    GROUP BY material, FORMAT(act_date, 'MMyyyy'), plant
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
    ), Tpoasn
AS (
    SELECT material, plant, act_date, sum(po_qty) AS po_qty, sum(asn_qty) AS asn_qty
    FROM [ivy.mm.dim.fact_poasn]
    -- WHERE po_num NOT LIKE '43%' -- exclude intra_company po not exclude for individual plant
    GROUP BY material, plant, act_date
    ), mrp01
AS (
    SELECT *
    FROM [ivy.mm.dim.mrp01]
    WHERE pgr != 'IEC' -- exclude IEC for total stock
    ), TOTAL
AS (
    SELECT T2.PL_plant, T1.thedate, T3.material, T3.nsp, T1.IsKissHoliday, T6.boPerWDs, T5.po_qty + T5.asn_qty AS poasn_qty, 
    T6.reorderPerWDs, T8.total_stock - T8.blocked - T8.subcont_qty AS On_hand_qty, T9.fcstPerWDs, 
    T9.accumWDs, T10.thisMthReOdqty
    FROM (
        SELECT DISTINCT PL_PLANT -- pl_plant
        FROM [ivy.mm.dim.mrp01]
        ) T2
    CROSS JOIN (
        SELECT THEDATE, IsKissHoliday
        FROM [ivy.mm.dim.date]
        WHERE thedate BETWEEN DATEADD(DAY, - 6, GETDATE()) AND DATEADD(MONTH, @mthwitdh, GETDATE())
        ) T1
    CROSS JOIN (
        SELECT a.MATERIAL, nsp --material
        FROM [ivy.mm.dim.mtrl] a
        INNER JOIN (
            SELECT material
            FROM [ivy.sd.fact.bill_ppp]
            WHERE act_date >= dateadd(mm, - 12, dateadd(dd, - 1, getdate())) AND qty > 0
            GROUP BY material
            
            UNION
            
            SELECT material
            FROM [ivy.mm.dim.factfcst]
            WHERE act_date >= dateadd(dd, - 1, getdate())
            GROUP BY material

            UNION

            SELECT material
            FROM [ivy.mm.dim.mrp01]
            WHERE total_stock > 0
            GROUP BY material
            ) b ON a.material = b.material
--        WHERE MS = '01' AND DIVISION = 'C2'
        ) T3
    LEFT JOIN Tpoasn T5 ON T3.material = T5.material -- poasn_qty
        AND T2.pl_plant = T5.plant AND T1.TheDate = T5.act_date
    LEFT JOIN pppbo T6 ON T3.material = T6.material -- average Monthly reorder qty
        AND T2.pl_plant = T6.plant
    LEFT JOIN [ivy.mm.dim.mrp01] T8 ON T3.material = T8.material -- on_hand qty
        AND T2.pl_plant = T8.pl_plant
    LEFT JOIN fcst T9 ON T3.material = T9.material -- fcstPerWDs, IsKissHoliday
        AND T2.pl_plant = T9.plant AND T9.TheDate = T1.TheDate
    LEFT JOIN pppDailyThisMonth T10 ON T10.material = T3.material AND T10.plant = T2.pl_plant
        -- WHERE T8.pgr != 'IEC' -- exclude IEC for total stock
        --LEFT JOIN [ivy.sd.fact.order] od ON od.act_date= T1.TheDate and od.material=T3.material
    ), TOTAL2
    -- NULL value to 0 (avgDbo, poasn_qty, avgDreorder, fcstPerWDs,On_hand_qty)
AS (
    SELECT pl_plant, TheDate, material, 
    CASE WHEN nsp IS NULL THEN 0 ELSE nsp END AS nsp, 
    CASE WHEN (boPerWDs IS NULL) THEN 0 ELSE boPerWDs END AS avgDbo, 
    CASE WHEN (poasn_qty IS NULL) THEN 0 ELSE poasn_qty END AS poasn_qty, 
    CASE WHEN (reorderPerWDs IS NULL) THEN 0 ELSE reorderPerWDs END AS avgDreorder, 
    CASE WHEN (fcstPerWDs IS NULL) THEN 0 ELSE fcstPerWDs END AS fcstPerWDs, 
    CASE WHEN (On_hand_qty IS NULL) THEN 0 ELSE On_hand_qty END AS On_hand_qty, 
    CASE WHEN (thisMthReOdqty IS NULL) THEN 0 ELSE thisMthReOdqty END AS thisMthReOdqty, 
        IsKissHoliday, accumWDs
    FROM Total
    )
SELECT pl_plant AS plant, TheDate, material AS mtrl, nsp, avgDbo, poasn_qty, avgDreorder, On_hand_qty, 
    CASE WHEN (fcstPerWDs = 0 AND IsKissHoliday = 0 AND pl_plant IN ('1100', '1400','G140','G110')) THEN 
                avgDreorder + avgDbo ELSE fcstPerWDs END AS fcstD, thisMthReOdqty
FROM TOTAL2
ORDER BY plant, mtrl, TheDate