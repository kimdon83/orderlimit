DECLARE @mthwitdh AS INT
DECLARE @3Mwds AS FLOAT

SELECT @mthwitdh = 7;

SELECT @3Mwds = (
        SELECT COUNT(*) AS WDs
        FROM [ivy.mm.dim.date]
        WHERE IsKissHoliday != 1 AND thedate BETWEEN DATEADD(MM, - 3, DATEADD(DD, - 1, GETDATE())) AND DATEADD(DD, - 1, GETDATE())
        GROUP BY IsKissHoliday
        );

WITH ppp
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
    SELECT cast(reorder3M AS FLOAT) / @3Mwds AS reorderPerWDs, T1.material, T1.plant, cast(bo3M AS FLOAT) / @3Mwds AS boPerWDs,
    cast(reorder3M AS FLOAT) / @3Mwds + cast(bo3M AS FLOAT) / @3Mwds AS demandPerWDs
    FROM ppp T1
    LEFT JOIN backOrder T2 ON T1.material = T2.material AND T1.plant = T2.plant
        --ORDER BY plant, material
    ), mindateT as(
    SELECT T1.material, --cast(po_date as date)as po_date, cast(getdate()+adj_pdt as date) as pdtdate, 
    CASE WHEN po_date is not null THEN cast(po_date as date) ELSE cast(getdate()+adj_pdt+10 as date) END mindate FROM [ivy.mm.dim.bo] T1
    LEFT JOIN [ivy.mm.dim.mtrl] T2 on T1.material =T2.material
    WHERE bo_bf_pdt='yes' and locn='total' and BOdays_bf_pdt>14 and bo_seq=1 and T2.ms='01' and bo_qty>0
),T4fcst
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
,fcst2 as (
    SELECT T1.material,TheDate, sum(fcstPerWDs) as fcstPerWDs, T2.mindate FROM mindateT T2
    Inner JOIN fcst T1 on T1.material = T2.material
    WHERE TheDate >GETDATE() and Thedate<T2.mindate
    GROUP BY TheDate, T1.material, T2.mindate
    -- ORDER BY T1.material, TheDate    
),
fcst3 as (
SELECT T1.material, T1.TheDate, 
CASE WHEN sum(fcstPerWDs) over (Partition by T1.material)=0 THEN T2.demandPerWDs else fcstPerWDs END as 
fcstPerWDs, 
mindate, sum(fcstPerWDs) over (Partition by T1.material) as total_fcst
FROM fcst2 T1
LEFT JOIN pppbo T2 on T1.material=T2.material
)

SELECT material, sum(fcstPerWDs) as fcstInperiod, mindate FROM fcst3
GROUP BY material, mindate

-- -- ORDER BY material, TheDate
-- )
-- SELECT *, CASE WHEN total_fcst=0 THEN 0 else fcstPerWDs END as 
--  FROM fcst3

-- -- SELECT material, sum(fcstPerWDs) as sumfcst from fcst2
-- -- GROUP BY material
-- -- ORDER BY material