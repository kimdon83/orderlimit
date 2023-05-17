WITH mtrlT as (
    SELECT T1.material, bo_qty/bo_days*BOdays_bf_pdt as bo_qty 
    ,case WHEN po_date is not null then po_date else cast(GETDATE()+ adj_pdt+ 10 as date)  END as adj_po_date
    FROM [ivy.mm.dim.bo] T1
    LEFT JOIN [ivy.mm.dim.mtrl] T2 on T1.material =T2.material
    LEFT join [ivy.mm.dim.mrp01] T3 on T3.material= T1.material
    WHERE bo_bf_pdt='yes' and locn='total' and BOdays_bf_pdt>14 and bo_seq=1 and T2.ms='01' and bo_qty>0 and T3.total_stock>0 
),  
-- WITH 
billTBL as (
    SELECT material, qty as unit_qty, SUM(qty) as bill_qty , SUM(qty)/qty as bill# FROM [ivy.sd.fact.bill]
    WHERE act_date BETWEEN dateadd(D,1,eomonth(GETDATE(),-7)) and dateadd(D,1,eomonth(GETDATE(),-1)) and qty>0
    GROUP BY material , qty
),  boTBL as (
    SELECT material, bo_qty as unit_qty, SUM(bo_qty) as bo_qty , SUM(bo_qty)/bo_qty as bo# FROM [ivy.sd.fact.bo]
    WHERE act_date BETWEEN dateadd(D,1,eomonth(GETDATE(),-7)) and dateadd(D,1,eomonth(GETDATE(),-1)) and bo_qty>0
    GROUP BY material , bo_qty
)
, billboTBL as (
    SELECT T1.material, T1.unit_qty, T1.bill_qty, T1.bill#, coalesce(T2.bo_qty,0) as bo_qty , coalesce(T2.bo#,0) as bo#, 
    cast(T1.bill_qty as float)+ cast(coalesce(T2.bo_qty,0) as float) *0.5 as total_qty,
    cast(T1.bill# as float)+ cast(coalesce(T2.bo#,0) as float) *0.5 as total#
    FROM billTBL T1
    LEFT JOIN boTBL T2 on T1.material= T2.material and T1.unit_qty= T2.unit_qty
)    
, total1 as (
    SELECT T0.material, T1.unit_qty, T1.total_qty, T1.total#, T1.bo_qty, T1.bo#, T2.ip
    FROM mtrlT T0
    LEFT JOIN billboTBL T1 on T0.material=T1.material 
    LEFT JOIN [ivy.mm.dim.mtrl] T2 on T1.material=T2.material
    where T2.ip is not null and T2.ip>0 
), total2 as (
    SELECT *,
    sum(total_qty) over (Partition by material) as mtrl_total_qty,
    sum(total_qty) over (PARTITION BY material ORDER BY unit_qty rows BETWEEN unbounded PRECEDING and CURRENT ROW) as cumsumqty
    FROM total1
),
total3 as (
SELECT material, unit_qty, total_qty, total#, bo_qty, bo#, ip, mtrl_total_qty, cumsumqty,
cast(total_qty as float)/ cast(mtrl_total_qty as float) as prop,
cast(cumsumqty as float) /cast(mtrl_total_qty as float) as cumsum_prop
FROM total2
-- ORDER BY material, unit_qty
)
SELECT * FROM total3
where unit_qty < ip