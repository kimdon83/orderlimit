WITH mtrlT as (
    SELECT T1.material FROM [ivy.mm.dim.bo] T1
    LEFT JOIN [ivy.mm.dim.mtrl] T2 on T1.material =T2.material
    WHERE bo_bf_pdt='yes' and locn='total' and BOdays_bf_pdt>14 and bo_seq=1 and T2.ms='01' and bo_qty>0
)
, total1 as (
    SELECT T0.material, T1.qty, SUM(T1.qty) as total_qty, SUM(T1.qty)/T1.qty as order#, T2.ip
    FROM mtrlT T0
    LEFT JOIN [ivy.sd.fact.order] T1 on T0.material=T1.material 
    LEFT JOIN [ivy.mm.dim.mtrl] T2 on T1.material=T2.material
    where act_date BETWEEN dateadd(D,1,eomonth(GETDATE(),-7)) and dateadd(D,1,eomonth(GETDATE(),-1)) and T2.ip is not null
    and T2.ip>0
    and T1.qty>0
    GROUP BY T0.material, T1.qty, T2.ip
), total2 as (
    SELECT *,
    sum(total_qty) over (Partition by material) as mtrl_total_qty,
    sum(total_qty) over (PARTITION BY material ORDER BY qty rows BETWEEN unbounded PRECEDING and CURRENT ROW) as cumsumqty
    FROM total1
)
SELECT material, qty as unit_qty, total_qty, order#, ip, mtrl_total_qty, cumsumqty,
total_qty/mtrl_total_qty as prop,
cumsumqty/mtrl_total_qty as cumsum_prop
FROM total2
ORDER BY material, qty