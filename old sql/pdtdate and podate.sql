SELECT T1.material, --cast(po_date as date)as po_date, cast(getdate()+adj_pdt as date) as pdtdate, 
CASE WHEN po_date is not null THEN cast(po_date as date) ELSE cast(getdate()+adj_pdt as date) END mindate FROM [ivy.mm.dim.bo] T1
LEFT JOIN [ivy.mm.dim.mtrl] T2 on T1.material =T2.material
WHERE bo_bf_pdt='yes' and locn='total' and BOdays_bf_pdt>14 and bo_seq=1 and T2.ms='01' and bo_qty>0