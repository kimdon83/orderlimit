# %%
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from pandas._libs.tslibs import NaT
from pandas.core.arrays.sparse import dtype
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from datetime import datetime
from dateutil.relativedelta import *
import time
import matplotlib.pyplot as plt
import matplotlib as mpl

# %%
timelist = []

# %%
targetPlant='total'
print(f'target plant:{targetPlant}')
todays = datetime.today()
today = todays.strftime('%Y-%m-%d')
curYM = todays.strftime('%Y%m')

# %% Connect to KIRA server
start = time.time()

import json

with open(r'C:\Users\KISS Admin\Desktop\IVYENT_DH\data.json', 'r') as f:
    data = json.load(f)

# get ID passwords from json
server = data['server']
database = data['database']
username = data['username']
password = data['password']
connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password
connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
engine = create_engine(connection_url)
print("Connection Established:")

end = time.time()
timelist.append([end-start, "Connect to KIRA server"])
# %% get the tables for this calcutation.

start = time.time()

print("start to read full table")
df_ft = pd.read_sql("""
WITH mtrlT as (
    SELECT T1.material, bo_qty/bo_days*BOdays_bf_pdt as bo_qty 
    ,case WHEN po_date is not null then po_date else cast(GETDATE()+ adj_pdt+ 10 as date)  END as adj_po_date
    FROM [ivy.mm.dim.bo] T1
    LEFT JOIN [ivy.mm.dim.mtrl] T2 on T1.material =T2.material
    LEFT join [ivy.mm.dim.mrp01] T3 on T3.material= T1.material
    WHERE bo_bf_pdt='yes' and locn='total' and BOdays_bf_pdt>14 and bo_seq=1 and T2.ms='01' and bo_qty>0 and T3.total_stock>0 
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

""", con=engine)
print("full table is ready")
df_ft.head()
start = time.time()

print("start to read full table")
df_bo_qty = pd.read_sql("""
    SELECT T1.material, bo_qty/bo_days*BOdays_bf_pdt as bo_qty 
    ,case WHEN po_date is not null then po_date else cast(GETDATE()+ adj_pdt+ 10 as date)  END as adj_po_date
    FROM [ivy.mm.dim.bo] T1
    LEFT JOIN [ivy.mm.dim.mtrl] T2 on T1.material =T2.material
    LEFT join [ivy.mm.dim.mrp01] T3 on T3.material= T1.material
    WHERE bo_bf_pdt='yes' and locn='total' and BOdays_bf_pdt>14 and bo_seq=1 and T2.ms='01' and bo_qty>0 and T3.total_stock>0 
""", con=engine)
print("df_bo_qty is ready")
df_bo_qty.head()

df_demand = pd.read_sql("""
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
    ), 
mindateT as(
    SELECT T1.material, --cast(po_date as date)as po_date, cast(getdate()+adj_pdt as date) as pdtdate, 
    CASE WHEN po_date is not null THEN cast(po_date as date) ELSE cast(getdate()+adj_pdt+10 as date) END mindate FROM [ivy.mm.dim.bo] T1
    LEFT JOIN [ivy.mm.dim.mtrl] T2 on T1.material =T2.material
    WHERE bo_bf_pdt='yes' and locn='total' and BOdays_bf_pdt>14 and bo_seq=1 and T2.ms='01' and bo_qty>0
),T4fcst
    -- Table to make fcst table. FROM this month to upcoming 5 month
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
, mtrlT as (
    SELECT T0.material
    FROM (
        SELECT T1.material, bo_qty/bo_days*BOdays_bf_pdt as bo_qty 
        ,case WHEN po_date is not null then po_date else cast(GETDATE()+ adj_pdt+ 10 as date)  END as adj_po_date
        FROM [ivy.mm.dim.bo] T1
        LEFT JOIN [ivy.mm.dim.mtrl] T2 on T1.material =T2.material
        LEFT join [ivy.mm.dim.mrp01] T3 on T3.material= T1.material
        WHERE bo_bf_pdt='yes' and locn='total' and BOdays_bf_pdt>14 and bo_seq=1 and T2.ms='01' and bo_qty>0 and T3.total_stock>0 
        ) T0
    LEFT JOIN [ivy.sd.fact.order] T1 on T0.material=T1.material 
    LEFT JOIN [ivy.mm.dim.mtrl] T2 on T1.material=T2.material
    where act_date BETWEEN dateadd(D,1,eomonth(GETDATE(),-7)) and dateadd(D,1,eomonth(GETDATE(),-1)) and T2.ip is not null
    and T2.ip>0
    and T1.qty>0
    GROUP BY T0.material
)

SELECT T0.material,sum(fcstPerWDs) as demandInperiod, mindate as adj_podate FROM mtrlT T0
LEFT JOIN fcst3 T1 on T0.material=T1.material
GROUP BY T0.material, mindate
""",con=engine)
df_demand.head()

# %% df_ft1 : select columns
df_ft1=df_ft.loc[:,["material","unit_qty","order#","total_qty","ip","prop","cumsum_prop"]].copy()
df_ft1.head()

# %% df_ft2 : simulate with orderlimit_qty range(ip,ip*10,ip)
df_ip = df_ft[['material', 'ip']].drop_duplicates().sort_values('material')
df_ip['ip'] = df_ip['ip'].astype(int)
df_ft2 = pd.DataFrame()

for material, ip in df_ip.groupby('material')['ip']:
    ip_in=ip.values[0]
    for orderlimit_qty in range(ip_in,ip_in*10,ip_in):
        print(material, orderlimit_qty)
        df_ft_temp = df_ft.loc[df_ft['material'] == material].copy()
        df_ft_temp['orderlimit'] = orderlimit_qty
        df_ft_temp['unit_qty2']      = df_ft_temp.apply(lambda row: min(row['unit_qty'], orderlimit_qty), axis=1)
        df_ft_temp['order#2']   = df_ft_temp.apply(lambda row: max(row['order#'],row['order#']*np.log10(10*row['unit_qty']/row['orderlimit'])), axis=1)
        df_ft_temp['total_qty2']= df_ft_temp['unit_qty2']*df_ft_temp['order#2']
        df_ft_temp['Var_qty']= -df_ft_temp['total_qty2']+df_ft_temp['total_qty']
        df_ft2 = pd.concat([df_ft2, df_ft_temp], ignore_index=True)

print("generating output is done")

# %%

df_ft2.to_csv('orderlimit_case.csv',index=False)

var_qty_by_ordlimit= df_ft2.groupby(["material","orderlimit"]).agg({'Var_qty':'sum'})
var_qty_by_ordlimit=var_qty_by_ordlimit.reset_index()
var_qty_by_ordlimit.head()
# %%
var_qty_by_ordlimit=var_qty_by_ordlimit.merge(df_bo_qty,how='left',on='material')
var_qty_by_ordlimit['diff']=np.abs(var_qty_by_ordlimit['Var_qty']-var_qty_by_ordlimit['bo_qty'])
var_qty_by_ordlimit.to_csv('var_qty_by_ordlimit.csv',index=False)
# Create a DataFrame to store the results
result_df = pd.DataFrame(columns=['material', 'orderlimit'])
min_orderlimits = var_qty_by_ordlimit.groupby('material')['diff'].idxmin().apply(lambda idx: var_qty_by_ordlimit.at[idx, 'orderlimit'])
min_orderlimits.to_csv('min_orderlimits.csv',index=False)

var_qty_by_ordlimit.groupby('material')['diff'].idxmin().apply(lambda idx: var_qty_by_ordlimit.loc[idx])

print(var_qty_by_ordlimit)
