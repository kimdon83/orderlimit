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
    ,case WHEN po_date is not null then po_date else cast(GETDATE()+ adj_pdt+ 10 as date)  END as adj_po_date, T2.ip
    FROM [ivy.mm.dim.bo] T1
    LEFT JOIN [ivy.mm.dim.mtrl] T2 on T1.material =T2.material
    LEFT join (
        SELECT material, SUM(total_stock) as total_stock FROM [ivy.mm.dim.mrp01] GROUP BY material
    ) T3 on T3.material= T1.material
    WHERE bo_bf_pdt='yes' and locn='total' and BOdays_bf_pdt>14 and bo_seq=1 and T2.ms='01' and bo_qty>0 and T3.total_stock>0 
),  
-- WITH 
billTBL as (
    SELECT material, qty as unit_qty, SUM(qty) as bill_qty , SUM(qty)/qty as bill# FROM [ivy.sd.fact.bill] T1
    LEFT JOIN [ivy.mm.dim.shiptoparty] T2 on T1.shiptoparty=T2.shiptoparty
    WHERE act_date BETWEEN dateadd(D,1,eomonth(GETDATE(),-7)) and dateadd(D,1,eomonth(GETDATE(),-1)) and qty>0
    and T2.shiptoparty not in ('0011008549', '0011002886', '0011011500', '0011011419', '0011011147')
    GROUP BY material , qty
),  boTBL as (
    SELECT material, bo_qty as unit_qty, SUM(bo_qty) as bo_qty , SUM(bo_qty)/bo_qty as bo# FROM [ivy.sd.fact.bo] T1
    LEFT JOIN [ivy.mm.dim.shiptoparty] T2 on T1.shiptoparty=T2.shiptoparty
    WHERE act_date BETWEEN dateadd(D,1,eomonth(GETDATE(),-7)) and dateadd(D,1,eomonth(GETDATE(),-1)) and bo_qty>0
    and T2.shiptoparty not in ('0011008549', '0011002886', '0011011500', '0011011419', '0011011147')
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
    SELECT T0.material, T1.unit_qty, T1.total_qty, T1.total#, T1.bo_qty, T1.bo#, T0.ip
    FROM mtrlT T0
    LEFT JOIN billboTBL T1 on T0.material=T1.material 
    where T0.ip is not null and T0.ip>0 and T1.unit_qty is not null
), total2 as (
    SELECT *,
    sum(total_qty) over (Partition by material) as mtrl_total_qty,
    sum(total_qty) over (PARTITION BY material ORDER BY unit_qty rows BETWEEN unbounded PRECEDING and CURRENT ROW) as cumsumqty
    FROM total1
)
SELECT material, unit_qty, total_qty, total#, bo_qty, bo#, ip, mtrl_total_qty, cumsumqty,
cast(total_qty as float)/ cast(mtrl_total_qty as float) as prop,
cast(cumsumqty as float) /cast(mtrl_total_qty as float) as cumsum_prop
FROM total2
ORDER BY material,unit_qty
""", con=engine)
print("full table is ready")
df_ft.head()
start = time.time()

print("start to read full table")
df_bo_qty = pd.read_sql("""
    SELECT T1.material, bo_qty/bo_days*BOdays_bf_pdt as bo_qty, BOdays_bf_pdt as BOdays 
    ,case WHEN po_date is not null then po_date else cast(GETDATE()+ adj_pdt+ 10 as date)  END as adj_po_date, T2.ip
    FROM [ivy.mm.dim.bo] T1
    LEFT JOIN [ivy.mm.dim.mtrl] T2 on T1.material =T2.material
    LEFT join (
        SELECT material, SUM(total_stock) as total_stock FROM [ivy.mm.dim.mrp01] GROUP BY material
    ) T3 on T3.material= T1.material
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
    FROM [ivy.sd.fact.bill_ppp] T1
    LEFT JOIN [ivy.mm.dim.shiptoparty] T2 on T1.shiptoparty=T2.shiptoparty
    WHERE act_date BETWEEN DATEADD(MM, - 3, DATEADD(DD, - 1, GETDATE())) AND DATEADD(DD, - 1, GETDATE()) AND ordsqc > 1
    and T2.shiptoparty not in ('0011008549', '0011002886', '0011011500', '0011011419', '0011011147')
    GROUP BY material, plant
    ), backOrder
    -- avgMbo within 3month, material, plant FROM [ivy.sd.fact.bo] 
AS (
    SELECT SUM(bo_qty) AS bo3M, material, plant
    FROM [ivy.sd.fact.bo] T1
    LEFT JOIN [ivy.mm.dim.shiptoparty] T2 on T1.shiptoparty=T2.shiptoparty
    WHERE (act_date BETWEEN DATEADD(MM, - 3, DATEADD(DD, - 1, GETDATE())) AND DATEADD(DD, - 1, GETDATE()))
    and T2.shiptoparty not in ('0011008549', '0011002886', '0011011500', '0011011419', '0011011147')
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

df_mtrl = pd.read_sql("""
WITH mtrlT as (
    SELECT T1.material, bo_qty/bo_days*BOdays_bf_pdt as bo_qty 
    ,case WHEN po_date is not null then po_date else cast(GETDATE()+ adj_pdt+ 10 as date)  END as adj_po_date, T2.ip
    FROM [ivy.mm.dim.bo] T1
    LEFT JOIN [ivy.mm.dim.mtrl] T2 on T1.material =T2.material
    LEFT join (
        SELECT material, SUM(total_stock) as total_stock FROM [ivy.mm.dim.mrp01] GROUP BY material
    ) T3 on T3.material= T1.material
    WHERE bo_bf_pdt='yes' and locn='total' and BOdays_bf_pdt>14 and bo_seq=1 and T2.ms='01' and bo_qty>0 and T3.total_stock>0 
),  
-- WITH 
billTBL as (
    SELECT material, qty as unit_qty, SUM(qty) as bill_qty , SUM(qty)/qty as bill# FROM [ivy.sd.fact.bill] T1
    LEFT JOIN [ivy.mm.dim.shiptoparty] T2 on T1.shiptoparty=T2.shiptoparty
    WHERE act_date BETWEEN dateadd(D,1,eomonth(GETDATE(),-7)) and dateadd(D,1,eomonth(GETDATE(),-1)) and qty>0
    and T2.shiptoparty not in ('0011008549', '0011002886', '0011011500', '0011011419', '0011011147')
    GROUP BY material , qty
),  boTBL as (
    SELECT material, bo_qty as unit_qty, SUM(bo_qty) as bo_qty , SUM(bo_qty)/bo_qty as bo# FROM [ivy.sd.fact.bo] T1
    LEFT JOIN [ivy.mm.dim.shiptoparty] T2 on T1.shiptoparty=T2.shiptoparty
    WHERE act_date BETWEEN dateadd(D,1,eomonth(GETDATE(),-7)) and dateadd(D,1,eomonth(GETDATE(),-1)) and bo_qty>0
    and T2.shiptoparty not in ('0011008549', '0011002886', '0011011500', '0011011419', '0011011147')
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
    SELECT T0.material, T1.unit_qty, T1.total_qty, T1.total#, T1.bo_qty, T1.bo#, T0.ip
    FROM mtrlT T0
    LEFT JOIN billboTBL T1 on T0.material=T1.material 
    where T0.ip is not null and T0.ip>0 and T1.unit_qty is not null
), total2 as (
    SELECT material
    FROM total1
    GROUP BY material
)
SELECT T2.*
FROM total2
LEFT JOIN [ivy.mm.dim.mtrl] T2 on total2.material= T2.material
ORDER BY T2.material
""",con=engine)
df_mtrl.head()

# %% 
# df_ft1=df_ft.loc[:,["material","unit_qty","total#","total_qty","ip","prop","cumsum_prop"]].copy()
# df_ft1.head()

df_ft.to_csv("1.order_pattern.csv",index=False)

# # Create a boolean mask using isin()
# mask = df_mtrl["material"].isin(df_ft["material"])

# # Apply the mask to filter df_material
# filtered_df_material = df_mtrl[mask].drop_duplicates(subset=["material"]).sort_values("material")

df_mtrl.to_csv("0. dim_mtrl_partial.csv",index=False)
# filtered_df_material.to_csv("0. dim_mtrl_partial.csv",index=False)

# %% df_ft2 : simulate with orderlimit_qty range(ip,ip*10,ip)
df_ip = df_ft[['material', 'ip']].drop_duplicates().sort_values('material')
df_ip['ip'] = df_ip['ip'].astype(int)
df_ft2 = pd.DataFrame()

for material, ip in df_ip.groupby('material')['ip']:
    ip_in=ip.values[0]
    df_ft_temp = df_ft.loc[df_ft['material'] == material].copy()
    
    # Create the range list and extend it with the values from the DataFrame column
    loop_list = list(range(ip_in, ip_in * 10, ip_in))
    loop_list.extend(df_ft_temp['unit_qty'].tolist())

    # Remove duplicates by converting to a set and then back to a list
    loop_list = list(set(loop_list))
    loop_list.sort()

    for orderlimit_qty in loop_list:
        print(material, orderlimit_qty)
        # df_ft_temp = df_ft.loc[df_ft['material'] == material].copy()
        df_ft_temp['orderlimit'] = orderlimit_qty
        df_ft_temp['unit_qty2']      = df_ft_temp.apply(lambda row: min(row['unit_qty'], orderlimit_qty), axis=1)
        df_ft_temp['total#2']   = df_ft_temp.apply(lambda row: max(row['total#'],row['total#']*np.log10(10*row['unit_qty']/row['orderlimit'])), axis=1)
        df_ft_temp['total_qty2']= df_ft_temp['unit_qty2']*df_ft_temp['total#2']
        df_ft_temp['Var_qty']= -df_ft_temp['total_qty2']+df_ft_temp['total_qty']
        df_ft2 = pd.concat([df_ft2, df_ft_temp], ignore_index=True)

print("generating output is done")
df_ft2=df_ft2.merge(df_demand.loc[:,["material","demandInperiod"]],how="left",on="material")
df_ft2["Var_qty_with_demand"]=df_ft2["Var_qty"]/  df_ft2["mtrl_total_qty"]*df_ft2["demandInperiod"]

# %%

df_ft2.to_csv('orderlimit_case.csv',index=False)

var_qty_by_ordlimit= df_ft2.groupby(["material","orderlimit"]).agg({'Var_qty_with_demand':'sum'})
var_qty_by_ordlimit=var_qty_by_ordlimit.reset_index()
var_qty_by_ordlimit.head()
# %%
var_qty_by_ordlimit=var_qty_by_ordlimit.merge(df_bo_qty,how='left',on='material')
var_qty_by_ordlimit['diff']=np.abs(var_qty_by_ordlimit['Var_qty_with_demand']-var_qty_by_ordlimit['bo_qty'])
var_qty_by_ordlimit["BOdays2"]=var_qty_by_ordlimit.apply(lambda row:-row["diff"]/ row["bo_qty"]*row["BOdays"] \
    if row["Var_qty_with_demand"]>row["bo_qty"] else row["diff"]/ row["bo_qty"]*row["BOdays"],axis=1)

# var_qty_by_ordlimit.to_csv('2.var_qty_by_ordlimit.csv',index=False)
# Create a DataFrame to store the results
result_df = pd.DataFrame(columns=['material', 'orderlimit'])
min_orderlimits = var_qty_by_ordlimit.groupby('material')['diff'].idxmin().apply(lambda idx: var_qty_by_ordlimit.loc[idx, ["material","orderlimit"]])
min_orderlimits = min_orderlimits.rename(columns={'orderlimit': 'recommendation_qty'})
min_orderlimits=min_orderlimits.drop("material",axis=1).reset_index()
# min_orderlimits.to_csv('3.min_orderlimits.csv',index=False)

var_qty_by_ordlimit=var_qty_by_ordlimit.merge(min_orderlimits,how='left',on='material')
var_qty_by_ordlimit.to_csv('2.var_qty_by_ordlimit.csv',index=False)

result_gb_ordlimit=var_qty_by_ordlimit.groupby('material')['diff'].idxmin().apply(lambda idx: var_qty_by_ordlimit.loc[idx])
result_gb_ordlimit=result_gb_ordlimit.drop("material",axis=1).reset_index()
# result_gb_ordlimit["BOdays2"]=result_gb_ordlimit.apply(lambda row:-row["diff"]/ row["bo_qty"]*row["BOdays"] \
#     if row["Var_qty_with_demand"]>row["bo_qty"] else row["diff"]/ row["bo_qty"]*row["BOdays"],axis=1)

result_gb_ordlimit.to_csv('4.result group by mtrl, orderlimit_qty.csv',index=False)

# %%
print(df_ft.head())
# %%
print(df_demand.head())
# %%
print(df_bo_qty.head())
# %%
print(df_ft2.head())
print(var_qty_by_ordlimit.head())
# %%
print(result_gb_ordlimit)
# %%

print("done")

# # %% write talble 1,2 in one excel file with openpyxl
# import openpyxl
# from openpyxl.utils.dataframe import dataframe_to_rows

# # Assuming df_ft and var_qty_by_ordlimit are your DataFrames

# # Create a new Excel workbook
# wb = openpyxl.Workbook()

# # Add the first sheet and write the first DataFrame (df_ft)
# ws1 = wb.active
# ws1.title = "orderPattern"
# for r in dataframe_to_rows(df_ft, index=False, header=True):
#     ws1.append(r)

# # Add the second sheet and write the second DataFrame (var_qty_by_ordlimit)
# ws2 = wb.create_sheet("Var qty per ord limit")
# for r in dataframe_to_rows(var_qty_by_ordlimit, index=False, header=True):
#     ws2.append(r)

# df_mtrl=pd.DataFrame( df_ft["material"].unique())
# ws3 = wb.create_sheet("mtrl")
# for r in dataframe_to_rows(df_mtrl, index=False, header=True):
#     ws3.append(r)



# # Save the Excel file
# wb.save("output.xlsx")




# # # Create a new Excel workbook and add a sheet
# # wb = openpyxl.Workbook()
# # ws = wb.active
# # ws.title = "Sheet1"

# # # Write the first DataFrame (df_ft) to the sheet
# # for r_idx, row in enumerate(dataframe_to_rows(df_ft, index=False, header=True)):
# #     for c_idx, value in enumerate(row):
# #         ws.cell(row=r_idx+1, column=c_idx+1, value=value)

# # # Find the number of columns in df_ft
# # num_columns_df_ft = len(df_ft.columns)

# # # Write the second DataFrame (var_qty_by_ordlimit) to the sheet, column-wise
# # for r_idx, row in enumerate(dataframe_to_rows(var_qty_by_ordlimit, index=False, header=True)):
# #     for c_idx, value in enumerate(row):
# #         ws.cell(row=r_idx+1, column=c_idx+1+num_columns_df_ft, value=value)

# # Save the Excel file
# # wb.save("output.xlsx")

# # conditional formatting : availablity
# # green_format = PatternFill(fgColor = '00CCFFCC', fill_type='solid')
# # red_format = PatternFill(fgColor = '00FF8080', fill_type='solid')
# # blue_format = PatternFill(fgColor = '0000FF80', fill_type='solid')
# # for k in range(1,max_row+1):
# #     result_value = str(ws.cell(row=k, column=4).value)
# #     if result_value == "NO":
# #         ws.cell(row=k, column=4).fill = red_format
# #         ws.cell(row=k, column=4).font = Font(color = '00800000')
# #     elif result_value == "OK":
# #         ws.cell(row=k, column=4).fill = green_format
# #         ws.cell(row=k, column=4).font = Font(color = '00008000')
# #     elif result_value == "YES":
# #         ws.cell(row=k, column=4).fill = blue_format
# #         ws.cell(row=k, column=4).font = Font(color = '00008000')

# # %%
# import plotly.graph_objects as go
# # Create a Plotly table
# fig = go.Figure(data=[go.Table(
#     header=dict(values=list(var_qty_by_ordlimit.columns),
#                 fill_color='paleturquoise',
#                 align='left'),
#     cells=dict(values=[var_qty_by_ordlimit[col] for col in var_qty_by_ordlimit.columns],
#                fill_color='lavender',
#                align='left'))
# ])

# # Save the table as an HTML file
# fig.write_html('table.html', full_html=True)


# %%
print("done writing")