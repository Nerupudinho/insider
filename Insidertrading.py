from datetime import date
import pandas as pd
import numpy as np
from datetime import datetime


now = datetime.now()

format = "%d-%b-%Y"
time2 = now.strftime(format)
format = "%d-%m-%Y"
time3 = now.strftime(format)
format = "%b"
time1 = now.strftime(format)
t3 = 0
format = "%Y"
time4 = now.strftime(format)
format = "%d"
time5 = now.strftime(format)


if time1 == 'Jan':
    t3 = "10"
    time4= int(time4)-1
if time1 == 'Feb':
    t3 = "11"
    time4= int(time4)-1
if time1 == 'Mar':
    t3 = "12"
    time4= int(time4)-1
if time1 == 'Apr':
    t3 = "01"
if time1 == 'May':
    t3 = "02"
if time1 == 'Jun':
    t3 = "03"
if time1 == 'Jul':
    t3 = "04"
if time1 == 'Aug':
    t3 = "05"
if time1 == 'Sep':
    t3 = "06"
if time1 == 'Oct':
    t3 = "07"
if time1 == 'Nov':
    t3 = "08"
if time1 == 'Dec':
    t3 = "09"
time6=str(time5)+"-"+str(t3)+"-"+str(time4)

file_insider = "CF-Insider-Trading-equities-"+time6+"-to-"+time3+".csv"
file_shareholder="CF-Shareholding-Pattern-equities-"+time6+"-to-"+time3+".csv"
file_pledge="CF-SAST-Pledged-Data-"+time2+".csv"
file_SAST="CF-SAST- Reg29-"+time2+".csv"

df = pd.read_csv(file_insider)
df.columns = [x.strip(' \n') for x in df.columns]
working_df=df[df["CATEGORY OF PERSON"].str.contains("Promoter")]
working_df=working_df[working_df["MODE OF ACQUISITION"].str.contains("Market Purchase")]
working_df=working_df[["SYMBOL","COMPANY","NO. OF SECURITIES (ACQUIRED/DISPLOSED)","VALUE OF SECURITY (ACQUIRED/DISPLOSED)"]]
working_df=working_df.reset_index(drop=True)
# Convert the Value of Security column to a float for summation below
working_df['VALUE OF SECURITY (ACQUIRED/DISPLOSED)'] = working_df['VALUE OF SECURITY (ACQUIRED/DISPLOSED)'].astype(
    float)
# Consolidate by the symbol, summing the values of the securities
working_df['VALUE OF SECURITY (ACQUIRED/DISPLOSED)'] = working_df.groupby('SYMBOL')[
    'VALUE OF SECURITY (ACQUIRED/DISPLOSED)'].transform('sum')
# Convert the Value of Security column to a float for summation below
working_df['NO. OF SECURITIES (ACQUIRED/DISPLOSED)'] = working_df['NO. OF SECURITIES (ACQUIRED/DISPLOSED)'].astype(
    float)
# Consolidate by the symbol, summing the values of the securities
working_df['NO. OF SECURITIES (ACQUIRED/DISPLOSED)'] = working_df.groupby('SYMBOL')[
    'NO. OF SECURITIES (ACQUIRED/DISPLOSED)'].transform('sum')
working_df.drop_duplicates(subset='SYMBOL', inplace=True, ignore_index=True)
working_df.columns=['Symbol','Company','Qty','Value']
working_df=working_df[working_df['Value']>10000000]
working_df["Target Price"]=working_df["Value"]/working_df["Qty"]

SaleList=df[df["CATEGORY OF PERSON"].str.contains("Promoter")]
SaleList_df=SaleList[SaleList["MODE OF ACQUISITION"].str.contains("Sale")]
working_df = working_df.assign(result_rebuy=working_df['Symbol'].isin(SaleList_df['SYMBOL']).astype(int))
working_df['result_rebuy']=np.where(working_df["result_rebuy"]==1,'No /','ok /')
working_df['result_rebuy'] = working_df['result_rebuy'].astype(str)

promoter_holding=pd.read_csv(file_shareholder)
promoter_holding.columns = [x.strip(' \n') for x in promoter_holding.columns]
promoter_holding=promoter_holding[['COMPANY','PROMOTER & PROMOTER GROUP (A)']]
working_df = pd.merge(left=working_df, right=promoter_holding, left_on='Company', right_on='COMPANY', how='left')
working_df.loc[working_df['PROMOTER & PROMOTER GROUP (A)'].isnull(),'result_share%'] = 'Check share% /'
working_df.loc[working_df['PROMOTER & PROMOTER GROUP (A)']>50,'result_share%']='ok /'
working_df.loc[working_df['PROMOTER & PROMOTER GROUP (A)']<=50,'result_share%']='No /'

SAST=pd.read_csv(file_SAST)
SAST.columns = [x.strip(' \n') for x in SAST.columns]
SAST=SAST[['COMPANY','TOTAL SALE (SHARES/VOTING RIGHTS/WARRANTS/ CONVERTIBLE SECURITIES/ANY OTHER INSTRUMENT)']]
SAST.columns=['COMPANY','SALE']
# Convert the Value of Security column to a float for summation below
SAST['SALE'] = SAST['SALE'].astype(float)
# Consolidate by the symbol, summing the values of the securities
SAST['SALE'] = SAST.groupby('COMPANY')['SALE'].transform('sum')
SAST.drop_duplicates(subset='COMPANY', inplace=True, ignore_index=True)
working_df = pd.merge(left=working_df, right=SAST, left_on='Company', right_on='COMPANY', how='left')
working_df.loc[working_df['SALE'].isnull(),'result_SAST'] = 'Check SAST% /'
working_df.loc[working_df['SALE']==0,'result_SAST']='ok /'
working_df.loc[working_df['SALE']>0,'result_SAST']='No /'

promoter_holding=pd.read_csv(file_pledge)
promoter_holding.columns = [x.strip(' \n') for x in promoter_holding.columns]
promoter_holding=promoter_holding[['NAME OF COMPANY','PROMOTER SHARES ENCUMBERED AS OF LAST QUARTER % OF PROMOTER SHARES (X/A)']]
working_df = pd.merge(left=working_df, right=promoter_holding, left_on='Company', right_on='NAME OF COMPANY', how='left')
working_df.loc[working_df['PROMOTER SHARES ENCUMBERED AS OF LAST QUARTER % OF PROMOTER SHARES (X/A)'].isnull(),'result_pledge%'] = 'Check Pledge% /'
working_df.loc[working_df['PROMOTER SHARES ENCUMBERED AS OF LAST QUARTER % OF PROMOTER SHARES (X/A)']==0,'result_pledge%']='ok /'
working_df.loc[working_df['PROMOTER SHARES ENCUMBERED AS OF LAST QUARTER % OF PROMOTER SHARES (X/A)']>0,'result_pledge%']='No /'

working_df['finalresult']=working_df['result_pledge%']+working_df['result_SAST']+working_df['result_share%']\
                          +working_df['result_rebuy']
working_df.drop_duplicates(subset='Symbol', inplace=True, ignore_index=True)
working_df=working_df[['Symbol','Company','Target Price','finalresult']]
working_df=working_df[~working_df["finalresult"].str.contains("No")]
print(working_df)

path= "Insider\InsiderTrading_" + str(date.today())+".csv"
working_df.to_csv(path,  index=None, sep=',')

