#----------------------------------------------------------------------------------
# Name:        ParseWTMDATA.py
# Purpose:     To derive the 1300 hour maximum solar radiation value for each month
# Author:      pyang
#
# Created:     05/23/2017
#-------------------------------------------------------------------------------

import csv
import pandas
import math
import glob
import datetime
#import numpy as np

#A function for writing a dict to a file
def saveDict(fn,dict_rap):
    f=open(fn, "wb")
    w = csv.writer(f)
    for key, val in dict_rap.items():
        w.writerow([key, val])
    f.close()

def dateFun(series):
##    day = int(series['Day_Julian'])
##    year = int(series['Year'])
##    print day,year
##    date = datetime.datetime(year, 1, 1) + datetime.timedelta(day)
##    return date.strftime('%Y%m%d')
    return (datetime.datetime(int(series['Year']), 1, 1) + datetime.timedelta(int(series['Day_Julian']))).strftime('%Y%m%d')

def zuluTime(series):
    from_zone = tz.tzlocal()
    to_zone = tz.tzutc() #changed to local zone
    #to_zone = tz.gettz("US/Central")
    TIMESTR = str(series['DATE']) + str(series['Local_Time'])
    print TIMESTR
    local = datetime.datetime.strptime(TIMESTR,"%Y%m%d%H%M")
    local=local.replace(tzinfo=from_zone)
    utc = local.astimezone(to_zone)
    return utc

##dict_solar = {}
##wtmdataflist = glob.glob('C:\\DEV\\MesoNet\\TFS_WTMDATA\\*.txt')
##for wtmdata in wtmdataflist:
##    fn = wtmdata[:18] + wtmdata[-12:-8] + '.txt'
##
###wtmdata = r'C:\\DEV\\MesoNet\\TFS_WTMDATA\\JAYT1210.txt'
##    with open(wtmdata) as txtfile, open(fn,'a') as solartxtfile :
##        pread = pandas.read_csv(txtfile,names=range(22))#,skiprows=6,names=range(37))
##        #pread = pandas.read_csv(txtfile,error_bad_lines=False)
##
##        df = pread.iloc[:,(0,1,2,18)]
##        columnNamelist = ['Array_ID', 'Day_Julian ','Local_Time','Solar_Radiation' ]
##        df.columns = columnNamelist
##        #remove the Array_ID ==2
##        df = df[df['Array_ID']==1].iloc[:,(1,2,3)]
##        #getting the time around 1300 hour no matter day saving or not
##        df = df[(df['Local_Time']>1100) & (df['Local_Time']<1500)]
##        #the maximum value should be based on hourly
##        df.loc[:,'Date_Hour'] = df.loc[:,('Local_Time')].astype(str).str[0:2]
##        #print df.loc[:,'Date_Hour']
##        df.loc[:,'HourlySolarRadiation'] = df.loc[:,'Solar_Radiation'].groupby(df['Date_Hour']).transform('mean')
##        print df.loc[:,'HourlySolarRadiation']
##        max_solar = df['HourlySolarRadiation'].max()
##        print wtmdata[-12:-8],'20'+ wtmdata[-8:-4],int(max_solar)
##        solarstr = wtmdata[-12:-8] + ',' + '20' + wtmdata[-8:-4] + ',' + str(int(max_solar))
##        dict_solar[wtmdata[-12:-4]]= int(max_solar)
##        solartxtfile.write( solarstr +'\n' )
#fn = r'C:\\DEV\\MesoNet\\solar_ratidation_monthly.csv'
#saveDict(fn,dict_solar)

dict_solar = {}
wtmdataflist = glob.glob('C:\\DEV\\MesoNet\\TFS_WTMDATA\\CANA*.txt')
##for wtmdata in wtmdataflist:
##    fn = wtmdata[:18] + wtmdata[-12:-8] + '.txt'
##    print wtmdata

wtmdata = r'C:\\DEV\\MesoNet\\TFS_WTMDATA\\CANA1605.txt'
with open(wtmdata) as txtfile:#, open(fn,'a') as solartxtfile :
    pread = pandas.read_csv(txtfile,names=range(22))#,skiprows=6,names=range(37))
    #pread = pandas.read_csv(txtfile,error_bad_lines=False)
    df = pread.iloc[:,(0,1,2,18)]
    columnNamelist = ['Array_ID', 'Day_Julian','Local_Time','Solar_Radiation' ]
    df.columns = columnNamelist
    #remove the Array_ID ==2
    df = df[df['Array_ID']==1].iloc[:,(1,2,3)]
    #print df
    #getting the time around 1300 hour no matter day saving or not
    #df = df[(df['Local_Time']>1200) & (df['Local_Time']<1400)]
    #df = df[(df['Local_Time']==1300)]
    #print len(df)
    #the maximum value should be based on hourly
    df.loc[:,'Year'] = '20' + wtmdata[-8:-6]
    df.loc[:,'Date_Hour'] = df.loc[:,('Local_Time')].astype(str).str[0:2]
    #print 'groups number: ',len(df.loc[:,'Solar_Radiation'].groupby(df['Date_Hour']))

    #print df.to_csv(r'C:\DEV\MesoNet\OriginalGailTTU.txt',sep=' ', index=False, header=False , mode='a')
    df.loc[:,'HourlySolarRadiation'] = df.loc[:,'Solar_Radiation'].groupby(df['Date_Hour']).transform('mean')
    #df.loc[:,'DATE'] = df.apply(dateFun,axis = 1)
    #df.loc[:,'ZULUTIME'] = df.apply(zuluTime,axis = 1)
    #df.loc[:,'ZULUTIME'] = df[(df['ZULUTIME'].dt.hour==19)]
    #print df.columns
    #print df.to_csv(r'C:\DEV\MesoNet\SolarRadiationTTU19Hour.txt',sep=',', index=False, header=False, mode='a')
    #print df
    print df.loc[:,'HourlySolarRadiation']
    max_solar_hour = df['HourlySolarRadiation'].max()
    df.loc[:,'max_solar_hour'] = max_solar_hour
    max_solar = df['Solar_Radiation'].max()
    min_solar_hour = df['HourlySolarRadiation'].min()
    min_solar = df['Solar_Radiation'].min()
    yearmonth = wtmdata[-12:-8],'20'+ wtmdata[-8:-4]
    dict_solar[wtmdata[-12:-4]]= int(max_solar_hour)
    print wtmdata[-12:-8],'20'+ wtmdata[-8:-4],'HourlyMeanMax:',int(max_solar_hour),'HourlyMax:',int(max_solar),'HourlyMeanMin:',int(min_solar_hour),'HourlyMin:',int(min_solar)
'''
#Create a data frame from  the dict_solar for monthly maximum for several years
df_solar = pandas.DataFrame(dict_solar.items(), columns=['Date', 'SolarRadiation'])
df_solar.loc[:,'Station']= df_solar.loc[:,'Date'].str[0:4]
df_solar.loc[:,'MONTH']=df_solar.loc[:,'Date'].astype(str).str[6:8]
#df_solar.loc[:,'MaxMeanSolar'] = df_solar.loc[:,'SolarRadiation'].groupby(df_solar['MONTH'],df_solar['Station']).transform('max')
#May need to aggregate the result from the groupped
for name,df_station in df_solar.groupby(df_solar['Station']):
    #df_station.loc[:,'MaxMeanSolar'] = df_station.loc[:,'SolarRadiation'].groupby(df_station['MONTH']).transform('max') #
    #Calculate the maximun value for the same month for differnt years and aggregate (downsize) to a new dataframe
    #First group by key 'MONTH' and use as_index=False to remove the key from groupping [otherwise will get a undesirable index]
    df_station = df_station.groupby(df_station['MONTH'],as_index=False)['SolarRadiation'].agg({'MaxMeanSolar' : 'max'})
    df_station.loc[:,'Station']= name
    #print df_station.loc[:,('Station','MONTH','MaxMeanSolar')]
    #Save the dataframe into a text file
    #print df_station.to_csv(r'C:\DEV\MesoNet\MonthlySolarRadiation.txt',sep=' ', index=False, header=False, mode='a')
'''
