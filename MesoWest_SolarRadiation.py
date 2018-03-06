#----------------------------------------------------------------------------------
# Name:        MesoWest_SolarRadiation.py
# Purpose:     To derive the 1300 hour maximum solar radiation value for each month
# Author:      pyang
# Created:     05/23/2017
# Updated:     08/11/2017 Mike mentioned a different way to calculate solar radiation table
#              1) Calculate the break point for 85%, 75% and 50% (should be reversed to 15%,25% and 50%)
#              2) Use the exactly observation time at 19 hour instead of hourly mean to get current percentage
#-------------------------------------------------------------------------------

import csv
import pandas
import math
import glob
import numpy as np

#A function for writing a dict to a file
def saveDict(fn,dict_rap):
    f=open(fn, "wb")
    w = csv.writer(f)
    for key, val in dict_rap.items():
        w.writerow([key, val])
    f.close()


#########################################################################
##Downlaod the solar radiation for 10 years
MyToken = '994a7e628db34fc68503d44c447aaa6f'
base_url = 'http://api.mesowest.net/v2/stations/'
query_type = 'timeseries'
csv_format = '&output=csv'
Variable = 'solar_radiation'
units = '&units=precip|in,temp|F,speed|mph'
#######################################################################
##Loop through each station for downloading and processing Mesonet Data
#http://api.mesowest.net/v2/stations/timeseries?state=dc&start=201307010000&end=201307020000&vars=air_temp,pressure&obtimezone=local&token=demotoken
##STATION='GGST2'
##StartTime,EndTime = ('200705251900','201705251900')
###print STATION,ID
##api_string = base_url + query_type + '?' + 'stid=' + STATION + '&start=' + StartTime + '&end=' + EndTime + '&vars=' + Variable + '&token=' + MyToken + units + csv_format
##print api_string
##print 'Downloading ASOS data for Station: ' + STATION
###filename = "%s-%s-MesoWest.csv"%(STATION,today.strftime("%Y%m%d%H%M"))
##filename = "%s-%s-%s.csv"%(STATION,StartTime,EndTime)
##print filename
##csvfile = os.path.join(MesonetArchive, filename)
##print csvfile
##urllib.urlretrieve(api_string,csvfile)

##dict_solar = {}
##wtmdataflist = glob.glob('C:\\DEV\\MesoNet\\TFS_WTMDATA\\*.txt')
##for wtmdata in wtmdataflist:
##    fn = wtmdata[:18] + wtmdata[-12:-8] + '.txt'
##
###wtmdata = r'C:\\DEV\\MesoNet\\TFS_WTMDATA\\JAYT1210.txt'

#fn = r'C:\\DEV\\MesoNet\\solar_ratidation_monthly.csv'
#saveDict(fn,dict_solar)

#Define a funtion to passing two parameters into a single parameter function
def percentile(n):
    def percentile_(x):
        return np.nanpercentile(x, n)
    percentile_.__name__ = 'percentile_%s' % n
    return percentile_


csvfilename1 = r'C:\DEV\MesoNet\CSV\JTST2-200705161900-201705301900_10year.csv'
csvfilename2 = r'C:\DEV\MesoNet\CSV\CATT2-201108242131-201705301900_6year.csv'
csvfilename3 = r'C:\DEV\MesoNet\CSV\CNST2-201606101900-201705301900_1year.csv'
csvfilename4 = r'C:\DEV\MesoNet\CSV\GGST2-200701011900-201705301900_10year.csv'

dict_solar = {}

Stations = {'CNST2':418703,
            'GGST2': 419102,
            'JTST2':419003,
            'CATT2':418903}

for csvfilename in [csvfilename1,csvfilename2,csvfilename3,csvfilename4]:
    #print csvfilename

#f = {'A':['mean'], 'B':['prod']}

    with open(csvfilename) as csvfile:#, open(fn,'a') as solartxtfile :
        pread = pandas.read_csv(csvfile,skiprows=6)#,skiprows=6,names=range(37)csvfile=range(22) )adiation.txt',sep=' ', index=False, header=False, mode='a')
        df = pread.iloc[1:] #removing the row for units
        #Subset the dataframe by choosing the required column
        df = df[['Station_ID','Date_Time','solar_radiation_set_1']]
        df [['solar_radiation_set_1']] = df[['solar_radiation_set_1']].astype(float)
        #create a column just based on the date and hour for group function
        df.loc[:,'Date'] = df.loc[:,('Date_Time')].str[0:10]
        df.loc[:,'Hour'] = df.loc[:,('Date_Time')].str[11:13]
        df.loc[:,'Month'] = df.loc[:,('Date_Time')].str[5:7]
        #only 19 hours will be used
        df = df[df['Hour']=='19']

        #need to group the df into each month to get the monthly percentile value!
        df_month= df.groupby(df['Month'],as_index=False).agg([ percentile(50), percentile(75), percentile(85)])
        print df_month
        df_month.loc[:,'Station'] = csvfilename[19:23]
        print df_month.to_csv(r'C:\DEV\MesoNet\MonthlySRPctl.txt',sep=' ', index=True, header=True , mode='a')

        #df_month= df.groupby(df['Month'],as_index=False).agg(lambda x: [np.percentile(x['solar_radiation_set_1'], q = 50),np.percentile(x['solar_radiation_set_1'], q = 75)])
'''
        df_month.loc[:,'FiftyPercentile']= df.groupby(df['Month'],as_index=False).agg(lambda x: np.percentile(x['solar_radiation_set_1'], q = 50))
        print df_month

##        df_month = df.groupby(df['Month'],as_index=False)['Percentile50'].transform({'solar_radiation_set_1' : 'np.nanpercentile'})
##        df_month = df.groupby(df['Month'],as_index=False)['Percentile50'].agg({'solar_radiation_set_1' : 'np.nanpercentile'})

        #The same way to calcaluated percentile using np.nanpercentile without filtering out
        print np.nanpercentile(df.loc[:,'solar_radiation_set_1'],50)
        #remove the NaN value in solar_radiation_set_1'
        df = df[np.isfinite(df['solar_radiation_set_1'])]
        ##updated on 08/14/2017 for quatile based calculation
        print df.loc[:,'solar_radiation_set_1'].quantile([.5, .75,.85])

##
##        #df.loc['HourlySolar'] = df.loc['HourlySolar'].apply('mean')
##        df.loc[:,'HourlySolar'] = df.loc[:,'solar_radiation_set_1'].groupby(df['Date']).transform('mean')
##        ##Subset only one record for each hour and select 3 fields
####        df = df[df.loc[:,('Date_Time')].str[14:19]=='00:00']
####        df = df[['Station_ID','Date_Time','HourlySolar']]
####        hourlycsv = csvfilename[:-4] + '_NineteenHourMean.csv'
####        ##Save hourly mean to a file for Mike review
####        print df.to_csv(hourlycsv,sep=' ', index=False, header=True , mode='a')
##        #print df.to_csv(r'C:\DEV\MesoNet\MonthlySolarRadiationGail_MesoWest.txt',sep=' ', index=False, header=True , mode='a')
##        df_month = df.groupby(df['Month'],as_index=False)['HourlySolar'].agg({'MaxMeanSolar' : 'max'})
##        #df.loc[:,'MonthlyHourlyMaxSolar'] = df.loc[:,'HourlySolar'].groupby(df['Date']).transform('max')
##        print df_month#.loc[:,'MonthlyHourlyMaxSolar']df_month
##        #df_month.rename(columns = {'MaxMeanSolar': (csvfilename[19:23] + '_MaxMeanSolar')}, inplace = True)
##        df_month.loc[:,'Station'] = csvfilename[19:23]
##        #print df_month.to_csv(r'C:\DEV\MesoNet\MonthlySRMesoWest.txt',sep=' ', index=False, header=True , mode='a')


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
    print df_station.to_csv(r'C:\DEV\MesoNet\MonthlySolarRadiation.txt',sep=' ', index=False, header=False, mode='a')
    #np.savetxt(r'C:\DEV\MesoNet\MonthlySolarRadiation.txt', df_station.values,fmt='%d')
'''
