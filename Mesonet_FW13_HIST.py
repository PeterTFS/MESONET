#------------------------------------------------------------------------------------
# Name:        MesoAPI_Mesonet.py
# Purpose:     Introduce the 4 stations observation from West Texas Mesomet for NFDRS
# Note :       Using a API instead of MesoPy
# Author:      pyang
# Created:     04/05/2017
# Copyright:   (c) pyang 2017
#-------------------------------------------------------------------------------
# Import the needed libraries
##from MesoPy import *

import time
import re
import string
import shutil
import pandas
import urllib, urllib2
import csv
import sys
import json
import os
import datetime
from dateutil import tz
from dateutil.relativedelta import relativedelta
import numpy as np
import math
import logging

pandas.options.mode.chained_assignment = None

# Processing the UTC time to Loca;
def UTC4LOCAL(observation_time):
    from_zone = tz.tzutc()
    to_zone = tz.tzlocal() #changed to local zone
    #to_zone = tz.gettz("US/Central")
    utc= datetime.datetime.strptime(observation_time,"%Y-%m-%dT%H:%M:%SZ")
    utc=utc.replace(tzinfo=from_zone)
    local = utc.astimezone(to_zone)
    return local
#---------------------------------------------------------------------------------------------------------------------------------------------------------
# Mesonet wind sensors are at a height of 10 meters, but the RAWS/WIMS standard is for 6 meter/20 foot winds.
#To estimate the 6 meter wind speed from the 10 meter measurement, the logarithmic wind profile method  math.log(6/0.0984)/math.log(10/0.0984) can be used
#To convert the knot to mph, the 1.15078 ration is used. Mike suggest mannually reduce the windspeed by 10% (*0.9) for WIMS
#----------------------------------------------------------------------------------------------------------------------------------------------------------
def windspeed(wind_speed):
##    print 'windspeed 10m',wind_speed
##    sixmeterwindspeed = wind_speed* 0.9 * math.log(6/0.0984)/math.log(10/0.0984)
##    print 'windspeed 6m',sixmeterwindspeed
    return wind_speed* 0.9 * math.log(6/0.0984)/math.log(10/0.0984)

#Use a special treatment for the value at 0.005 or smaller precipitation
def CorrectPrcpAmount(p):
    if p <= 0.005 : p = 0.0
    return p

#Define hourly precipitation based on the measurable amount of precipitation
def precipDuration(p):
    if p > 0.005:return 1
    else:return 0

#Formatting Float into Int
def formatFloat(v):
    return int(round(v))

#Formatting precipitation with 2 decimals
def formatPrecip(p):
    return round(p, 2)

#-------------------------------------------------------------------------------
# Function for generate hours report for each hour (including the 'O' and 'R' record)
# Input: data.fram for the day and row for the current hour and a dictionary
# Output: A dictionary X with all required information
# Use the 18 UTC hour instead of local hour to check if it is the 'O' or 'R'
#------------------------------------------------------------------------------
def Report9(meso,X,row):
    UTC_Hour = row['Date_Hour'][-2:]
    X['Ob Date'] = row['obs_time_local'].strftime("%Y%m%d")
    X['Ob Time']=  row['obs_time_local'].strftime("%H%M")
    #X['SeasonCode'] = get_season(LOCTIME)
    ##Larry mentioned to round up the minutes to whole instead of changing the system configuration,
    ##however, Brad and Mike prefer use the original time for human intervention (updated 09/24/2015)
    if UTC_Hour=='18': #Should always be 18 and don't interfere with local time
        X['Type']='O'
    else:
        X['Type']='R'

    X['Temp'] = formatFloat(row['AirTemperature'])
    X['Moisture']= formatFloat(row['RelativeHumidity'])#use type 2 Relative Humidity
    X['WindSpeed'] = formatFloat(row['WindSpeed']) #this has been re-calculated
    if X['WindSpeed']==0:
        X['WindDir']=0
    else:
        X['WindDir'] = formatFloat(row['WindDirection'])

    X['GustSpeed'] = formatFloat(row['WindGust'])
##    if X['WindGust']==0:
##        X['GustDir']= 0
##    else:
##        X['GustDir'] = X['WindDir']

    X['SolarRad'] = formatFloat(row['SolarRadiation'])

    #print 'Ob Time:',X['Ob Time'],'WindSpeed:',X['WindSpeed'],'WindDir:',X['WindDir']##'GustDir:',X['GustDir'],'GustSpeed:',X['GustSpeed']
    X['Tmax'] = formatFloat(max(meso['MaxAirTemperature']))
    X['Tmin'] = formatFloat(min(meso['MinAirTemperature']))

    X['RHmax'] = formatFloat(max(meso['RelativeHumidity']))
    X['RHmin'] = formatFloat(min(meso['RelativeHumidity']))

    TotPr = 0
    #This should be a method to compute the measurable precipitation, the measuable precipitation should be bigger than 0.005
    X['PrecipDur'] = meso['precip_duration'].sum()
    TotPr = meso['Precipitation'].sum()
    ##print 'TotPr', TotPr

##    #This is the total precipitation in the previous 24 hours, given in thousands of an inch. For
##    #example, an observation of 0.04? would be entered as ___40, preceded by three
##    #blanks/spaces. An observation of 1.25? would be entered as _1250, preceded by one space.
##    #An observation of no rainfall would be entered as all blanks/spaces.
##    #Updated 10/26/2015, rounding precip into hundredths
    X['PrecipAmt'] = formatPrecip(TotPr)
##    StateOfWeather(X,row)
    #Moisture Type code (1=Wet bulb, 2=Relative Humidity, 3=Dewpoint).
    X['MoistType'] = 2
    #Measurement Type code: 1=U.S.
    X['MeasType'] = 1

##   no need greeness code and seasoncode because of 78G model
##    X['Herb'] = herbaceousGreennessF[X['Station Number']]
##    X['Shrub'] = shrubGreennessF[X['Station Number']]
##    X['SeasonCode'] = seasonCode[X['Station Number']]
##    if X['State of Weather'] == 7:
##        X[SnowFlag]='Y'
    #print X
    return X


def Report13(meso,X,row):
    UTC_Hour = row['Date_Hour'][-2:]
    X['Ob Date'] = row['obs_time_local'].strftime("%Y%m%d")
    X['Ob Time']=  row['obs_time_local'].strftime("%H%M")
    #X['SeasonCode'] = get_season(LOCTIME)
    if UTC_Hour=='18': #Should always be 18 and don't interfere with local time
        X['Type']='O'
    else:
        X['Type']='R'
    X['Temp'] = formatFloat(row['AirTemperature'])
    X['Moisture']= formatFloat(row['RelativeHumidity'])#use type 2 Relative Humidity
    X['WindSpeed'] = formatFloat(row['WindSpeed']) #this has been re-calculated
    if X['WindSpeed']==0:
        X['WindDir']=0
    else:
        X['WindDir'] = formatFloat(row['WindDirection'])

    X['GustSpeed'] = formatFloat(row['WindGust'])

    if X['GustSpeed']==0:
        X['GustDir']= 0
    else:
        X['GustDir'] = X['WindDir']

    X['Tmax'] = formatFloat(max(meso['MaxAirTemperature']))
    X['Tmin'] = formatFloat(min(meso['MinAirTemperature']))

    X['RHmax'] = formatFloat(max(meso['RelativeHumidity']))
    X['RHmin'] = formatFloat(min(meso['RelativeHumidity']))

    TotPr = 0
    #This should be a method to compute the measurable precipitation, the measuable precipitation should be bigger than 0.005
    X['PrecipDur'] = meso['precip_duration'].sum()
    TotPr = meso['Precipitation'].sum()
    ##print 'TotPr', TotPr
    X['PrecipAmt'] = formatPrecip(TotPr)
##    StateOfWeather(X,row)
    #Moisture Type code (1=Wet bulb, 2=Relative Humidity, 3=Dewpoint).
    X['MoistType'] = 2
    #Measurement Type code: 1=U.S.
    X['MeasType'] = 1
    #Solar radiation (watts per square meter).
    X['SolarRad'] = formatFloat(row['SolarRadiation'])

##   no need greeness code and seasoncode because of 78G model
##    X['Herb'] = herbaceousGreennessF[X['Station Number']]
##    X['Shrub'] = shrubGreennessF[X['Station Number']]
##    X['SeasonCode'] = seasonCode[X['Station Number']]
##    if X['State of Weather'] == 7:
##        X[SnowFlag]='Y'
    return X
#--------------------------------------------------------------------
# Function for Formatting the extracted information to the wf9 format
# Input: Dictionary (X) that contains all the information
# Output: A string with fw9 format
#-------------------------------------------------------------------
def FormatFW9( X ):
    ##To define the byte writing structure using a tuple
    Fields = (('W98',(0,'3A')),('Station Number',(3,'6A')),('Ob Date',(9,'8A')),('Ob Time',(17,'4A')),
              ('Type',(21,'1A')),('State of Weather',(22,'1N')),('Temp',(23,'3N')),('Moisture',(26,'3N')),
              ('WindDir',(29,'3N')),('WindSpeed',(32,'3N')),('10hr Fuel',(35,'2N')),('Tmax',(37,'3N')),
              ('Tmin',(40,'3N')),('RHmax',(43,'3N')),('RHmin',(46,'3N')),('PrecipDur',(49,'2N')),
              ('PrecipAmt',(51,'5N')),('WetFlag',(56,'1A')),('Herb',(57,'2N')),('Shrub',(59,'2N')),
              ('MoistType',(61,'1N')),('MeasType',(62,'1N')),('SeasonCode',(63,'1N')),('SolarRad',(64,'4N'))
              )
    Out = []
    for f,p in Fields:
        val = X[f]
        #str(X[f]).zfill()
        length = int(p[1][:-1]) #not working
        format = p[1][-1]
            #This is the total precipitation in the previous 24 hours, given in thousands of an inch. For
            #example, an observation of 0.04? would be entered as ___40, preceded by three
            #blanks/spaces. An observation of 1.25? would be entered as _1250, preceded by one space.
            #An observation of no rainfall would be entered as all blanks/spaces.
        if f=='PrecipAmt':
            if val == 0:
                val=-999
            else:
                val*=1000
        #WindParaList = ['WindSpeed','WindDir']
        WindParaList = ['WindSpeed']#Updated on 01092017 after meeting discussion that the 0 should be replaced by 3 mph
        if f in WindParaList :
            if val == 0:
               val = 3 #Updated on 01092017 after discussion that the 0 windspeed should be replaced by 3 mph
        else:
            ZeroPad = ''
        if format == 'N' and val != -999:
            #q = str(0).zfill(length)
            q = '%%%s%dd' % (ZeroPad,length)
        elif format == 'N' and val == -999:
            val = ' '
            q = '%%%s%ds' % (ZeroPad,length)
        else:
            q = '%%%ds' % length
        try:
            Out.append( q % val )
        except:
            print f, p, q, val, type(val)
    return string.join( Out, '' )

#--------------------------------------------------------------------
# Function for Formatting the extracted information to the wf13 format
# Input: Dictionary (X) that contains all the information
# Output: A string with fw13 format
#-------------------------------------------------------------------
def FormatFW13( X ):
    Fields = (('W13',(0,'3A')),('Station Number',(3,'6N')),('Ob Date',(9,'8A')),('Ob Time',(17,'4A')),
              ('Type',(21,'1A')),('State of Weather',(22,'1N')),('Temp',(23,'3N')),('Moisture',(26,'3N')),
              ('WindDir',(29,'3N')),('WindSpeed',(32,'3N')),('10hr Fuel',(35,'2N')),('Tmax',(37,'3N')),
              ('Tmin',(40,'3N')),('RHmax',(43,'3N')),('RHmin',(46,'3N')),('PrecipDur',(49,'2N')),
              ('PrecipAmt',(51,'5N')),('WetFlag',(56,'1A')),('Herb',(57,'2N')),('Shrub',(59,'2N')),
              ('MoistType',(61,'1N')),('MeasType',(62,'1N')),('SeasonCode',(63,'1N')),('SolarRad',(64,'4N')),
              ('GustDir',(68,'3N')),('GustSpeed',(71,'3N')),('SnowFlag',(74,'1A')) ##Updated 02/03/2016 for the new three parameters
              )
    Out = []
    for f,p in Fields:
        val = X[f]
        #str(X[f]).zfill()
        length = int(p[1][:-1]) #not working
        format = p[1][-1]
        #This is the total precipitation in the previous 24 hours, given in thousands of an inch. For
        #example, an observation of 0.04? would be entered as ___40, preceded by three
        #blanks/spaces. An observation of 1.25? would be entered as _1250, preceded by one space.
        #An observation of no rainfall would be entered as all blanks/spaces.

        if f=='PrecipAmt':
            if val == 0:
                val=-999
            else:
                val*=1000
        ##Check if there is a zero in WindSpeed and WindDirection
        WindParaList = ['WindSpeed']#Updated on 01092017 after meeting discussion that the 0 should be replaced by 3 mph
        if f in WindParaList :
            if val == 0:
               val = 3 #Updated on 01092017 after discussion that the 0 windspeed should be replaced by 3 mph
        else:
            ZeroPad = ''
        if format == 'N' and val != -999:
            #q = str(0).zfill(length)
            q = '%%%s%dd' % (ZeroPad,length)
        elif format == 'N' and val == -999:
            val = ' '
            q = '%%%s%ds' % (ZeroPad,length)
        else:
            q = '%%%ds' % length
        try:
            Out.append( q % val )
        except:
            print f, p, q, val, type(val)
    return string.join( Out, '' )
#----------------------------------------------------------------------------------------
# Function to inteprete the downloaded West TX Mesonet csv file and extract the relevant information
# Input : csv file for the precious 48 hours;station name
# Output: string stream fils formatted in fw9
#----------------------------------------------------------------------------------------
def IntepreteMesoNet(csvfile,STATION,ID):
    #Open the downloaded csv for information
    with open(csvfile) as filt_csv:
        pread = pandas.read_csv(filt_csv,skiprows=6)
    #print type(df.index)
    #print(pread.ix[0,:])
    df = pread.iloc[1:] #removing the row for units
    if 'precip_accum_one_minute_set_1' in pread.columns:
        df.rename(columns = {'precip_accum_one_minute_set_1':'precip_accum_set'}, inplace = True)
    #    df.loc[:,'precip_accum_set'] = df.loc[:,'precip_accum_one_minute_set_1']
    else:
        df.rename(columns = {'precip_accum_five_minute_set_1':'precip_accum_set'}, inplace = True)
    #    df.loc[:,'precip_accum_set'] = df.loc[:,'precip_accum_five_minute_set_1']

    #Subset the dataframe by choosing the required column
    df = df[['Station_ID','Date_Time','air_temp_set_1','relative_humidity_set_1',
            'wind_speed_set_1','wind_direction_set_1','wind_gust_set_1','precip_accum_set',
            'solar_radiation_set_1','dew_point_temperature_set_1d']]

    #create a column just based on the date and hour for group function
    df.loc[:,'Date_Hour'] = df.loc[:,('Date_Time')].str[0:13]
    ################################################################################################################################################
    ##Need to set up a series of rules for processing temperature, relative humidity, wind speed, wind direction, solar radiation, precipitation...
    ## Precipitation: get the maximum value for each 5 minutes as the precipitation amount for this hour
    ## SolarRadiation: get the mean value for each 5 minutes as the solar radiation for this hour
    #First need to change the data type from string to float before processing
    df [['air_temp_set_1','relative_humidity_set_1','solar_radiation_set_1',
        'wind_speed_set_1','wind_direction_set_1','wind_gust_set_1','precip_accum_set',
        'dew_point_temperature_set_1d']] = df[['air_temp_set_1','relative_humidity_set_1','solar_radiation_set_1',
        'wind_speed_set_1','wind_direction_set_1','wind_gust_set_1','precip_accum_set',
        'dew_point_temperature_set_1d']].astype(float)

    #print(type(df.loc[:,'precip_accum_set']))

    #Apply group function on fields for hourly records
    df.loc[:,'Precipitation'] = df.loc[:,'precip_accum_set'].groupby(df['Date_Hour']).transform('max')
    df.loc[:,'SolarRadiation'] = df.loc[:,'solar_radiation_set_1'].groupby(df['Date_Hour']).transform('mean')
    df.loc[:,'AirTemperature'] = df.loc[:,'air_temp_set_1'].groupby(df['Date_Hour']).transform('mean')
    df.loc[:,'RelativeHumidity'] = df.loc[:,'relative_humidity_set_1'].groupby(df['Date_Hour']).transform('mean')
    df.loc[:,'MaxAirTemperature'] = df.loc[:,'air_temp_set_1'].groupby(df['Date_Hour']).transform('max')
    df.loc[:,'MinAirTemperature'] = df.loc[:,'air_temp_set_1'].groupby(df['Date_Hour']).transform('min')
    df.loc[:,'WindSpeed'] = df.loc[:,'wind_speed_set_1'].groupby(df['Date_Hour']).transform('mean')
    df.loc[:,'WindDirection'] = df.loc[:,'wind_direction_set_1'].groupby(df['Date_Hour']).transform('mean')
    df.loc[:,'WindGust'] = df.loc[:,'wind_gust_set_1'].groupby(df['Date_Hour']).transform('max')

    #Keep only one record for each hour by choosing the end our records (How about choose only the end hour record)
    df = df.groupby(df['Date_Hour']).apply(lambda t: t[t['Date_Time']==t['Date_Time'].max()])
    if len(df) != 48:
        MSG = "The records of station: %s is %d less than 48 records "% (STATION,len(df))
        print(MSG)
        #Should not exit here, use continue
        logging.info(MSG)
        print('something is wrong!',len(df))
    ##need to confirm the unit of several parameters
    '''
    air_temp_set_1                    Fahrenheit
    relative_humidity_set_1                    %
    wind_speed_set_1                  Miles/hour
    wind_direction_set_1                 Degrees
    wind_gust_set_1                   Miles/hour
    precip_accum_five_minute_set_1        Inches
    solar_radiation_set_1                 W/m**2
    dew_point_temperature_set_1d          Fahrenheit
    '''
    #print(df.ix[1,:])

    MESO = df[['Station_ID','Date_Time','Precipitation','SolarRadiation','AirTemperature','RelativeHumidity',
               'MaxAirTemperature','MinAirTemperature','WindSpeed','WindDirection','WindGust','Date_Hour']]

    #Add a column for the local time zone
    MESO.loc[:,'obs_time_local'] = MESO.loc[:,'Date_Time'].apply(UTC4LOCAL)

    #Regarding the rain duration, onyly > 0.005 will be recorded(so 0.005 should be disregarded)
    MESO.loc[:,'Precipitation']=MESO.loc[:,'Precipitation'].apply(CorrectPrcpAmount)
    #To define the Precipitation Duration hours
    MESO.loc[:,'precip_duration']=MESO.loc[:,'Precipitation'].apply(precipDuration)
    ##MESO.loc[:,'precip_duration']=MESO.loc[:,'Precipitation'].apply(lambda t: 1 if t > 0.0 else 0)
    MESO.loc[:,'WindSpeed']=MESO.loc[:,'WindSpeed'].apply(windspeed)
    #Gust speed
    MESO.loc[:,'WindGust']=MESO.loc[:,'WindGust'].apply(windspeed)

    #define a dictionary to hold all the information
##    X9 = {'W98':'W98', 'Station Number':'000000', 'Ob Date':'YYYYMMDD', 'Ob Time':0,
##              'Type':'R', 'State of Weather':0, 'Temp':0, 'Moisture':0,
##              'WindDir':0, 'WindSpeed':0, '10hr Fuel':0, 'Tmax':-999,
##              'Tmin':-999, 'RHmax':-999, 'RHmin':-999, 'PrecipDur':0,
##              'PrecipAmt':0, 'WetFlag':'N', 'Herb':20, 'Shrub':15,
##              'MoistType':2, 'MeasType':1, 'SeasonCode':3, 'SolarRad':0
##            }
    X9 = {'W98':'W98', 'Station Number':'000000', 'Ob Date':'YYYYMMDD', 'Ob Time':0,
              'Type':'R', 'State of Weather':0, 'Temp':0, 'Moisture':0,
              'WindDir':0, 'WindSpeed':0, '10hr Fuel':0, 'Tmax':-999,
              'Tmin':-999, 'RHmax':-999, 'RHmin':-999, 'PrecipDur':0,
              'PrecipAmt':0, 'WetFlag':'N', 'Herb':20, 'Shrub':15,
              'MoistType':2, 'MeasType':1, 'SeasonCode':3, 'SolarRad':0
            }
    X13 = {'W13':'W13', 'Station Number':'000000', 'Ob Date':'YYYYMMDD', 'Ob Time':0,
      'Type':'R', 'State of Weather':0, 'Temp':0, 'Moisture':0,
      'WindDir':0, 'WindSpeed':0, '10hr Fuel':0, 'Tmax':-999,
      'Tmin':-999, 'RHmax':-999, 'RHmin':-999, 'PrecipDur':0,
      'PrecipAmt':0, 'WetFlag':'N', 'Herb':20, 'Shrub':15,
      'MoistType':2, 'MeasType':1, 'SeasonCode':3, 'SolarRad':0,
      'GustDir':0,'GustSpeed':0,'SnowFlag':'N' ##Updated 02/03/2016 for the new three parameters
      ##According to Juan, the gust direction of the peak wind should be the same with the hourly wind direction
    }

    #pass the station ID
    X9['Station Number'] = ID
    ##X13['Station Number'] = ID
    X13['Station Number'] = ID
    with open(fileWF13,'a') as F13, open(fileWF9,'a') as F9:
    #Get each a 24 hour period for calculating the fire weather parameters
        for hour in range(1,25): #change the sequence from later to latest
            #print hour, hour+24
            df = MESO.iloc[hour:hour+24]
            #print(len(df))
            #need to create a dict from the last row
            currenthour= df.tail(1).set_index('Station_ID').T.to_dict()
            #print(currenthour,type(currenthour))
            currenthourdf =  currenthour[STATION]
            #print(currenthourdf)
            #currenthour = currenthour[STATION]
##            Report9(df,X9,currenthourdf)
##            ##Write the records into a FW13 and FW9 format
##            F9.write( FormatFW9( X9 ) +'\n' )
            ##Pass the previous 24 hours and current hour record for WIMS fw9 format
            Report13(df,X13,currenthourdf)
            ##Write the records into a FW13 and FW9 format
            F13.write( FormatFW13( X13 ) +'\n' )
############################################
#Setting the directories for data archiving
############################################
WorkSpace = os.getcwd()
print WorkSpace
MesonetArchive = os.path.join(WorkSpace, "CSV")
FW13Archive  = os.path.join(WorkSpace, "FW13")
FW9Archive  = os.path.join(WorkSpace, "FW9")
LOGArchive  = os.path.join(WorkSpace, "LOG")

if not os.path.exists(MesonetArchive):
    os.makedirs(MesonetArchive)
if not os.path.exists(FW13Archive):
    os.makedirs(FW13Archive)
if not os.path.exists(FW9Archive):
    os.makedirs(FW9Archive)
if not os.path.exists(LOGArchive):
    os.makedirs(LOGArchive)

LOGfile = os.path.join(LOGArchive,"MESONET4WIMS.log")
#Set up a logger for logging the running information
logging.basicConfig(filename=LOGfile,
                    format='%(asctime)s   %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S%p',
                    filemode='w',
                    level=logging.INFO)

#define the fire weather file name
fileWF13 = os.path.join(WorkSpace, "tx-mesonet.fw13")
fileWF9 = os.path.join(WorkSpace, "tx-mesonet.fw9")
#for each day first removing the existing file
if os.path.isfile(fileWF13):
    os.remove(fileWF13)
#for each day first removing the existing file
if os.path.isfile(fileWF9):
    os.remove(fileWF9)

#today = date(datetime.now())
# set up date information

#need to create a list of two day cuples
#need to create a list of two day cuples

five_yrs_ago = datetime.datetime.utcnow() - relativedelta(years=5)
delta = datetime.datetime.utcnow()-five_yrs_ago
backdays = delta.days

four_yrs_ago = datetime.datetime.utcnow() - relativedelta(years=4)
delta = datetime.datetime.utcnow()-four_yrs_ago
backdays = delta.days

ten_yrs_ago = datetime.datetime.utcnow() - relativedelta(years=10)
delta = datetime.datetime.utcnow()-ten_yrs_ago
backdays = delta.days

today = datetime.datetime.today()
TODATSTR = today.strftime("%Y%m%d")

daytupleList = []
START_UTC = "1900"
END_UTC = "1859"
for d in range(backdays,1,-1):
    pre_day = datetime.timedelta(days=d)
    cur_day = datetime.timedelta(days=(d-2))
    PREDAY = today - pre_day
    CURDAY = today - cur_day
    #print PREDAY, CURDAY
    PREDAYSTR = PREDAY.strftime("%Y%m%d") + START_UTC
    CURDAYSTR = CURDAY.strftime("%Y%m%d") + END_UTC
    daytupleList.append((PREDAYSTR,CURDAYSTR))



daytupleList = [('201708131900', '201708151859')]
##StartTime  =  TWODAYSTR + START_UTC
##EndTime = TODATSTR + END_UTC
##
##StartTime  =  '201204051900'
##EndTime = TODATSTR + END_UTC

#print StartTime,EndTime

'''
The stations Mike requested are:
1.	6E Candian, Hemphill county, LDM ID: XCA1 MesoWestID: CNST2
2.	2ESE Gail, Borden county, LDM ID: XGGS MesoWestID: GGST2
3.	1SSE Jayton, Kent county, LDM ID: XJTS MesoWestID: JTST2
4.	3 NNW Quitaque, Briscoe county, LDM ID: XQU1 MesoWestID: CATT2
'''
#only GGST2 (Gail) had 10 years data
#The WIMS ID's: 418703 - Canadian, 419102 - Gail, 419003 - Jayton, 418903 - Quitaque.

Stations = {'CNST2':418703,
            'GGST2': 419102,
            'JTST2':419003,
            'CATT2':418903}


Stations = {'GGST2': 419102}
##Station&ID = {'Candian':'CNST2',
##                'Gail': 'GGST2',
##                'Jayton':'JTST2',
##                'Quitaque':'CATT2'}

Stations = {'MNDT2 ':419202}

#####Parameters for downloading Mesonet data using MesoWest API#####
MyToken = '994a7e628db34fc68503d44c447aaa6f'
base_url = 'http://api.mesowest.net/v2/stations/'
query_type = 'timeseries'
csv_format = '&output=csv'
units = '&units=precip|in,temp|F,speed|mph'
#######################################################################
##Loop through each station for downloading and processing Mesonet Data
for STATION,ID in Stations.items():
    for datetuple in daytupleList:
        if datetuple == ('201208151900', '201208171859') or datetuple == ('201208161900', '201208181859'):
            continue
        else:
            StartTime,EndTime = datetuple

        try:
            #print STATION,ID
            api_string = base_url + query_type + '?' + 'stid=' + STATION + '&start=' + StartTime + '&end=' + EndTime + '&token=' + MyToken + units + csv_format
            print api_string
            print 'Downloading ASOS data for Station: ' + STATION
            #filename = "%s-%s-MesoWest.csv"%(STATION,today.strftime("%Y%m%d%H%M"))
            filename = "%s-%s-%s.csv"%(STATION,StartTime,EndTime)
            print filename
            csvfile = os.path.join(MesonetArchive, filename)
            print csvfile
            urllib.urlretrieve(api_string,csvfile)
        # Catch the error type from the urllib.urlretrieve(URL,csvfile)
        except:
            MSG = "The MesoWest source data were not downloaded successfully for Station: %s %s"% (STATION,Stations[STATION])
            print MSG
            #Should not exit here, use continue
            logging.info(MSG)
            continue
            #exit()
        IntepreteMesoNet(csvfile,STATION,ID)
##    try:
##        print 'Processing MesoNet Station: ' + STATION + ' with Station ID: ' + str(ID)
##        IntepreteMesoNet(csvfile,STATION,ID)
##    except:
##        MSG = "The MesoNet data were not processed successfully for Station: %s %s"% (STATION,Stations[STATION])
##        logging.info(MSG)
##        print "Unexpected error:", sys.exc_info()[0]
        #sendEmail(MSG)


#Archive the WF9 file for each day
##archivefileWF9 = today.strftime("%Y%m%d") + ".fw9"
##archivefileWF9 = os.path.join(FW9Archive, archivefileWF9)
##shutil.copyfile(fileWF9,archivefileWF9)
###Archive the WF13 file for each day
##archivefileWF13 = today.strftime("%Y%m%d") + ".fw13"
##archivefileWF13 = os.path.join(FW13Archive, archivefileWF13)
##shutil.copyfile(fileWF13,archivefileWF13)
###Keep Record of the log file
##archivefileLOG = today.strftime("%Y%m%d") + ".log"
##archivefileLOG = os.path.join(LOGArchive, archivefileLOG)
#shutil.copyfile(LOGfile,archivefileLOG)