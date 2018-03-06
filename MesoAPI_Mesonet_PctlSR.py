#------------------------------------------------------------------------------------
# Name:        MesoAPI_Mesonet.py
# Purpose:     Introduce the 4 stations observation from West Texas Mesomet for NFDRS
# Note : Using a API instead of MesoPy
# Author:      pyang
# Updated 06/02/2017 Calculating SR% based on historical analyses and derive SOW
# Updated 08/18/2017 using a percentile based solar radiation and use the exact oberservation for SOW
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
import numpy as np
import math
import logging
import smtplib
import numpy as np
pandas.options.mode.chained_assignment = None

#Send email if issue happenned for notifying Mike for mannually editting
def sendEmail(TXT):
    server = smtplib.SMTP('tfsbarracuda.tamu.edu', 25)
    #server.set_debuglevel(1)
    SUBJECT = 'There is an issue with MESONET2WIMS'
    message = 'Subject: %s\n\n%s' % (SUBJECT, TXT)
    print "Sending email to " + message
    #tolist=["pyang@tfs.tamu.edu","rmodala@tfs.tamu.edu"]
    tolist=["pyang@tfs.tamu.edu"]
    server.sendmail("pyang@tfs.tamu.edu", tolist, message)

# Processing the UTC time to Loca;
def UTC4LOCAL(observation_time):
    from_zone = tz.tzutc()
    to_zone = tz.tzlocal() #changed to local zone
    #to_zone = tz.gettz("US/Central")
    utc= datetime.datetime.strptime(observation_time,"%Y-%m-%dT%H:%M:%SZ")
    utc=utc.replace(tzinfo=from_zone)
    local = utc.astimezone(to_zone)
    return local

#--------------------------------------------------------------------------------------------------------------------------------
# Convert UTC to Standard time (only for central standard time)
# Since we know there will be always 6 hours difference between the UTC and standard time(disregard the daylight saving effect
#-------------------------------------------------------------------------------------------------------------------------------
def UTC2STANDARD(TIMESTR):
    utc= datetime.datetime.strptime(TIMESTR,"%Y-%m-%dT%H:%M:%SZ")
    standard = utc - datetime.timedelta(hours=6)
    #print utc,standard
    return standard

#-----------------------------------------------------------------------------------------------------------------
# Mesonet wind sensors are at a height of 10 meters, but the RAWS/WIMS standard is for 6 meter/20 foot winds.
#To estimate the 6 meter wind speed from the 10 meter measurement, the logarithmic wind profile method  math.log(6/0.0984)/math.log(10/0.0984) can be used
#To convert the knot to mph, the 1.15078 ration is used. Mike suggest mannually reduce the windspeed by 10% (*0.9) for WIMS
#------------------------------------------------------------------------------------------------------------------
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
##    X['Ob Date'] = row['obs_time_local'].strftime("%Y%m%d")
##    X['Ob Time']=  row['obs_time_local'].strftime("%H%M")
    X['Ob Date'] = row['obs_time_standard'].strftime("%Y%m%d")
    X['Ob Time']=  row['obs_time_standard'].strftime("%H%M")
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

    ##print meso.tail(1)
    ##print row
## based on the Solar Radiation value and precipitation to calculate SOW
    StateOfWeather(X,row,SRtable,SRtable_Percentile)
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


#----------------------------------------------------------------------------------------
'''
From WIMS User's Guide
New gateway routines in WIMS Version 2.0 estimate the State of Weather (SOW) and Wet Flag (WF) for the “R” observations at regular_scheduled_obs_time (RS) from
solar radiation (percent of possible for the latitude and date, and time) and precipitation amount and duration for the current hour,
the previous 3 hours, and the past 24 hours. Default thresholds are by climate class.
The following cases define logic and thresholds that set SOW and Wet Flag for the RS observation. Station owners may modify and restore default thresholds in the ENFDR module.
Case 1: No Precipitation in past 24 hours: Set SOW based solely on Solar Radiation
PCNT_SOLAR is the 1-hour averaged Solar Radiation (watts/m^2) converted to percent possible for that station/date/hour. In this example if PCNT_SOLAR >= 80, SOW would be 0 (clear).
Condition: (24HR_PRECIP ==0)
Actions: If (PCNT_SOLAR >= Pcnt_Clear) SOW = 0 (Clear, < 1/10 cloud cover)
If (PCNT_SOLAR >= Pcnt_Scattered AND PCNT_SOLAR < Pcnt_Clear) SOW = 1 (Scattered, 1/10 to 5/10 cloud cover)
If (PCNT_SOLAR >= Pcnt_Broken AND PCNT_SOLAR < Pcnt_Scattered) SOW = 2 (Scattered, 6/10 to 9/10 cloud cover)
If (PCNT_SOLAR < Pcnt_Broken) SOW = 3 (Overcast, > 9/10 cloud cover)
If (PCNT_SOLAR < Pcnt_Broken AND RELATIVE_HUMIDITY > 95) SOW = 4 (Fog)
If (OBS_SOLAR < 25) SOW = 3 Catch observations during the nighttime.
Wetflag = ‘N’
Case 2: Precipitation in last 24 hours, but none last 3 hours
Condition: (24HR_PRECIP<>0 AND 3HR_PRECIP == 0)
Actions: SOW set as in Case 1.
WetFlag = ‘N’
IF (24HR_DURATION > 24HR_DUR_WetFlag OR 24HR_AMT > 24HR_AMT_WetFlag) WetFlag = “Y”
Case 3: Precipitation during previous 3 hours but none last hour
Condtion: (3HR_PRECIP> 0 AND 1HR_PRECIP== 0)
Actions: SOW set as in Case 1.
WetFlag = ‘N’
IF (3HR_PRECIP_DUR > 3HR_DUR_WetFlag OR 3HR_PRECIP_AMT > 3HR_AMT_WetFlag) WetFlag = “Y”
Case 4: Precipitation during previous hour
Condition: (1HR_ PRECIP > 0)
Actions:
if (oneHrPrecipAmt <= one_hr_drizzle) SOW and WetFlag set as in Case 1.
Otherwise
if (oneHrPrecipAmt > one_hr_drizzle and oneHrPrecipAmt <= one_hr_rain) sow = 6; // rain
if (oneHrPrecipAmt > one_hr_rain and oneHrPrecipAmt <= one_hr_shower) sow = 8; //t-showers
if (oneHrPrecipAmt > one_hr_shower) sow = 9; //thunderstorm
if (dbtemp<=32.0f) sow = 7; //snow/sleet
Then set WetFlag for SOW or 3-hr or 24-hour WF Thresholds
wetflag = "N" ;
if (sow==6 or sow==7 or twentyfourHrPrecipDur> twentyfour_hr_dur_wetflag or twentyfourHrPrecipAmt > twentyfour_hr_amt_wetflag
or threeHrPrecipDur>three_hr_dur_wetflag or threeHrPrecipAmt>three_hr_amt_wetflag) wetflag = "Y";
'''

# Function to determin the State of Weather value by a defined rule
# RULE: from 9 to 0
# It will need precipitation for current hour,( the previous 3 hours, and the past 24 hours) and solar radiation and the historical SR table as input
# first look into the raw_text for lightning information,
# then to wx_string for thunderstorm shower, snow ,rain and drizzle
# then to skycover for 4,3,2,1
# Input : Dictionary and Current hour record
# Updated (09/30/2015):
#----------------------------------------------------------------------------------------
def StateOfWeather(X,row,SRtable,SRtable_Percentile):
    #print X['Ob Date']
    #print row
    month = int(X['Ob Date'][4:6])
    stationid = str(X['Station Number'])

##Condition: (24HR_PRECIP ==0)
##Actions: If (PCNT_SOLAR >= Pcnt_Clear) SOW = 0 (Clear, < 1/10 cloud cover)
##If (PCNT_SOLAR >= Pcnt_Scattered AND PCNT_SOLAR < Pcnt_Clear) SOW = 1 (Scattered, 1/10 to 5/10 cloud cover)
##If (PCNT_SOLAR >= Pcnt_Broken AND PCNT_SOLAR < Pcnt_Scattered) SOW = 2 (Scattered, 6/10 to 9/10 cloud cover)
##If (PCNT_SOLAR < Pcnt_Broken) SOW = 3 (Overcast, > 9/10 cloud cover)
##If (PCNT_SOLAR < Pcnt_Broken AND RELATIVE_HUMIDITY > 95) SOW = 4 (Fog)
##If (OBS_SOLAR < 25) SOW = 3 Catch observations during the nighttime.
    #print row['Precipitation'],X['PrecipAmt']
    CurrentHourPrecip = row['Precipitation']
    CurrentHourSolar = row['EndHourSolarRadiation']
    print CurrentHourSolar,row['SolarRadiation'],row['SolarRadiationMedian']
    if CurrentHourPrecip <= 0.1: ##the current hour precipitaiton is 0 and use solar radiation for get the SOW
        #intead of using a single average value using a combintation of p50,p75 and p85 to determine the state of code
        #print SRtable
        #print month,stationid,type(month),type(stationid)
        df_row = SRtable_Percentile.loc[(SRtable_Percentile['Month'] == month) & (SRtable_Percentile['StationID']== int(stationid)) ]
        p50 = float(df_row['percentile_50'])
        p75 = float(df_row['percentile_75'])
        p85 = float(df_row['percentile_85'])

        MaxHourSR = SRtable.loc[month,stationid]
        print "currenthoursolar: ",CurrentHourSolar,"Hourly Mean Solar: ",row['SolarRadiation'],"Hourly Max Solar: ",MaxHourSR,"CurrentHourSolar/MaxHourSR: ",(CurrentHourSolar/MaxHourSR),"X['SolarRad']/MaxHourSR: ",(row['SolarRadiation']/MaxHourSR)
        ##need to find a way to get the close value to RAWS one time observation duting the hour


        #using the current hour solar radiation
        if CurrentHourSolar < 25:
            Weathercode = 3
        else:
            ##need to discuss the rules:
            print "currenthoursolar: ",CurrentHourSolar,"p85: ",p85,"p75: ",p75,"p50: ",p50
            if CurrentHourSolar > p85:
                Weathercode = 0;
            elif p75 < CurrentHourSolar <= p85:
                Weathercode = 1;
            elif p50 < CurrentHourSolar <= p75:
                Weathercode = 2
            elif CurrentHourSolar <= p50:
                if X['Moisture'] >= 95:
                ##If (PCNT_SOLAR < Pcnt_Broken AND RELATIVE_HUMIDITY > 95) SOW = 4 (Fog)
                    Weathercode = 4
                else:
                    Weathercode = 3

        #print row
        #Different way to calculate State of Weather based on percentile breaks

        PCNT_SOLAR = row['SolarRadiation']/MaxHourSR
        PCNT_SOLAR = round(float(PCNT_SOLAR),2)
        #print PCNT_SOLAR,X['SolarRad'],X['Moisture']
        ##If (OBS_SOLAR < 25) SOW = 3 Catch observations during the nighttime
        ##If (OBS_SOLAR < 25) SOW = 3 Catch observations during the nighttime
        #Also need to add the night time hours for confirming
        if X['SolarRad'] < 25:
            Weathercode_2 = 3
            ##If (PCNT_SOLAR >= Pcnt_Clear) SOW = 0 (Clear, >90% SR)
        else:
            if PCNT_SOLAR >= 0.85:
                Weathercode_2 = 0
            ##1---Scattered:
            elif 0.75 <= PCNT_SOLAR < 0.85:
                Weathercode_2 = 1
            ##2---Broken:
            elif  0.50 <= PCNT_SOLAR < 0.75:
                Weathercode_2 = 2
            ##3---Overcast:
            elif PCNT_SOLAR < 0.50:
                if X['Moisture'] >= 95:
                ##If (PCNT_SOLAR < Pcnt_Broken AND RELATIVE_HUMIDITY > 95) SOW = 4 (Fog)
                    Weathercode_2 = 4
                ##If (PCNT_SOLAR < Pcnt_Broken) SOW = 3 (Overcast, > 9/10 cloud cover)
                else:
                    Weathercode_2 = 3


        if X['SolarRad'] < 25:
            X['State of Weather'] = 3
        else:
            ##If (PCNT_SOLAR >= Pcnt_Clear) SOW = 0 (Clear, >90% SR)
            if PCNT_SOLAR >= 0.90:
                X['State of Weather'] = 0
            ##1---Scattered:
            elif 0.50 <= PCNT_SOLAR < 0.90:
                X['State of Weather'] = 1
            ##2---Broken:
            elif  0.10 <= PCNT_SOLAR < 0.50:
                X['State of Weather'] = 2
            ##3---Overcast:
            elif PCNT_SOLAR < 0.10:
                if X['Moisture'] >= 95:
                ##If (PCNT_SOLAR < Pcnt_Broken AND RELATIVE_HUMIDITY > 95) SOW = 4 (Fog)
                    X['State of Weather'] = 4
                ##If (PCNT_SOLAR < Pcnt_Broken) SOW = 3 (Overcast, > 9/10 cloud cover)
                else:
                    X['State of Weather'] = 3

    ##If precipitation for the current hour bigger than 0.1, determine the SOW based on rain amount
    else:
        ##if temprature is below 32F there will be snow
        if X['Temp'] < 32:
            X['State of Weather'] = 7
        else:
            ## 1HR_Drizzle (inches): 0.1
            #08092017 Need to be corrected to use the current hour precipitation not the 24 hour total!!!
            if 0.1 < CurrentHourPrecip <= 0.15:
                X['State of Weather'] = 5
            ## 1HR_Rain (inches): 0.15
            elif 0.15 < CurrentHourPrecip <= 0.5:
                X['State of Weather'] = 6
            ## 1HR_Showers (inches):0.5
            elif 0.5 < CurrentHourPrecip <= 0.8:
                X['State of Weather'] = 8
            ##Thunderstorm
            elif CurrentHourPrecip > 0.8:
                X['State of Weather'] = 9

        #print X['PrecipAmt'], X['State of Weather']

    print X['State of Weather'],Weathercode, Weathercode_2

    return X

    ##6---Rain
    ##7---Snow
    ##8---Shower
    ##9---Thunderstorm
'''
SOW Thresholds for each station from station catalog (WIMSTEST)
All 4 stations have the same thresholds for
        1HR_Drizzle (inches): 0.1
        1HR_Rain (inches): 0.15
        1HR_Showers (inches):0.5
'''
##    elif not wxstring.find('TS') ==-1 :
##        X['State of Weather'] = 9
##    elif not wxstring.find('SH') ==-1 :
##        X['State of Weather'] = 8
##    elif not wxstring.find('SN') ==-1 :
##        X['State of Weather'] = 7
##    elif not wxstring.find('RA') ==-1 :
##        #How to determin the rain code? bigger than 0.1,
##        if X['PrecipAmt'] >= 0.1:
##            X['State of Weather'] = 6
##    elif not wxstring.find('DZ') ==-1 :
##        #How to determin the drizzle code? bigger than 0.01
##        if X['PrecipAmt'] >= 0.01:
##            X['State of Weather'] = 5
##    elif not wxstring.find('FG') ==-1 :
##        X['State of Weather'] = 4
##    #elif not wxstring.find('HZ') ==-1 :
##    #    X['State of Weather'] = 4
##    elif not wxstring.find('BR') ==-1 :
##        X['State of Weather'] = 4
##    elif 'OVC' in skycover:
##        X['State of Weather'] = 3
##    elif 'BKN' in skycover:
##        X['State of Weather'] = 2
##    elif 'SCT' in skycover or 'FEW' in skycover:
##        X['State of Weather'] = 1
##    elif 'CLR' in skycover or 'SKC' in skycover:
##        X['State of Weather'] = 0
    #print 'State of Weather is',X['State of Weather']
    #######################################################
    #Updated 09-28-2015
    #Per Discussion with Mike and Brad on 09/24/2015
    #The wet flag will be always be set to 'N' because human intervention will be needed for the determination
##    if X['State of Weather'] == 5 or X['State of Weather'] == 6 or X['State of Weather'] == 7:
##        X['WetFlag']= 'Y'
##    #If the SOW is 8 (showers) or 9 (thunderstorms) and the station of interest reported any precipitation in the past hour, set the Wet Flag to Y.
##    elif X['State of Weather'] == 8 or X['State of Weather'] == 9:
##        if row['precip_duration'] == 1:
##            X['WetFlag']= 'Y'
##    else:
##        X['WetFlag']= 'N'



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


# Define a function for groupped df to get the last 10 minutes's average
def Last10Min(dfgrouped):
    #print dfgrouped[-10:]
    last10MinutesWind = dfgrouped[-10:].mean()
    return last10MinutesWind


def f(x): return x[-10:].mean()
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
    df.loc[:,'SolarRadiationMedian'] = df.loc[:,'solar_radiation_set_1'].groupby(df['Date_Hour']).transform('median')
    #get the last 15 minutes record in the hour
##    df.loc[:,'EndHourSolarRadiation'] = df.loc[:,'solar_radiation_set_1'].groupby(df['Date_Hour']).filter(lambda x:np.mean([x[col] > 100 for col in goalsScoredDF.columns]))
##    goalsScoredDF.groupby(level='Month').filter(lambda x:np.all([x[col] > 100 for col in goalsScoredDF.columns]))

    # for name,df in df.loc[:,'solar_radiation_set_1'].groupby(df['Date_Hour']):
    #     #look at the different statistics of the hourly value
    #     print name,'HourlyMean: ', df.mean(),'HourlyMedian: ', df.median(),'last30MeanValue: ',df[-30:].mean(),'last15MeanValue: ',df[-15:].mean(),'last10MeanValue: ',df[-10:].mean(),'last5MeanValue: ',df[-5:].mean(),'LastRecord: ',df[-1:]
        #print name,'HourlyMedian: ', df.median()
    #not sure why the apply function is not worked as expected!forget about it
    #df.loc[:, 'WindSpeed1'] = df.loc[:, 'wind_speed_set_1'].groupby(df['Date_Hour']).apply(lambda gp: gp.apply(mean))

    df.loc[:, 'WindSpeed'] = df.loc[:, 'wind_speed_set_1'].groupby(df['Date_Hour']).transform(Last10Min)
    # df.loc[:, 'WindSpeed1'] = df.loc[:, 'wind_speed_set_1'].groupby(df['Date_Hour']).Apply(f)
    print df
    # for name, df in df.loc[:, 'wind_speed_set_1'].groupby(df['Date_Hour']):
    #     #print name, 'HourlyMean: ', df.mean(), 'HourlyMedian: ', df.median(), 'last30MeanValue: ', df[-30:].mean(), 'last15MeanValue: ', df[-15:].mean(), 'last10MeanValue: ', df[-10:].mean(), 'last5MeanValue: ', df[-5:].mean(), 'LastRecord: ', df[-1:].mean()
    #     #look at the different statistics of the hourly value
    #     #print name, 'HourlyMean: ', df.mean(), 'HourlyMedian: ', df.median(), 'last30MeanValue: ', df[-30:].mean(), 'last15MeanValue: ', df[-15:].mean(), 'last10MeanValue: ', df[-10:].mean(), 'last5MeanValue: ', df[-5:].mean(), 'LastRecord: ', df[-1:].mean()
    #     last10Wind = df[-10:].mean()
    #df.loc[:, 'WindSpeed'] = last10Wind
"""
    for name, dfgrouped in df.loc[:, 'wind_speed_set_1'].groupby(df['Date_Hour']):
        #print name, 'HourlyMean: ', dfgrouped.mean(), 'HourlyMedian: ', dfgrouped.median(), 'last30MeanValue: ', dfgrouped[-30:].mean(), 'last15MeanValue: ', dfgrouped[-15:].mean(), 'last10MeanValue: ', dfgrouped[-10:].mean(), 'last5MeanValue: ', dfgrouped[-5:].mean(), 'LastRecord: ', dfgrouped[-1:].mean()
        last10MinutesWS = dfgrouped[-10:].mean()
        print name, last10MinutesWS
    df.loc[:, 'WindSpeed'] = last10MinutesWS

    for name, dfgrouped in df.loc[:, 'wind_direction_set_1'].groupby(df['Date_Hour']):
        #print name, 'HourlyMean: ', dfgrouped.mean(), 'HourlyMedian: ', dfgrouped.median(), 'last30MeanValue: ', dfgrouped[-30:].mean(), 'last15MeanValue: ', dfgrouped[-15:].mean(), 'last10MeanValue: ', dfgrouped[-10:].mean(), 'last5MeanValue: ', dfgrouped[-5:].mean(), 'LastRecord: ', dfgrouped[-1:].mean()
        last10MinutesWD = dfgrouped[-10:].mean()
        print name, last10MinutesWD
        #df.loc['name', 'WindDirection'] = last10MinutesWD
        #print df[[name, "wind_speed_set_1"]]
    #print df.loc[:, 'WindDirection']
    #df.loc[:, 'WindDirection'] = last10MinutesWD

    #df.loc[:, 'WindDirection'] = df.loc[:, 'wind_direction_set_1'].groupby(df['Date_Hour']).transform('mean')
    #using medium instead of mean
    #df.loc[:,'SolarRadiationMedian'] = df.loc[:,'solar_radiation_set_1'].groupby(df['Date_Hour']).transform('median')

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

    MESO = df[['Station_ID','Date_Time','Precipitation','SolarRadiation','EndHourSolarRadiation','SolarRadiationMedian','AirTemperature','RelativeHumidity',
               'MaxAirTemperature','MinAirTemperature','WindSpeed','WindDirection','WindGust','Date_Hour']]

    #Add a column for the local time zone
    #MESO.loc[:,'obs_time_local'] = MESO.loc[:,'Date_Time'].apply(UTC4LOCAL)
    MESO.loc[:,'obs_time_standard'] = MESO.loc[:,'Date_Time'].apply(UTC2STANDARD)

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
              'Type':'R', 'State of Weather':-999, 'Temp':0, 'Moisture':0,
              'WindDir':0, 'WindSpeed':0, '10hr Fuel':0, 'Tmax':-999,
              'Tmin':-999, 'RHmax':-999, 'RHmin':-999, 'PrecipDur':0,
              'PrecipAmt':0, 'WetFlag':'N', 'Herb':20, 'Shrub':15,
              'MoistType':2, 'MeasType':1, 'SeasonCode':3, 'SolarRad':0
            }
##    X13 = {'W13':'W13', 'Station Number':'000000', 'Ob Date':'YYYYMMDD', 'Ob Time':0,
##      'Type':'R', 'State of Weather':0, 'Temp':0, 'Moisture':0,
##      'WindDir':0, 'WindSpeed':0, '10hr Fuel':0, 'Tmax':-999,
##      'Tmin':-999, 'RHmax':-999, 'RHmin':-999, 'PrecipDur':0,
##      'PrecipAmt':0, 'WetFlag':'N', 'Herb':20, 'Shrub':15,
##      'MoistType':2, 'MeasType':1, 'SeasonCode':3, 'SolarRad':0,
##      'GustDir':0,'GustSpeed':0,'SnowFlag':'N' ##Updated 02/03/2016 for the new three parameters
##      ##According to Juan, the gust direction of the peak wind should be the same with the hourly wind direction
##    }

    #pass the station ID
    X9['Station Number'] = ID
    ##X13['Station Number'] = ID
    with open(fileWF9,'a') as F9:
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
            Report9(df,X9,currenthourdf)
            ##Write the records into a FW13 and FW9 format
            F9.write( FormatFW9( X9 ) +'\n' )
"""
############################################
#Setting the directories for data archiving
############################################
WorkSpace = os.getcwd()
print WorkSpace
MesonetArchive = os.path.join(WorkSpace, "CSV")
#FW13Archive  = os.path.join(WorkSpace, "FW13")
FW9Archive  = os.path.join(WorkSpace, "FW9")
LOGArchive  = os.path.join(WorkSpace, "LOG")

if not os.path.exists(MesonetArchive):
    os.makedirs(MesonetArchive)
##if not os.path.exists(FW13Archive):
##    os.makedirs(FW13Archive)
if not os.path.exists(FW9Archive):
    os.makedirs(FW9Archive)
if not os.path.exists(LOGArchive):
    os.makedirs(LOGArchive)

##A table for the 1300 hour historical maximum solar radiation hourly mean

srtable_Percentile = os.path.join(WorkSpace,'SRTable_byPercentile.csv')
SRtable_Percentile = pandas.read_csv(srtable_Percentile,sep=',')


srtable = os.path.join(WorkSpace,'SRTable.csv')
SRtable =  pandas.read_csv(srtable,sep=' ')

#SRtable=SRtable.set_index(['Month'])
#print SRtable.loc[6,'CNST2']

LOGfile = os.path.join(LOGArchive,"MESONET4WIMS.log")
#Set up a logger for logging the running information
logging.basicConfig(filename=LOGfile,
                    format='%(asctime)s   %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S%p',
                    filemode='w',
                    level=logging.INFO)

#define the fire weather file name
#fileWF13 = os.path.join(WorkSpace, "tx-asos.fw13")
fileWF9 = os.path.join(WorkSpace, "tx-mesonet.fw9")
#for each day first removing the existing file
##if os.path.isfile(fileWF13):
##    os.remove(fileWF13)
#for each day first removing the existing file
if os.path.isfile(fileWF9):
    os.remove(fileWF9)

START_UTC = "1900"
END_UTC = "1859"
#today = date(datetime.now())
# set up date information
today = datetime.datetime.today()
logging.info("Start MESONET processing for %s", today.strftime("%Y%m%d"))
#logging.info("Start ASOS processing for %s", datetime.now().strftime("%Y%m%d%H"))
UTC_now = datetime.datetime.utcnow()
UTCHOUR_now = int(UTC_now.strftime("%H"))

#How about using the UTC hour (should always be 19:00) to avoid day saving issue
if UTCHOUR_now >= 19:
## Process today's data
    TODATSTR = today.strftime("%Y%m%d")
    two_day = datetime.timedelta(days=2)
    Twodaybeforetoday = today - two_day
    TWODAYSTR = Twodaybeforetoday.strftime("%Y%m%d")

else:
    three_day = datetime.timedelta(days=3)
    one_day = datetime.timedelta(days=1)
    ThreeDay = today - three_day
    YesterDay = today - one_day
    TODATSTR = YesterDay.strftime("%Y%m%d")
    TWODAYSTR = ThreeDay.strftime("%Y%m%d")

StartTime  =  TWODAYSTR + START_UTC
EndTime = TODATSTR + END_UTC
print StartTime,EndTime

'''
The stations Mike requested are:
1.	6E Candian, Hemphill county, LDM ID: XCA1 MesoWestID: CNST2
2.	2ESE Gail, Borden county, LDM ID: XGGS MesoWestID: GGST2
3.	1SSE Jayton, Kent county, LDM ID: XJTS MesoWestID: JTST2
4.	3 NNW Quitaque, Briscoe county, LDM ID: XQU1 MesoWestID: CATT2
'''

##start_date = '201704011900'
##end_date   = '201704031900'
##
##start_date = '201704021900'
##end_date   = '201704041900'
##
###To check 5 years historical data
##start_date = '201204051900'
##end_date   = '201704051900'
###3 of 4 have 5 years data except Canadian
##
###To check 10 years back data
##start_date = '200704021900'
##end_date   = '200704041900'
#only GGST2 (Gail) had 10 years data
#The WIMS ID's: 418703 - Canadian, 419102 - Gail, 419003 - Jayton, 418903 - Quitaque.

# Stations = {'CNST2':418703,
#             'GGST2': 419102,
#             'JTST2':419003,
#             'CATT2':418903}
Stations = {'GCMT2': 419205}
##Station&ID = {'Candian':'CNST2',
##                'Gail': 'GGST2',
##                'Jayton':'JTST2',
##                'Quitaque':'CATT2'}
'''
name,station,stationid,coordinate_WIMSTest,coordinate_MesoWest
Candian,CNST2,418703,(35.9184193,-100.2845250),(35.91842,-100.28453)
Gail,GGST2,419102,(32.7550721,-101.4143443),(32.75508,-101.41439)
Jayton,JTST2,419003,(33.2324140,-100.5677755),(33.23241,-100.56778)
Quitaque,CATT2,418903,(34.4125276,-101.0683749),(34.41253,-101.06838)
'''
#Test：
# Found out on the maps that GAIL (419102,419101) are 1.5 mile distance
#                            Quitaque and CapRoack are 1 mile distance
#Stations = {'CATT2':418903}
#####Parameters for downloading Mesonet data using MesoWest API#####
MyToken = '994a7e628db34fc68503d44c447aaa6f'
base_url = 'http://api.mesowest.net/v2/stations/'
query_type = 'timeseries'
csv_format = '&output=csv'
units = '&units=precip|in,temp|F,speed|mph'
#######################################################################
##Loop through each station for downloading and processing Mesonet Data
for STATION,ID in Stations.items():
    try:
        print STATION,ID
        api_string = base_url + query_type + '?' + 'stid=' + STATION + '&start=' + StartTime + '&end=' + EndTime + '&token=' + MyToken + units + csv_format
        print api_string
        print 'Downloading ASOS data for Station: ' + STATION
        #filename = "%s-%s-MesoWest.csv"%(STATION,today.strftime("%Y%m%d%H%M"))
        filename = "%s-%s-%s.csv"%(STATION,StartTime,EndTime)
        print filename
        csvfile = os.path.join(MesonetArchive, filename)
        print csvfile
        urllib.urlretrieve(api_string,csvfile)
        logging.info("Downloaded MESONET source dat for %s", STATION)
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

'''
#Archive the WF9 file for each day
archivefileWF9 = today.strftime("%Y%m%d") + ".fw9"
archivefileWF9 = os.path.join(FW9Archive, archivefileWF9)
shutil.copyfile(fileWF9,archivefileWF9)
#Archive the WF13 file for each day
##archivefileWF13 = today.strftime("%Y%m%d") + ".fw13"
##archivefileWF13 = os.path.join(FW13Archive, archivefileWF13)
##shutil.copyfile(fileWF13,archivefileWF13)
#Keep Record of the log file
archivefileLOG = today.strftime("%Y%m%d") + ".log"
archivefileLOG = os.path.join(LOGArchive, archivefileLOG)
shutil.copyfile(LOGfile,archivefileLOG)
'''
