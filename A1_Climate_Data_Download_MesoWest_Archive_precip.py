from datetime import datetime, timedelta
import os
import urllib
import json
from MesoPy import Meso
import csv
import re

Workspace = os.getcwd()
Workspace = r"C:\\DEV\\MesoNet"
print(Workspace)
Climate_Archive = os.path.join(Workspace, "Inputs", "NWS_Station_Data", "Climate_Obs")

m = Meso(token='994a7e628db34fc68503d44c447aaa6f')

# Set up date stucture for retrieving 7 days of data
today = datetime.today()
one_day = timedelta(days=1)
sixty_day = timedelta(days=60)

#yesterday = today - one_day

time = "1200"

#startdate = yesterday.strftime("%Y%m%d")
#enddate = today.strftime("%Y%m%d")

# Observation period begins on the beginning (today) and ends on the endday
# The days count back from today to the date specified below
endday = today
beginning = endday.strftime("%Y-%m-%d")

startday = endday - one_day
startday = endday - sixty_day


days = 0
# Change the date below to whatever date you want the process to end
while beginning != "2017-10-17":
    
    startdate = startday.strftime("%Y%m%d")
    enddate = endday.strftime("%Y%m%d")

    start = startdate + time
    end = enddate + time

    print('start:',start,'end:',end)
    
    # Set to endday for now, but need to only check once in 7 day run
    comparedate = today.strftime("%Y-%m-%d")

    archiveyear = startday.strftime("%Y")

    # Step back start and end days by one day
    endday -= one_day
    startday -= one_day

    beginning = endday.strftime("%Y-%m-%d")
    previousDay = startday.strftime("%Y%m%d")

    # Increment one day
    days += 1

    Climate_archiveFolder = os.path.join(Climate_Archive, archiveyear)

    if not os.path.exists(Climate_archiveFolder):
        os.makedirs(Climate_archiveFolder)

    observations = os.path.join(Climate_archiveFolder, "nws_obs_" + startdate + ".csv")
    
    observationsPre = os.path.join(
        Climate_archiveFolder, "nws_obs_" + previousDay + ".csv")
    # print(observations, observationsPre)

    # Added 'KGTU'(Georgetown) and 'KHYI' (San Marcos) 
    kbdistations = ['KABI','KALI','KAMA','KGKY','KAUS','KATT','KBPT','KBRO','KBMQ','KCDS','KCLL','KCXO',
                    'KCRP','KCRS','KCOT','KDHT','KDAL','KDRT','KDTO','KDFW','K6R6','KELP','KFST','KAFW','KFTW',
                    'KGLS','KGDP','KHRL','KHDO','KHOU','KUTS','KJCT','KNQI','KLRD','KGGG','KLBB','KLFK','KMRF',
                    'KMFE','KTKI','KMAF','KMWL','KNGP','KBAZ','KODO','KPSX','KPRX','KRKP','KSJT','KSAT','KTRL',
                    'KTYR','KVCT','KACT','KSPS','KINK','KSHV','KTXK','KIAH', 'KGTU', 'KHYI']

    stationsDict = {'KMRF': {'STID': 'KMRF', 'NAME': 'MARFA'}, 'KODO': {'STID': 'KODO', 'NAME': 'ODESSA'},
                    'KSJT': {'STID': 'KSJT', 'NAME': 'SAN ANGELO'}, 'KCRP': {'STID': 'KCRP', 'NAME': 'CORPUS CHRISTI'},
                    'KALI': {'STID': 'KALI', 'NAME': 'ALICE'}, 'KNGP': {'STID': 'KNGP', 'NAME': 'NAVY CORPUS'},
                    'KCRS': {'STID': 'KCRS', 'NAME': 'CORSICANA'}, 'KATT': {'STID': 'KATT', 'NAME': 'AUSTIN MABRY'},
                    'KDAL': {'STID': 'KDAL', 'NAME': 'DALLAS LOVE FIELD'},'KINK': {'STID': 'KINK', 'NAME': 'WINK'},
                    'KMWL': {'STID': 'KMWL', 'NAME': 'MINERAL WELLS'}, 'KTKI': {'STID': 'KTKI', 'NAME': 'MCKINNEY'},
                    'KJCT': {'STID': 'KJCT', 'NAME': 'JUNCTION'}, 'KTYR': {'STID': 'KTYR', 'NAME': 'TYLER'},
                    'KHDO': {'STID': 'KHDO', 'NAME': 'HONDO'}, 'KPSX': {'STID': 'KPSX', 'NAME': 'PALACIOS'},
                    'KSPS': {'STID': 'KSPS', 'NAME': 'WICHITA FALLS'}, 'KFST': {'STID': 'KFST', 'NAME': 'FORT STOCKTON'},
                    'KBAZ': {'STID': 'KBAZ', 'NAME': 'NEW BRAUNFELS'}, 'KDRT': {'STID': 'KDRT', 'NAME': 'DEL RIO'},
                    'KCOT': {'STID': 'KCOT', 'NAME': 'COTULLA'}, 'KGLS': {'STID': 'KGLS', 'NAME': 'GALVESTON'},
                    'KCXO': {'STID': 'KCXO', 'NAME': 'CONROE'}, 'KGDP': {'STID': 'KGDP', 'NAME': 'GUADALUPE PASS'},
                    'KBMQ': {'STID': 'KBMQ', 'NAME': 'BURNET'}, 'KBRO': {'STID': 'KBRO', 'NAME': 'BROWNSVILLE'},
                    'KBPT': {'STID': 'KBPT', 'NAME': 'BEAUMONT'}, 'KAMA': {'STID': 'KAMA', 'NAME': 'AMARILLO'},
                    'KRKP': {'STID': 'KRKP', 'NAME': 'ROCKPORT'}, 'KGKY': {'STID': 'KGKY', 'NAME': 'ARLINGTON'},
                    'KMFE': {'STID': 'KMFE', 'NAME': 'MCALLEN'}, 'KAUS': {'STID': 'KAUS', 'NAME': 'AUSTIN BERGSTROM'},
                    'KLBB': {'STID': 'KLBB', 'NAME': 'LUBBOCK'}, 'KLFK': {'STID': 'KLFK', 'NAME': 'LUFKIN'},
                    'KHOU': {'STID': 'KHOU', 'NAME': 'HOUSTON HOBBY'}, 'KMAF': {'STID': 'KMAF', 'NAME': 'MIDLAND'},
                    'KSAT': {'STID': 'KSAT', 'NAME': 'SAN ANTONIO'}, 'KACT': {'STID': 'KACT', 'NAME': 'WACO'},
                    'KNQI': {'STID': 'KNQI', 'NAME': 'KINGSVILLE'}, 'KUTS': {'STID': 'KUTS', 'NAME': 'HUNTSVILLE'},
                    'KGGG': {'STID': 'KGGG', 'NAME': 'LONGVIEW'}, 'KDTO': {'STID': 'KDTO', 'NAME': 'DENTON'},
                    'KPRX': {'STID': 'KPRX', 'NAME': 'PARIS'}, 'KAFW': {'STID': 'KAFW', 'NAME': 'FORT WORTH ALLIANCE'},
                    'KVCT': {'STID': 'KVCT', 'NAME': 'VICTORIA'}, 'KDHT': {'STID': 'KDHT', 'NAME': 'DALHART'},
                    'KDFW': {'STID': 'KDFW', 'NAME': 'DFW AIRPORT'}, 'KABI': {'STID': 'KABI', 'NAME': 'ABILENE'},
                    'KTRL': {'STID': 'KTRL', 'NAME': 'TERRELL'}, 'KFTW': {'STID': 'KFTW', 'NAME': 'FORT WORTH MEACHAM'},
                    'KCDS': {'STID': 'KCDS', 'NAME': 'CHILDRESS'}, 'KHRL': {'STID': 'KHRL', 'NAME': 'HARLINGEN'},
                    'KELP': {'STID': 'KELP', 'NAME': 'EL PASO'}, 'KCLL': {'STID': 'KCLL', 'NAME': 'COLLEGE STATION'},
                    'K6R6': {'STID': 'K6R6', 'NAME': 'DRYDEN'}, 'KLRD': {'STID': 'KLRD', 'NAME': 'LAREDO'},
                    'KSHV': {'STID': 'KSHV', 'NAME': 'SHREVEPORT'},'KTXK': {'STID': 'KTXK', 'NAME': 'TEXARKANA'},
                    'KGTU': {'STID': 'KGTU', 'NAME': 'GEORGETOWN'}, 'KIAH': {'STID': 'KIAH', 'NAME': 'HOUSTON INTRCTNL'},
                    'KHYI': {'STID': 'KHYI', 'NAME': 'SAN MARCOS'}}
    kbdistations = ['CDDT2']
    #kbdistations = ['KABI']
    stationsDict = {'CDDT2': {'STID': 'CDDT2', 'NAME': 'CADDO'}}
    #stationsDict = {'KABI': {'STID': 'KABI', 'NAME': 'ABILENE'}}

    print len(kbdistations)

    # Set default values for dictionary entries. These will later be overwritten if 
    for station in stationsDict:
        stationsDict[station]['RECENT_OBS'] = -99
        stationsDict[station]['MX_TEMP'] = -99
        stationsDict[station]['MW_PCP'] = 0
        stationsDict[station]['LONGITUDE'] = 0
        stationsDict[station]['LATITUDE'] = 0
        
    # meta data request to determine if station is current
    mwmetadata = m.metadata(stid=kbdistations, start=start, end=end)

    # precipitation query will return total precipitation over the last 24 hours
    start, end = '201712111800','201712121200'
    mwprecipdata = m.precip(stid=kbdistations, start=start, end=end, units='precip|in')
    #precip = m.precip(stid='CDDT2', start='201709261800',end='201711271200', units='precip|in')
    #print(mwprecipdata)

    # temperature query will return all observations over the last 24 hours
    # Add vars for air_temp_high_6_hour
    
    mwtempdata = m.timeseries(stid=kbdistations, start=start, end=end,
                                  vars='air_temp,air_temp_high_6_hour',
                                  units='temp|F,precip|in')           

    for ob in mwmetadata['STATION']:
        recentob = ob['PERIOD_OF_RECORD']['end'][:10]
        obid = ob['STID']
        stationsDict[obid]['RECENT_OBS'] = ob['PERIOD_OF_RECORD']['end']
        stationsDict[obid]['LONGITUDE'] = ob['LONGITUDE']
        stationsDict[obid]['LATITUDE'] = ob['LATITUDE']

        if recentob == comparedate:
            stationsDict[obid]['RECENT_OBS'] = "CURRENT"
            #print obid + " is current"
            
        #else:
            #print obid + " was last current " + ob['PERIOD_OF_RECORD']['end']


    for ob in mwprecipdata['STATION']:
        if 'total_precip_value_1' in ob['OBSERVATIONS']:
            obid = ob['STID']
            if "E" in str(ob['OBSERVATIONS']['total_precip_value_1']):
                stationsDict[obid]['MW_PCP'] = ob['OBSERVATIONS']['total_precip_value_1'][2:]
            elif ob['OBSERVATIONS']['total_precip_value_1'] == "T 0":
                stationsDict[obid]['MW_PCP'] = 0.0
            # Try to catch any unexpected tags in data and add to log file
            elif re.search('[a-zA-Z]', str(ob['OBSERVATIONS']['total_precip_value_1'])):
                print str(ob['OBSERVATIONS']['total_precip_value_1'])
            # Otherwise, just add the value to the dictionary
            else:
                stationsDict[obid]['MW_PCP'] = ob['OBSERVATIONS']['total_precip_value_1']
                
    for ob in mwtempdata['STATION']:
        if 'air_temp_set_1' in ob['OBSERVATIONS']:
            obid = ob['STID']
            #obCount = len(ob['OBSERVATIONS']['air_temp_set_1'])
            TempobCount = 0
            for obs in ob['OBSERVATIONS']['air_temp_set_1']:
                if obs is not None:
                    TempobCount += 1
                    
            print obid, len(ob['OBSERVATIONS']['air_temp_set_1']), TempobCount

            # if TempobCount >= 18:
            #     # If fewer than 18 observation, MX_TEMP may not be accurately captured
            #     # 18/24 = 75%, so hopefully the true max or close is in that group
            #     stationsDict[obid]['MX_TEMP'] = max(ob['OBSERVATIONS']['air_temp_set_1'])
            #     if 'air_temp_high_6_hour_set_1' in ob['OBSERVATIONS']:
            #         Max6HourTemp = max(ob['OBSERVATIONS']['air_temp_high_6_hour_set_1'])
            #     if stationsDict[obid]['MX_TEMP'] > Max6HourTemp:
            #         stationsDict[obid]['MX_TEMP'] = Max6HourTemp
                    
    #for station in stationsDict:
        #print stationsDict[station]['STID'], stationsDict[station]['NAME'], stationsDict[station]['RECENT_OBS'], stationsDict[station]['MX_TEMP'], stationsDict[station]['MW_PCP']

    ##need to open the precious day's archive for getting today's NoRainDays record
    prcpAmt = 0
    noRainDays = 0
    #if exists(yesterdayObs):
    if os.path.exists(observationsPre):
        print(observationsPre)
        with open(observationsPre, 'r +') as yesterdayObs:
            rawReader = csv.reader(yesterdayObs, delimiter=',')
            rawReader.next()
            for row in rawReader:
                prcpAmt = row[5]
                noRainDays = row [6]
                print(prcpAmt, noRainDays)
            # shrubGreennessF[int(row[6])] = int(row[7])
            # herbaceousGreennessF[int(row[6])] = int(row[8])
            # seasonCode[int(row[6])] = int(row[9])
    #previousDay =     
    header = ("STID", "NAME", "LONG", "LAT", "MX_TEMP", "PRECIP","NoRainDays")
    #Here need to get the precious day's record for getting the NoRainDays number 
    #if today's precipitation is bigger than the threshold (e.g. 0.25 in), the NoRainDays should be set to 0,
    #Otherwise, get the NoRainDays number and add it 1 
    with open(observations, 'wb') as climateobs:
        writer = csv.writer(climateobs, delimiter = ',')
        writer.writerow(header)
        for row in stationsDict:
            stid = stationsDict[row]['STID']
            name = stationsDict[row]['NAME']            
            longitude = stationsDict[row]['LONGITUDE']
            latitude = stationsDict[row]['LATITUDE']
            tmp = stationsDict[row]['MX_TEMP']
            pcp = stationsDict[row]['MW_PCP']
            norainday = 0
            if(pcp >= 0.25):
                norainday = 0
            else:
                print(noRainDays,norainday,pcp)
                norainday = int(noRainDays) +1
            
            rows = (stid, name, longitude, latitude, tmp, pcp, norainday)
            #print(pcp, norainday)
            #writer.writerow(rows)

    
    
