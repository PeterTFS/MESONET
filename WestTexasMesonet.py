#------------------------------------------------------------------------------------
# Name:        WestTexasMesonet.py
# Purpose:     Introduce the 4 stations observation from West Texas Mesomet for NFDRS
#
# Author:      pyang
#
# Created:     04/03/2017
# Copyright:   (c) pyang 2017
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import pandas as pd
from MesoPy import Meso #MesoPy from MesoWest (http://mesowest.utah.edu/) API
import datetime
MyToken = '994a7e628db34fc68503d44c447aaa6f'
#1) Time period for query (UTM)
#Use urrent date as the end and 48 hours before today's 13 hour to get the full records
#the 13 hour corrsponding UTM should be 1900
UTCHOUR = "1900"
#today = date(datetime.now())
# set up date information
today = datetime.datetime.today()
TODATSTR = today.strftime("%Y%m%d")
two_day = datetime.timedelta(days=2)
Twodaybeforetoday = today - two_day
TWODAYSTR = Twodaybeforetoday.strftime("%Y%m%d")
#logging.info("Start ASOS processing for %s", today.strftime("%Y%m%d"))
#logging.info("Start ASOS processing for %s", datetime.now().strftime("%Y%m%d%H"))

StartTime  =  TWODAYSTR + UTCHOUR
EndTime = DATESTR + UTCHOUR
print StartTime,EndTime

#For Test Only
StartTime  =  201704011955
EndTime = 201704012000

#For ASOS
StartTime = '201704011900'
EndTime   = '201704031900'

m = Meso(token=MyToken)
#q = m.timeseries(start=StartTime,end=EndTime,stid='CNST2')#,vars='wind_speed,pressure')
#print(type(q['STATION']))
#print type(q['STATION'])
##for l in q['STATION']:
##    for k,v in l.items():
##        print k,v
#meso_df = pd.DataFrame.from_dict(q,orient='columns')
#meso_df
#print m.variables()
'''
SENSOR_VARIABLES {u'wind_speed': {u'wind_speed_set_1': {u'position': u''}},
u'date_time': {u'date_time': {}},
u'solar_radiation': {u'solar_radiation_set_1': {u'position': u''}},
u'wind_gust': {u'wind_gust_set_1': {u'position': u''}},
u'pressure': {u'pressure_set_1': {u'position': u''}},
u'precip_accum_one_minute': {u'precip_accum_one_minute_set_1': {u'position': u''}},
u'wind_direction': {u'wind_direction_set_1': {u'position': u''}},
u'wind_chill': {u'wind_chill_set_1d': {u'derived_from': [u'air_temp_set_1', u'wind_speed_set_1']}},
u'wind_cardinal_direction': {u'wind_cardinal_direction_set_1d': {u'derived_from': [u'wind_direction_set_1']}},
u'relative_humidity': {u'relative_humidity_set_1': {u'position': u''}},
u'sea_level_pressure': {u'sea_level_pressure_set_1d': {u'derived_from': [u'pressure_set_1', u'air_temp_set_1', u'relative_humidity_set_1']}},
u'air_temp': {u'air_temp_set_1': {u'position': u''}},
u'dew_point_temperature': {u'dew_point_temperature_set_1d': {u'derived_from': [u'air_temp_set_1', u'relative_humidity_set_1']}},
u'altimeter': {u'altimeter_set_1d': {u'derived_from': [u'pressure_set_1']}}}
'''
q_asos = m.timeseries(start=StartTime,end=EndTime,stid='KCLL')#,vars='wind_speed,pressure')
#print type(q_asos['STATION'])
print q_asos['STATION'][0]['SENSOR_VARIABLES']