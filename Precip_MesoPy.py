# from MesoPy import Meso
# m = Meso(token='994a7e628db34fc68503d44c447aaa6f')

# #Station list file located in c:\DEV\Mesonet\Weather_Stations_91.csv

# precip = m.timeseries(stid='CDDT2', start='201712131200',
#                       end='201712141200', vars='precip_accum_24_hour,precip_accum', units='precip|in')
# #201712141200('start:', '201712131200', 'end:', '201712141200')
# # precip = m.precip(stid='CDDT2', start='201712131200',
# #                   end='201712141200', units='precip|in')
# precip_total = m.precip(stid='CDDT2', start='201709261800',
#                       end='201711271200', vars='precip_accum_24_hour,precip_accum', units='precip|in')

# # var = m.variables()
# # metadata = m.metadata(stid='KCLL')

# print precip

# remember we stored the dictionary in the precip variable
# station = precip_total['STATION'][0]['STID']
# totalPrecip = precip_total['STATION'][0]['OBSERVATIONS']['total_precip_value_1']
# print('The total accumulated precip at ' +
#       station + ' was ' + str(totalPrecip) + '"')

#it turned out that ASOS stations have one to 24 hour accummulation of precipitation 
#https://api.mesowest.net/v2/stations/timeseries?token=994a7e628db34fc68503d44c447aaa6f&stid=KCLL%20&start=201506010000&end=201506020000&vars=precip_accum_24_hour&output=csv
#but RAWS has the number to show the overall accummulation for the whole time
#https://api.mesowest.net/v2/stations/timeseries?token=994a7e628db34fc68503d44c447aaa6f&stid=CDDT2&start=201506010000&end=201506020000&vars=precip_accum&output=csv


Stations = {"KDHT": 418702,
            "KAMA": 418803,
            "KSPS": 419302,
            "KINK": 417501,
            "KFST": 417601,
            "KLBB": 419002,
            "KJCT": 417803,
            "KSJT": 419204,
            "KELP": 416901,
            "KDRT": 418003,
            "KHDO": 418103,
            "KSSF": 418104,
            "KCOT": 418402,
            "KALI": 418504,
            "KBAZ": 418105,
            "KCLL": 413901,
            #                "KCRS": 412001,
            "KTYR": 411701,
            "KTRL": 419703,
            "KDTO": 419603,
            "KMWL": 419404}

# base_url = 'http://api.mesowest.net/v2/stations/'
# query_type = 'timeseries'
# csv_format = '&output=csv'
# noHighFrequecy = '&hfmetars=0'

# Station list
#stid: string, optional Single or comma separated list of MesoWest station IDs. e.g. stid = 'kden,kslc,wbb'
stationsIDs = []
for key, value in Stations.items():
    stationsIDs.append(key)
# print key, value for key, value in Stations
# stationIDs = lambda s: s.key, Stations
# stationsIDs = {key, value for key, value in Stations}
print stationsIDs
