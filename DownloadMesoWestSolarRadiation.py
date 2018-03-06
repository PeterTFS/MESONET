##start_date = '200704021900'
##end_date   = '200704041900'
MyToken = '994a7e628db34fc68503d44c447aaa6f'
base_url = 'http://api.mesowest.net/v2/stations/'
query_type = 'timeseries'
csv_format = '&output=csv'
Variable = 'solar_radiation'
units = '&units=precip|in,temp|F,speed|mph'
#######################################################################
##Loop through each station for downloading and processing Mesonet Data
#http://api.mesowest.net/v2/stations/timeseries?state=dc&start=201307010000&end=201307020000&vars=air_temp,pressure&obtimezone=local&token=demotoken

STATION='JTST2'
STATION='CATT2'
STATION = 'CNST2'
STATION='GGST2'
StartTime,EndTime = ('200701011900','201705301900')
#print STATION,ID
api_string = base_url + query_type + '?' + 'stid=' + STATION + '&start=' + StartTime + '&end=' + EndTime + '&vars=' + Variable + '&token=' + MyToken + units + csv_format
#&obtimezone=local&token=demotoken
#http://api.mesowest.net/v2/stations/timeseries?stid=GGST2&start=201705230000&end=201705261100&vars=solar_radiation&obtimezone=local&token=994a7e628db34fc68503d44c447aaa6f&units=precip|in,temp|F,speed|mph&output=csv
print api_string
print 'Downloading ASOS data for Station: ' + STATION
#filename = "%s-%s-MesoWest.csv"%(STATION,today.strftime("%Y%m%d%H%M"))
filename = "%s-%s-%s_10year.csv"%(STATION,StartTime,EndTime)
print filename
csvfile = os.path.join(MesonetArchive, filename)
print csvfile
urllib.urlretrieve(api_string,csvfile)