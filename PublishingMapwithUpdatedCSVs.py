
# coding: utf-8

# <div class="alert alert-info">
# 
# **Note:** Refer here for instructions to <a href="https://developers.arcgis.com/python/sample-notebooks/#Download-and-run-the-sample-notebooks">download and run this sample locally</a> on your computer
# 
# </div>

# # Publishing SDs, Shapefiles and CSVs
# 
# Publishing your data can be accomplished in two simple steps:
# 1. Add the local data as an item to the portal
# 2. Call the publish() method on the item
# 
# This sample notebook shows how different types of GIS datasets can be added to the GIS, and published as web layers.

# In[1]:


#from IPython.display import display

#from arcgis.gis import GIS
import arcgis
print(arcgis.__version__)
#import os
gis = GIS("https://www.arcgis.com", "pyang_tamu", "TfS@2017")
#gis = GIS("https://www.arcgis.com", "pyang@tfs.tamu.edu", "TfS#2017")
user = gis.users.me
print(user)

# # Publish a CSV file initially
#csv_file = 'data/Chennai_precipitation.csv''
# import pandas
# csv_file = r"C:\DEV\MesoNet\Weather_Stations_NoRainDays.csv"
# stations_csv = pandas.read_csv(csv_file)
# stations_csv.head()
# item_prop = {'title':'Stations with no rain days'}
# csv_item = gis.content.add(item_properties=item_prop, data=csv_file)
# display(csv_item)
# cities_item = csv_item.publish()

#Get the hosted feature set 
search_result = gis.content.search("title:Stations_with_no_rain_days", item_type = "Feature Layer")
existing_noRainMap = search_result[0]
#display(existing_noRainMap)
print(existing_noRainMap.url)

# ## Overwrite the feature layer
# Let us overwrite the feature layer using the new csv file we just created. To overwrite, we will use the `overwrite()` method.
# In[32]:
from arcgis.features import FeatureLayerCollection
station_collection = FeatureLayerCollection.fromitem(existing_noRainMap)


# In[33]:


#call the overwrite() method which can be accessed using the manager property
csv2 = r"C:\DEV\MesoNet\Weather_Stations_NoRainDays_Update.csv"
print(station_collection.manager.overwrite(csv2))
