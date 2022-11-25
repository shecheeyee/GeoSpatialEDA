# pip install the additional dependencies
#!pip install pyarrow
#!pip install fastparquet
#!pip install geopandas

# import libraries
import pandas as pd
import glob
import matplotlib.pyplot as plt
import seaborn as sns
import geopandas as gpd
from bs4 import BeautifulSoup
import sys
import datetime

# import dask
import dask
import dask.dataframe as dd

# import fiona and activate KML usage
import fiona
fiona.drvsupport.supported_drivers['kml'] = 'rw' # enable KML support which is disabled by default
fiona.drvsupport.supported_drivers['KML'] = 'rw' # enable KML support which is disabled by default

# glob glob glob
sg_files = glob.glob("/content/drive/MyDrive/grab-posis-city=Singapore/city=Singapore/*parquet")
sg_files

# read file using .read_parquet
df_0 = pd.read_parquet(sg_files[0])
df_0

# combine the parquets
df_list = []

for sg_par in sg_files:
  df_list.append(pd.read_parquet(sg_par))

df = pd.concat(df_list, ignore_index = True)

#formatting of datetime
def format_datetime(df, col_name):
    # get datetime obj for all timestamps
    dt = df[col_name].apply(datetime.datetime.fromtimestamp)
    
    df["time"] = dt.apply(lambda x: x.time())
    df["day_of_week"] = dt.apply(lambda x: x.weekday())
    df["month"] = dt.apply(lambda x: x.month)
    df["year"] = dt.apply(lambda x: x.year)

# convert datatypes in DataFrame to different types 
df['trj_id'] = df['trj_id'].astype('category')
df['driving_mode'] = df['driving_mode'].astype('category')
df['osname'] = df['osname'].astype('category')
df['pingtimestamp'] = df['pingtimestamp'].astype('int32')
df['rawlat'] = df['rawlat'].astype('float32')
df['rawlng'] = df['rawlng'].astype('float32')
df['speed'] = df['speed'].astype('float32')
df['bearing'] = df['bearing'].astype('int32')
df['accuracy'] = df['accuracy'].astype('float32')

# earlier, we imported dask.dataframe as dd
ddf_0 = dd.read_parquet(sg_files[0])
#print(ddf_0)

ddf_list = []

for sg_par in sg_files:
  ddf_list.append(dd.read_parquet(sg_par))

ddf = dd.concat(ddf_list)
#print(ddf)

#usual ipython objects
ipython_vars = ['In', 'Out', 'exit', 'quit', 'get_ipython', 'ipython_vars']

# Get a sorted list of the objects and their sizes
sorted([(x, sys.getsizeof(globals().get(x))) for x in dir() if not x.startswith('_') and x not in sys.modules and x not in ipython_vars], key=lambda x: x[1], reverse=True)

#sort the DataFrame based on trajectory ID and timestamp
df_sorted = df.sort_values(['trj_id', 'pingtimestamp'])
#print(df_sorted)

#histogram visualization
plt.hist(df_sorted[df_sorted['speed']< 40]['speed'])

df_sorted['speed'].median
df_test = df_sorted[['trj_id','osname','rawlat','rawlng','accuracy','speed']]
df_inaccurate = df_test[df_test['accuracy']>1000]

len(df_inaccurate)

#visualizing the difference between android and IOS
sns.boxplot(data = test_df, x = 'osname', y = 'speed', showfliers = False)

#dataframe
gdp_test = gpd.GeoDataFrame(df_inaccurate, geometry=gpd.points_from_xy(df_inaccurate.rawlng, df_inaccurate.rawlat))

#visualization of gdp plott
gdp_test.plot()

#getting sg map to overlay 
sg_map = gpd.read_file('/content/drive/MyDrive/national-map-line/national-map-line-geojson.geojson')

sg_map.plot()

#using BS4 to parse
temp_soup = BeautifulSoup(sg_map['Description'][0])

#print(temp_soup.prettify())

temp_soup.find_all('tr')

# test print out everything in the row
for row in temp_soup.find_all('tr')[1:]:
  print(row.td.text.strip())

#getting the individual values
map_name = []
folderpath = []
symbolid = []

for location in sg_map['Description']:
  temp_soup = BeautifulSoup(location)
  temp_tr = temp_soup.find_all('tr')

  map_name.append(temp_tr[1].td.text.strip())
  folderpath.append(temp_tr[2].td.text.strip())
  symbolid.append(temp_tr[3].td.text.strip())

# declare new columns
sg_map['name'] = map_name
sg_map['folderpath'] = folderpath
sg_map['symbolid'] = symbolid

 #plot the different data types on the map
sg_map[sg_map['symbolid'] == '3'].plot()

gdp_test.plot()

sg_map[sg_map['symbolid'] == '3'].plot()
gdp_test.plot()

#plot with sgmap, coloured roads and points
fig, ax = plt.subplots(figsize = (16, 9))
sg_map[sg_map['symbolid'] == '1'].plot(column = 'name', ax = ax, cmap = 'Set1', legend = True)
gdp_test.plot(ax = ax, color = 'black')