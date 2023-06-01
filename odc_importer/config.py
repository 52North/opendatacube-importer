import os
from anthroprotect import AnthroprotectLoader
from loaders.cmems_waves import CmemsWavesLoader
from loaders.cmems_physics import CmemsPhysicsLoader
from loaders.cmems_currents import CmemsCurrentLoader
from loaders.gfs_weather import GfsWeatherLoader
#
# Open Data Cube configuration
#
DATACUBE_CONF = os.getenv('DATACUBE_CONF', 'datacube.conf')

#
# Folders
# - BASE_FOLDER: base folder as absolute path (expected to be the parent of DATA_FOLDER)
# - DATA_FOLDER: parent folder (relative to BASE_FOLDER) where data and metadata (odc yaml files)
#                from all data sources are stored
#
# ToDo: create folder where only metadata (odc yaml files) are stored?
BASE_FOLDER = os.getenv('BASE_FOLDER', '/odc')
DATA_FOLDER = os.getenv('DATA_FOLDER', 'data')

#
# AnthroProtect data source (http://rs.ipb.uni-bonn.de/data/anthroprotect/)
# Data source size: 19,5 GB (anthroprotect.zip) -> 48,7 GB (unzipped)
#
ANTHROPROTECT_ENABLED = os.getenv('ANTHROPROTECT_ENABLED', False)

#
# Data sources to be added to Open Data Cube
# tuples of data source descriptor and loader class
#
DATASOURCES = []
if ANTHROPROTECT_ENABLED:
    DATASOURCES.append(('anthroprotect', AnthroprotectLoader))
    
# Check if CMEMS Waves data source is enabled
CMEMS_WAVES_ENABLED = os.getenv('CMEMS_WAVES_ENABLED', True)
if CMEMS_WAVES_ENABLED:
    DATASOURCES.append(('cmems_waves', CmemsWavesLoader))

# Check if CMEMS Physics data source is enabled
CMEMS_PHYSICS_ENABLED = os.getenv('CMEMS_PHYSICS_ENABLED', True)
if CMEMS_PHYSICS_ENABLED:
    DATASOURCES.append(('cmems_physics', CmemsPhysicsLoader))

# Check if CMEMS Current data source is enabled
CMEMS_CURRENT_ENABLED = os.getenv('CMEMS_CURRENT_ENABLED', True)
if CMEMS_CURRENT_ENABLED:
    DATASOURCES.append(('cmems_current', CmemsCurrentLoader))

# Check if GFS Weather data source is enabled
GFS_WEATHER_ENABLED = os.getenv('GFS_WEATHER_ENABLED', True)
if GFS_WEATHER_ENABLED:
    DATASOURCES.append(('gfs_weather', GfsWeatherLoader))
    
print(DATASOURCES)