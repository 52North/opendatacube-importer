import os
from anthroprotect import AnthroprotectLoader

#
# Open Data Cube configuration
#
DATACUBE_CONF = os.getenv('DATACUBE_CONF', 'datacube.conf')

#
# Folders
# - GLOBAL_DATA_FOLDER: parent folder where data and metadata (odc yaml files) from all datasets are stored
#
# ToDo: create folder where only metadata (odc yaml files) are stored?
GLOBAL_DATA_FOLDER = os.getenv('GLOBAL_DATA_FOLDER', '/DATA')

#
# AnthroProtect dataset (http://rs.ipb.uni-bonn.de/data/anthroprotect/)
# Dataset size: 19,5 GB (anthroprotect.zip) -> 48,7 GB (unzipped)
#
ANTHROPROTECT_ENABLED = os.getenv('ANTHROPROTECT_ENABLED', False)

#
# Datasets to be added to Open Data Cube
# tuples of dataset descriptor and loader class
#
DATASETS = []
if ANTHROPROTECT_ENABLED:
    DATASETS = DATASETS + ('anthroprotect', AnthroprotectLoader)
