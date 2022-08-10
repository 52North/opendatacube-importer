import os
from anthroprotect import AnthroprotectLoader

#
# Open Data Cube configuration
#
DATACUBE_CONF = os.getenv('DATACUBE_CONF', 'datacube.conf')

#
# Folders
# - BASE_FOLDER: base folder as absolute path (expected to be the parent of DATA_FOLDER)
# - DATA_FOLDER: parent folder (relative to BASE_FOLDER) where data and metadata (odc yaml files)
#                from all datasets are stored
#
# ToDo: create folder where only metadata (odc yaml files) are stored?
BASE_FOLDER = os.getenv('BASE_FOLDER', '/odc')
DATA_FOLDER = os.getenv('DATA_FOLDER', 'DATA')

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
    DATASETS.append(('anthroprotect', AnthroprotectLoader))
