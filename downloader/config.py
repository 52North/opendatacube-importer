import os
from anthroprotect import Anthroprotect


#
# Open Data Cube configuration
#
DATACUBE_CONF = os.getenv('DATACUBE_CONF', 'datacube.conf')

#
# AnthroProtect dataset
# Dataset size: 19,5 GB (anthroprotect.zip) -> 48,7 GB (unzipped)
#
ANTHROPROTECT_ENABLED = os.getenv('ANTHROPROTECT_ENABLED', False)


#
# Datasets to be added to Open Data Cube
# tuples of dataset descriptor and loader class
#
DATASETS = []
if ANTHROPROTECT_ENABLED:
    DATASETS = DATASETS + ('anthroprotect', Anthroprotect)
