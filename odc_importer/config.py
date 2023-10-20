# Copyright (C) 2022-2023 52°North Spatial Information Research GmbH
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License version 2 as published
# by the Free Software Foundation.
#
# If the program is linked with libraries which are licensed under one of
# the following licenses, the combination of the program with the linked
# library is not considered a "derivative work" of the program:
#
#     - Apache License, version 2.0
#     - Apache Software License, version 1.0
#     - GNU Lesser General Public License, version 3
#     - Mozilla Public License, versions 1.0, 1.1 and 2.0
#     - Common Development and Distribution License (CDDL), version 1.0
#
# Therefore the distribution of the program linked with libraries licensed
# under the aforementioned licenses, is permitted by the copyright holders
# if the distribution is compliant with both the GNU General Public
# License version 2 and the aforementioned licenses.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
# Public License for more details.
#
import ast
import os

from loaders.anthroprotect import AnthroprotectLoader
from loaders.cmems import CmemsWavesLoader, CmemsCurrentsLoader, CmemsPhysicsLoader
from loaders.gfs import GfsLoader
from loaders.global_relief import GlobalReliefLoader

#
# Open Data Cube configuration
#
# The ODC configuration file is created from the environment variables DB_HOST, DB_PORT, DB_USER, DB_PASSWORD and DB
# and saved to the file specified by DATACUBE_CONF.
# Reference: https://datacube-core.readthedocs.io/en/latest/installation/database/setup.html#create-configuration-file
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
ANTHROPROTECT_ENABLED = ast.literal_eval(os.getenv('ANTHROPROTECT_ENABLED', 'False'))

#
# CMEMS data source (https://data.marine.copernicus.eu/products)
#
CMEMS_CURRENTS_ENABLED = ast.literal_eval(os.getenv('CMEMS_CURRENTS_ENABLED', 'False'))
CMEMS_PHYSICS_ENABLED = ast.literal_eval(os.getenv('CMEMS_PHYSICS_ENABLED', 'False'))
CMEMS_WAVES_ENABLED = ast.literal_eval(os.getenv('CMEMS_WAVES_ENABLED', 'False'))

#
# GFS data source (https://www.ncei.noaa.gov/products/weather-climate-models/global-forecast)
#
GFS_ENABLED = ast.literal_eval(os.getenv('GFS_ENABLED', 'False'))


#
# Global relief data source (https://www.ncei.noaa.gov/products/etopo-global-relief-model)
#
GLOBAL_RELIEF_ENABLED = ast.literal_eval(os.getenv('GLOBAL_RELIEF_ENABLED', 'False'))

#
# Data sources to be added to Open Data Cube
# tuples of data source descriptor and loader class
#
DATASOURCES = []


if ANTHROPROTECT_ENABLED:
    DATASOURCES.append(('anthroprotect', AnthroprotectLoader))

if CMEMS_CURRENTS_ENABLED:
    DATASOURCES.append(('cmems_currents', CmemsCurrentsLoader))

if CMEMS_PHYSICS_ENABLED:
    DATASOURCES.append(('cmems_physics', CmemsPhysicsLoader))

if CMEMS_WAVES_ENABLED:
    DATASOURCES.append(('cmems_waves', CmemsWavesLoader))

if GFS_ENABLED:
    DATASOURCES.append(('gfs', GfsLoader))
    
if GLOBAL_RELIEF_ENABLED:
    DATASOURCES.append(('global_relief', GlobalReliefLoader))

#
# Settings for periodic import
# Reference: https://schedule.readthedocs.io/en/stable/index.html
#
PERIODIC = ast.literal_eval(os.getenv('PERIODIC', 'False'))
PERIODIC_EVERY = ast.literal_eval(os.getenv('PERIODIC_EVERY')) if os.getenv('PERIODIC_EVERY') else None
PERIODIC_UNIT = os.getenv('PERIODIC_UNIT')
PERIODIC_AT = os.getenv('PERIODIC_AT')
PERIODIC_TIMEZONE = os.getenv('PERIODIC_TIMEZONE', 'UTC')
PERIODIC_UNTIL = os.getenv('PERIODIC_UNTIL')
PERIODIC_SLEEP = ast.literal_eval(os.getenv('PERIODIC_SLEEP', '1'))
