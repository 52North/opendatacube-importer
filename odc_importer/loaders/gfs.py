# Copyright (C) 2023 52°North Spatial Information Research GmbH
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
import logging.config
import os
import yaml

from .netcdf import NetCDFLoader

# ------------------------------------------- #
#              Band definitions               #
# ------------------------------------------- #

# NetCDF data types: https://docs.unidata.ucar.edu/nug/current/md_types.html

gfs_measurements = {
    'Temperature_surface': {
        'aliases': ['TMP'],
        'dtype': 'float32',
        'nodata': 'NaN',
        'units': 'K',
    },
    'Pressure_reduced_to_MSL_msl': {
        'aliases': ['PRMSL'],
        'dtype': 'float32',
        'nodata': 'NaN',
        'units': 'Pa'
    },
    'Wind_speed_gust_surface': {
        'aliases': ['GUST'],
        'dtype': 'float32',
        'nodata': 'NaN',
        'units': 'm/s',
    },
    'u_component_of_wind_height_above_ground': {
        'aliases': ['u-component_of_wind_height_above_ground', 'u_wind_height_above_ground', 'UGRD'],
        'layer': 'u-component_of_wind_height_above_ground',
        'dtype': 'float32',
        'nodata': 'NaN',
        'units': 'm/s',
    },
    'v_component_of_wind_height_above_ground': {
        'aliases': ['v-component_of_wind_height_above_ground', 'v_wind_height_above_ground', 'VGRD'],
        'layer': 'v-component_of_wind_height_above_ground',
        'dtype': 'float32',
        'nodata': 'NaN',
        'units': 'm/s',
    },
    # 'u-component_of_wind_sigma': {
    #     'aliases': ['BAND_6'],
    #     'dtype': 'uint16',
    #     'nodata': 0.0,
    #     'unit': '1',
    # },
    # ',v-component_of_wind_sigma': {
    #     'aliases': ['BAND_7'],
    #     'dtype': 'uint16',
    #     'nodata': 0.0,
    #     'units': '1',
    # },
}


logging_config_file = os.path.join(os.path.dirname(__file__), 'logging.yaml')
level = logging.DEBUG
if os.path.exists(logging_config_file):
    with open(logging_config_file, 'rt') as file:
        try:
            config = yaml.safe_load(file.read())
            logging.config.dictConfig(config)
        except Exception as e:
            print(e)
            print('Error while loading logging configuration from file "{}". Using defaults'
                  .format(logging_config_file))
            logging.basicConfig(level=level)
else:
    print('Logging file configuration does not exist: "{}". Using defaults.'.format(
        logging_config_file))
    logging.basicConfig(level=level)

logger = logging.getLogger(__name__)


class GfsLoader(NetCDFLoader):
    """
    A class for loading GFS weather data.
    """
    measurement_dict = gfs_measurements

    def __init__(self):
        super().__init__()
        self.folder = os.getenv('GFS_FOLDER', 'weather')
        self.product_names = [os.getenv('GFS_PRODUCT_NAME', 'weather')]


    def download(self, global_data_folder):
        """
        Check if GFS data was already downloaded. The actual download functionality is implemented in the
        Django app 'geonode_marinedata' and maridatadownloader.
        :param global_data_folder:
        :return: 'True' if the configured folder was successfully created or already exists else 'False'
        """

        out_folder = os.path.join(global_data_folder, self.folder)

        if os.path.exists(out_folder):
            logger.info(f"Folder '{out_folder}' already exists.")
            return True
        else:
            logger.info(f"Folder {out_folder} does not exist.")
            return False
