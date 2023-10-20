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

import requests


# NetCDF data types: https://docs.unidata.ucar.edu/nug/current/md_types.html
global_relief_measurements = {
    'z': {
        'aliases': ['global_relief', 'depth', 'water_depth', 'height', 'elevation', 'topography', 'bathymetry'],
        'dtype': 'float32',
        'nodata': 'NaN',
        'units': 'm',
    }
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


class GlobalReliefLoader(NetCDFLoader):
    """
    A class for loading the ETOPO Global Relief Model into an ODC instance.
    The NetCDF file format is used. GeoTIFF would in principle be available, but isn't implemented.

    Model version:
        - default: "bedrock elevation"
        - options: "bedrock elevation", "ice surface elevation", "geoid height"
                    The version can be changed using the environment variables GLOBAL_RELIEF_URL and
                    GLOBAL_RELIEF_FILE_NAME

    Model resolution:
        - default: 30 Arc-Seconds
        - options: 30 Arc-Seconds, 60 Arc-Seconds, [15 Arc-Seconds]
                   The resolution can be changed using the environment variables GLOBAL_RELIEF_URL and
                   GLOBAL_RELIEF_FILE_NAME
                   Although a 15 Arc-Second resolution is in principle available, it can't be used yet because the
                   dataset it is spread over multiple files which isn't supported yet

    References:
        - https://www.ncei.noaa.gov/products/etopo-global-relief-model
    """
    measurement_dict = global_relief_measurements

    def __init__(self):
        super().__init__()
        self.url = os.getenv(
            'GLOBAL_RELIEF_URL',
            'https://www.ngdc.noaa.gov/thredds/fileServer/global/ETOPO2022/30s/30s_bed_elev_netcdf'
            '/ETOPO_2022_v1_30s_N90W180_bed.nc'
        )
        self.folder = os.getenv('GLOBAL_RELIEF_FOLDER', 'global_relief')
        self.file_name = os.getenv('GLOBAL_RELIEF_FILE_NAME', 'ETOPO_2022_v1_30s_N90W180_bed.nc')
        self.chunk_size = 8192  # in bytes
        self.product_names = [os.getenv('GLOBAL_RELIEF_PRODUCT_NAME', 'global_relief')]

    def download(self, global_data_folder):
        """
        Download global relief data directly from ETOPO Global Relief Model
        :param global_data_folder:
        :return: bool, status of download
        """
        # Create output folder and output file
        out_folder = os.path.join(global_data_folder, self.folder)
        file_output = os.path.join(out_folder, self.file_name)

        # Check if the directory already exists 
        if os.path.exists(out_folder):
            logger.info(f"Folder '{out_folder}' already exists.")
        else:
            logger.info(f"Folder {out_folder} does not exist.")
            logger.info(f"Creating {out_folder} ...")
            os.makedirs(out_folder, exist_ok=True)

        # Download the global relief data (approx 1.7 GB for default settings)
        logger.info('Start downloading global relief data')
        try:
            if not os.path.exists(file_output):
                with requests.get(self.url, stream=True) as r:
                    r.raise_for_status()
                    with open(file_output, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=self.chunk_size):
                            f.write(chunk)
                logger.info(f"Download of '{file_output}' successful")
                return True
            else:
                logger.info('Global relief data already exists')
                return True
        except Exception as err:
            logger.error(err)
            return False
