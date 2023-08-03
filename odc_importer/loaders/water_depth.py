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
import shutil



# NetCDF data types: https://docs.unidata.ucar.edu/nug/current/md_types.html

water_depth_measurements = {
    'z': {
        'aliases': ['water_depth', 'height'],
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


class WaterDepthLoader(NetCDFLoader):
    """
    A class for loading Water depth weather data.
    """
    measurement_dict = water_depth_measurements

    def __init__(self):
        super().__init__()
        self.url = os.getenv('WATER_DEPTH_URL', 'https://www.ngdc.noaa.gov/thredds/fileServer/global/ETOPO2022/30s/30s_bed_elev_netcdf/ETOPO_2022_v1_30s_N90W180_bed.nc')
        self.folder = os.getenv('WATER_DEPTH_FOLDER', 'water_depth')
        self.file_name= "ETOPO_2022_v1_30s_N90W180_bed.nc"
        self.chunck_size = 8192 # in bytes
        self.product_names = [os.getenv('WATER_DEPTH_PRODUCT_NAME', 'water_depth')]


    def download(self, global_data_folder):
        """
        Download water depth data directly from  
        ETOPO Global Relief Model
        :param global_data_folder:
        :return: water depth nc file 
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
            # Create the directory if it does not exist
            os.makedirs(out_folder, exist_ok=True)
            
        logger.info('Star downloading Water Depth data')
        
        # Download the water depth data (approx 1.7 GB)
        try:
            if not os.path.exists(file_output):         
                with requests.get(self.url, stream=True) as r:
                    r.raise_for_status()                
                    with open(file_output, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=self.chunck_size):
                            f.write(chunk)
                logger.info(f"Download of '{f}' successful")
                return True 
            else:
                logger.info('Water depth data already exists')
                return True 
            
        except Exception as err:
            logger.error(err)
            return False
            
            
