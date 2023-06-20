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

# ------------------------------------------- #
#              Band definitions               #
# ------------------------------------------- #

# NetCDF data types: https://docs.unidata.ucar.edu/nug/current/md_types.html

# waves
waves_measurements = {
    'VHM0': {
        'aliases': ['sea_surface_wave_significant_height'],
        'dtype': 'int16',
        'nodata': -32767,
        'units': 'm',
        'scale_factor': 0.01,
        'add_offset': 0.0
    },
    'VTPK': {
        'aliases': ['sea_surface_wave_period_at_variance_spectral_density_maximum'],
        'dtype': 'int16',
        'nodata': -32767,
        'units': 's',
        'scale_factor': 0.01,
        'add_offset': 0.0
    },
    'VMDR': {
        'aliases': ['sea_surface_wave_from_direction'],
        'dtype': 'int16',
        'nodata': -32767,
        'units': 'degree',
        'scale_factor': 0.01,
        'add_offset': 180.0
    },
    # 'VPED': {
    #     'aliases': ['sea_surface_wave_from_direction_at_variance_spectral_density_maximum'],
    #     'dtype': 'int16',
    #     'nodata': -999,
    #     'units': 'degree',
    #     'scale_factor': 0.01,
    #     'add_offset': 0.0
    # }
}

# currents
currents_measurements = {
    # 'uo': {
    #     'aliases': ['eastward_sea_water_velocity'],
    #     'dtype': 'float32',
    #     'nodata': -999,
    #     'unit': 'm s-1',
    # },
    # 'vo': {
    #     'aliases': ['northward_sea_water_velocity'],
    #     'dtype': 'float32',
    #     'nodata': -999,
    #     'unit': 'm s-1'
    # },
    # 'vsdx': {
    #     'aliases': ['sea_surface_wave_stokes_drift_x_velocity'],
    #     'dtype': 'float32',
    #     'nodata': -999,
    #     'unit': 'm s-1',
    # },
    # 'vsdy': {
    #     'aliases': ['sea_surface_wave_stokes_drift_y_velocity'],
    #     'dtype': 'float32',
    #     'nodata': -999,
    #     'unit': 'm s-1',
    # },
    # 'utide': {
    #     'aliases': ['surface_sea_water_x_velocity_due_to_tide'],
    #     'dtype': 'float32',
    #     'nodata': -999,
    #     'unit': 'm s-1',
    # },
    # 'vtide': {
    #     'aliases': ['surface_sea_water_y_velocity_due_to_tide'],
    #     'dtype': 'float32',
    #     'nodata': -999,
    #     'unit': 'm s-1',
    # },
    'utotal': {
        'aliases': ['surface_sea_water_x_velocity'],
        'dtype': 'float32',
        'nodata': -999,
        'units': 'm s-1',
    },
    'vtotal': {
        'aliases': ['surface_sea_water_y_velocity'],
        'dtype': 'float32',
        'nodata': -999,
        'units': 'm s-1',
    }
}

# physics
physics_measurements = {
    'thetao': {
        'aliases': ['sea_water_potential_temperature'],
        'dtype': 'float32',
        'nodata': -999,
        'units': 'degrees_C',
    },
    # 'uo': {
    #     'aliases': ['eastward_sea_water_velocity'],
    #     'dtype': 'float32',
    #     'nodata': -999,
    #     'units': 'm s-1'
    # },
    # 'vo': {
    #     'aliases': ['northward_sea_water_velocity'],
    #     'dtype': 'float32',
    #     'nodata': -999,
    #     'units': 'm s-1',
    # },
    'zos': {
        'aliases': ['sea_surface_height_above_geoid'],
        'dtype': 'float32',
        'nodata': -999,
        'units': 'm',
    },
    'so': {
        'aliases': ['sea_water_salinity'],
        'dtype': 'float32',
        'nodata': -999,
        'units': '1e-3',
    }
}

# ------------------------------------------- #
#                  Classes                    #
# ------------------------------------------- #

class CmemsLoader(NetCDFLoader):
    """
    An abstract class for loading CMEMS data.
    """
    def __init__(self):
        super().__init__()

    def download(self, global_data_folder):
        """
        Check if CMEMS data was already downloaded. The actual download functionality is implemented in the
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


class CmemsWavesLoader(CmemsLoader):
    """
    A class for loading CMEMS waves data.
    """
    measurement_dict = waves_measurements

    def __init__(self):
        super().__init__()
        self.folder = os.getenv('CMEMS_WAVES_FOLDER', 'waves')
        self.product_names = [os.getenv('CMEMS_WAVES_PRODUCT_NAME', 'waves')]


class CmemsCurrentsLoader(CmemsLoader):
    """
    A class for loading CMEMS currents data.
    """
    measurement_dict = currents_measurements

    def __init__(self):
        super().__init__()
        self.folder = os.getenv('CMEMS_CURRENTS_FOLDER', 'currents')
        self.product_names = [os.getenv('CMEMS_CURRENTS_PRODUCT_NAME', 'currents')]


class CmemsPhysicsLoader(CmemsLoader):
    """
    A class for loading CMEMS physics data.
    """
    measurement_dict = physics_measurements

    def __init__(self):
        super().__init__()
        self.folder = os.getenv('CMEMS_PHYSICS_FOLDER', 'physics')
        self.product_names = [os.getenv('CMEMS_PHYSICS_PRODUCT_NAME', 'physics')]