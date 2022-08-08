# Copyright (C) 2022 52°North Spatial Information Research GmbH
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
import os
import uuid
import time
import argparse
import logging.config
from pathlib import Path
import yaml
import datacube
import datacube.index.hl
import datacube.model
from pyproj import CRS
import rasterio
from utils import verify_database_connection, ensure_odc_connection_and_database_initialization, unzip

import requests
import zipfile
import re

from config import *

S2_BANDS = {
    'blue': {
        'aliases': 'BAND_2',
        'unit': '1',
    },
    'green': {
        'aliases': 'BAND_3',
        'unit': '1'
    },
    'red': {
        'aliases': 'BAND_4',
        'unit': '1',
    },
    'vegetation_red_edge1': {
        'aliases': 'BAND_5',
        'unit': '1',
    },
    'vegetation_red_edge2': {
        'aliases': 'BAND_6',
        'unit': '1',
    },
    'vegetation_red_edge3': {
        'aliases': 'BAND_7',
        'unit': '1',
    },
    'nir': {
        'aliases': 'BAND_8',
        'unit': '1',
    },
    'narrow_nir': {
        'aliases': 'BAND_8A',
        'unit': '1',
    },
    'swir1': {
        'aliases': 'BAND_11',
        'unit': '1',
    },
    'swir2': {
        'aliases': 'BAND_12',
        'unit': '1',
    }
}

LCS_BANDS = {
    'corine': {
        'aliases': '',
        'unit': '1',
    },
    'modis_1': {
        'aliases': '',
        'unit': '1'
    },
    'cgls': {
        'aliases': '',
        'unit': '1',
    },
    'globcover': {
        'aliases': '',
        'unit': '1',
    }
}

S2_SCL_BANDS = {
    'scl': {
        'aliases': '',
        'unit': '1',
    }
}

logging_config_file = Path(Path(__file__).parent, 'logging.yaml')
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
    print('Logging file configuration does not exist: "{}". Using defaults.'.format(logging_config_file))
    logging.basicConfig(level=level)

logger = logging.getLogger(__name__)


class Anthroprotect:

    def __init__(self, global_data_folder):
        self.url = os.getenv('ANTHROPROTECT_URL', 'https://uni-bonn.sciebo.de/s/6wrgdIndjpfRJuA/download')
        self.zip_file = os.getenv('ANTHROPROTECT_ZIP', 'anthroprotect.zip')
        self.folder = os.getenv('ANTHROPROTECT_FOLDER', 'anthroprotect')
        self.chunk_size = 8192  # in bytes

        product_names = os.getenv('ANTHROPROTECT_PRODUCT_NAMES')
        if product_names:
            self.product_names = product_names.split(" ")
        else:
            self.product_names = ['s2_anthropo', 's2_wdpa_Ia', 's2_wdpa_Ib', 's2_wdpa_II', 'lcs_anthropo',
                                  'lcs_wdpa_Ia', 'lcs_wdpa_Ib', 'lcs_wdpa_II', 's2_scl_anthropo',
                                  's2_scl_wdpa_Ia', 's2_scl_wdpa_Ib', 's2_scl_wdpa_II', 's2_investigative']
        self.product_dataset_map = self.create_product_dataset_map(global_data_folder)

    def download(self, global_data_folder):
        """
        Download anthroprotect dataset in chunks with given chunk size. Takes ~ >1 hour.

        :param global_data_folder:
        :returns: 'True' if anthroprotect folder was successfully created or already exists else 'False'
        """

        zip_file = os.path.join(global_data_folder, self.zip_file)
        out_folder = os.path.join(global_data_folder, self.folder)

        # Check existence of anthroprotect folder or zip file
        if os.path.exists(out_folder):
            logger.info("Folder '{}' already exists. Continue without download.".format(out_folder))
            return True
        elif os.path.exists(zip_file):
            logger.info("Zip file '{}' already exists. Try to unzip.".format(zip_file))
            try:
                #  Unzip zip file (takes ~6 minutes)
                unzip(zip_file, out_folder)
                return True
            except Exception as err:
                logger.error("Could not unzip '{}': '{}'".format(zip_file, err))
                return False

        # Download zip file
        try:
            logger.info("Try to download '{}' from {}.".format(zip_file, self.url))
            with requests.get(self.url, stream=True) as r:
                r.raise_for_status()
                with open(zip_file, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=self.chunk_size):
                        f.write(chunk)
        except Exception as err:
            logger.error("Could not download AnthroProtect dataset: '{}'".format(err))
            return False

        # Unzip zip file (takes ~6 minutes)
        try:
            logger.info("Try to unzip '{}'.".format(zip_file))
            unzip(zip_file, out_folder)
            return True
        except Exception as err:
            logger.error("Could not unzip '{}': '{}'".format(zip_file, err))
            return False

    def create_product_dataset_map(self, global_data_folder):

        data_folder = os.path.join(global_data_folder, self.folder)
        product_dataset_map = {}
        # get file names including path for each odc product
        for subfolder in ['s2', 's2_scl', 'lcs']:
            files = os.listdir(os.path.join(data_folder, 'tiles', subfolder))
            for category in ['anthropo', 'wdpa-Ia', 'wdpa-Ib', 'wdpa-II']:
                p = re.compile(category)
                datasets_category = [os.path.join(data_folder, 'tiles', subfolder, element) for element in files if p.match(element)]
                odc_product = '{}_{}'.format(subfolder, category).replace('-', '_')
                product_dataset_map.update({odc_product: datasets_category})
        product_dataset_map.update({'s2_investigative': os.listdir(os.path.join(data_folder, 'investigative'))})

        # Check odc product names
        for odc_product in self.product_names:
            if odc_product not in product_dataset_map.keys():
                logger.info("Product '{}' is missing in product-dataset-map!".format(odc_product))

        # Check if file names in subfolders are identical (they should be)
        assert product_dataset_map['s2_anthropo'] == product_dataset_map['s2_scl_anthropo'] == product_dataset_map['lcs_anthropo']
        assert product_dataset_map['s2_wdpa_Ia'] == product_dataset_map['s2_scl_wdpa_Ia'] == product_dataset_map['lcs_wdpa_Ia']
        assert product_dataset_map['s2_wdpa_Ib'] == product_dataset_map['s2_scl_wdpa_Ib'] == product_dataset_map['lcs_wdpa_Ib']
        assert product_dataset_map['s2_wdpa_II'] == product_dataset_map['s2_scl_wdpa_II'] == product_dataset_map['lcs_wdpa_II']

        return product_dataset_map

    def get_product_dataset_map(self):
        return self.product_dataset_map

    def get_product_names(self):
        return self.product_names

    def create_product_metadata_eo3(self, odc_product_name, metadata_dict):
        """
        Create product metadata in eo3 metadata format
        :param odc_product_name: product name
        :param metadata_dict:
        :returns: `dict` of product metadata
        """

        # assert odc_product_name

        measurements = []
        for band in bands:
            measurements.append(
                {
                    'name': band,
                    'dtype': dtype,
                    'units': bands[band]['unit'],
                    'nodata': nodata,
                    'aliases': [bands[band]['aliases']],
                }
            )

        product_yaml = {
            'metadata_type': 'eo3',
            'name': odc_product_name,
            'description': metadata_dict['description'],
            'metadata': metadata_dict['metadata'],
            'measurements': measurements,
        }
        return product_yaml

    def create_metadata_document(self, odc_product_name):

        keywords = ['AnthroProtect']

        # if odc_product_name.startswith('s2_scl'):
        #     keywords.extend(list(S2_SCL_BANDS.keys()))
        # elif odc_product_name.startswith('s2_'):
        #     keywords.extend(list(S2_BANDS.keys()))
        # elif odc_product_name.startswith('lcs_'):
        #     keywords.extend(list(LCS_BANDS.keys()))
        # else:
        #     logger.error("No band information found for product '{}'".format(odc_product_name))

        return {
            'product': {
                'name': odc_product_name
            },
            'keywords': keywords,
            'links': [
                {
                    'type': 'text/html',
                    'rel': 'canonical',
                    'title': 'AnthroProtect dataset',
                    'href': 'http://rs.ipb.uni-bonn.de/data/anthroprotect/',
                    'hreflang': 'en-US'
                }
            ]
        }

    def create_dataset_metadata_eo3(self, file_name, odc_product_name):
        """
        Create dataset metadata in eo3 metadata format
        :param file_name: file name with full path
        :param odc_product_name: odc product name
        :returns: `dict` of dataset metadata
        """

        assert os.path.exists(file_name)

        if odc_product_name.startswith('s2_scl'):
            bands = S2_SCL_BANDS
            platform = 'Sentinel-2 scene classiﬁcation map'
            instrument = None
        elif odc_product_name.startswith('s2_'):
            bands = S2_BANDS
            platform = 'Sentinel-2 Level-2A, 10 m resolution'
            instrument = 'Multi-spectral instrument (MSI)'
        elif odc_product_name.startswith('lcs_'):
            bands = LCS_BANDS
            platform = None
            instrument = None
        else:
            logger.error("No band information found for product '{}'".format(odc_product_name))

        # Dataset crs
        # if orientation.lower() != 'north_up':
        #     south = True
        # else:
        #     south = False
        # crs = CRS.from_dict({'proj': map_projection.lower(), 'zone': utm_zone, 'ellps': ellipsoid, 'south': south})

        # Extract geo-information from file
        geotiff = rasterio.open(file_name)
        bbox = geotiff.bounds
        dataset_transform = geotiff.get_transform()

        # Band information
        measurements = {}
        band_idx = 1
        for band in bands:
            measurements[band] = {
                'path': file_name,
                'band': band_idx
            }
            band_idx = band_idx + 1

        metadata = create_metadata_document()
        dataset_yaml = {
            'id': str(uuid.uuid5(uuid.NAMESPACE_URL, file_name)),
            '$schema': 'https://schemas.opendatacube.org/dataset',
            'product': {
                'name': odc_product,
            },
            'keywords': metadata.get('keywords'),
            'links': metadata.get('links'),
            'crs': geotiff.crs.to_string(),
            'geometry': {
                'type': 'Polygon',
                'coordinates': [[
                    [
                        bbox.left,
                        bbox.bottom
                    ]
                    ,
                    [
                        bbox.left,
                        bbox.top
                    ],
                    [
                        bbox.right,
                        bbox.top
                    ],
                    [
                        bbox.right,
                        bbox.bottom
                    ],
                    [
                        bbox.left,
                        bbox.bottom
                    ]
                ]]
            },
            'grids': {
                'default': {
                    'shape': [
                        geotiff.height,
                        geotiff.width
                    ],
                    'transform': [
                        dataset_transform[1],
                        dataset_transform[2],
                        dataset_transform[0],
                        dataset_transform[4],
                        dataset_transform[5],
                        dataset_transform[3],
                        0,
                        0,
                        1
                    ]
                }
            },
            'measurements': measurements,
            'properties': {
                'eo:platform': platform,
                'eo:instrument': instrument,
                'datetime': '2020-08-01T12:00:00.00Z',
                'odc:processing_datetime': '2021-08-01T12:00:00.00Z',
                'odc:file_format': 'GeoTIFF'
            },
            'lineage': {}
        }
        return dataset_yaml
