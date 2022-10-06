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
import shutil
import yaml
import datacube
import datacube.index.hl
import datacube.model
from pyproj import CRS
import rasterio
from loader import BasicLoader
from utils import calc_sha256, ensure_odc_connection_and_database_initialization, unzip, verify_database_connection

import requests
import re

# Band definitions for product metadata
S2_BANDS = {
    'blue': {
        'aliases': ['BAND_2'],
        'dtype': 'uint16',
        'nodata': 0.0,
        'unit': '1',
    },
    'green': {
        'aliases': ['BAND_3'],
        'dtype': 'uint16',
        'nodata': 0.0,
        'unit': '1'
    },
    'red': {
        'aliases': ['BAND_4'],
        'dtype': 'uint16',
        'nodata': 0.0,
        'unit': '1',
    },
    'vegetation_red_edge1': {
        'aliases': ['BAND_5'],
        'dtype': 'uint16',
        'nodata': 0.0,
        'unit': '1',
    },
    'vegetation_red_edge2': {
        'aliases': ['BAND_6'],
        'dtype': 'uint16',
        'nodata': 0.0,
        'unit': '1',
    },
    'vegetation_red_edge3': {
        'aliases': ['BAND_7'],
        'dtype': 'uint16',
        'nodata': 0.0,
        'unit': '1',
    },
    'nir': {
        'aliases': ['BAND_8'],
        'dtype': 'uint16',
        'nodata': 0.0,
        'unit': '1',
    },
    'narrow_nir': {
        'aliases': ['BAND_8A'],
        'dtype': 'uint16',
        'nodata': 0.0,
        'unit': '1',
    },
    'swir1': {
        'aliases': ['BAND_11'],
        'dtype': 'uint16',
        'nodata': 0.0,
        'unit': '1',
    },
    'swir2': {
        'aliases': ['BAND_12'],
        'dtype': 'uint16',
        'nodata': 0.0,
        'unit': '1',
    }
}

LCS_BANDS = {
    'corine': {
        'aliases': [],
        'dtype': 'uint16',
        'nodata': 0.0,
        'unit': '1',
    },
    'modis_1': {
        'aliases': [],
        'dtype': 'uint16',
        'nodata': 0.0,
        'unit': '1'
    },
    'cgls': {
        'aliases': [],
        'dtype': 'uint16',
        'nodata': 0.0,
        'unit': '1',
    },
    'globcover': {
        'aliases': [],
        'dtype': 'uint16',
        'nodata': 0.0,
        'unit': '1',
    }
}

S2_SCL_BANDS = {
    'scl': {
        'aliases': [],
        'dtype': 'uint16',
        'nodata': 0.0,
        'unit': '1',
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
    print('Logging file configuration does not exist: "{}". Using defaults.'.format(logging_config_file))
    logging.basicConfig(level=level)

logger = logging.getLogger(__name__)


class AnthroprotectLoader(BasicLoader):

    def __init__(self):
        super().__init__()
        self.url = os.getenv('ANTHROPROTECT_URL', 'https://uni-bonn.sciebo.de/s/6wrgdIndjpfRJuA/download')
        self.zip_file = os.getenv('ANTHROPROTECT_ZIP', 'anthroprotect.zip')
        self.folder = os.getenv('ANTHROPROTECT_FOLDER', 'anthroprotect')
        self.chunk_size = 8192  # in bytes
        self.force_download = os.getenv('ANTHROPROTECT_FORCE_DOWNLOAD', False)
        self.zip_sha256_hash = os.getenv('ANTHROPROTECT_ZIP_SHA256', '88ab511c2c89b64cd29d5f0d03174b450af3dd66a48423bc7f212f4391206f0d')

        product_names = os.getenv('ANTHROPROTECT_PRODUCT_NAMES')
        if product_names:
            self.product_names = product_names.split(' ')
        else:
            self.product_names = ['s2', 's2_scl', 'lcs']

    def create_product_dataset_map(self, global_data_folder):
        """
        Create a dictionary with product names as keys and lists of datasets (full path to file) as values
        :param global_data_folder:
        """

        data_folder = os.path.join(global_data_folder, self.folder)
        product_dataset_map = {}
        # get file names including path for each odc product
        # include only '.tif' files (there might be yaml files stored next to them)
        for idx, subfolder in enumerate(['s2', 's2_scl', 'lcs']):
            files = os.listdir(os.path.join(data_folder, 'tiles', subfolder))
            datasets = [os.path.join(data_folder, 'tiles', subfolder, element)
                        for element in files if element.endswith('.tif')]
            product_dataset_map.update({self.product_names[idx]: datasets})

        # Check if tif file names in subfolders 's2', 's2_scl' and 'lcs' are identical (they should be)
        assert [os.path.basename(filename) for filename in product_dataset_map[self.product_names[0]]] \
               == [os.path.basename(filename) for filename in product_dataset_map[self.product_names[1]]] \
               == [os.path.basename(filename) for filename in product_dataset_map[self.product_names[2]]]

        # Add files from 'investigative' folder to 's2' datasets
        product_dataset_map.update(
            {
                self.product_names[0]: product_dataset_map.get(self.product_names[0]) +
                                       [os.path.join(data_folder, 'investigative', element)
                                        for element in os.listdir(os.path.join(data_folder, 'investigative'))
                                        if element.endswith('.tif')]
            }
        )

        # Check odc product names
        for odc_product in self.product_names:
            if odc_product not in product_dataset_map.keys():
                logger.info("Product '{}' is missing in product-dataset-map!".format(odc_product))

        self.product_dataset_map = product_dataset_map

    def create_product_metadata_eo3(self, odc_product_name):
        """
        Create product metadata in eo3 metadata format
        :param odc_product_name: product name
        :return: `dict` of product metadata
        """

        if odc_product_name.startswith('s2_scl'):
            bands = S2_SCL_BANDS
        elif odc_product_name.startswith('s2'):
            bands = S2_BANDS
        elif odc_product_name.startswith('lcs'):
            bands = LCS_BANDS
        else:
            logger.error("No band information found for product '{}'".format(odc_product_name))

        product_metadata = {
            'name': odc_product_name,
            'description': odc_product_name,
            'metadata': self._create_metadata_document(odc_product_name),
            'bands': bands,
        }
        return product_metadata

    def create_dataset_metadata_eo3(self, odc_product_name, dataset):
        """
        Create dataset metadata in eo3 metadata format
        :param odc_product_name: odc product name
        :param dataset: file name with full path
        :return: `dict` of dataset metadata
        """

        if odc_product_name.startswith('s2_scl'):
            bands = dict.fromkeys(list(S2_SCL_BANDS.keys()))
            platform = 'Sentinel-2 scene classiﬁcation map'
            instrument = None
        elif odc_product_name.startswith('s2'):
            bands = dict.fromkeys(list(S2_BANDS.keys()))
            platform = 'Sentinel-2 Level-2A'
            instrument = 'Multi-spectral instrument (MSI)'
        elif odc_product_name.startswith('lcs'):
            bands = dict.fromkeys(list(LCS_BANDS.keys()))
            platform = 'Copernicus CORINE Land Cover dataset, MODIS Land Cover Type 1, ' \
                       'Copernicus Global Land Service, ESA GlobCover'
            instrument = None
        else:
            logger.error("No band information found for product '{}'".format(odc_product_name))

        for idx, band in enumerate(bands):
            bands[band] = {
                'path': dataset,
                'band': idx + 1
            }

        metadata = self._create_metadata_document(odc_product_name)

        # Extract geo-information from file
        geotiff = rasterio.open(dataset)
        bbox = geotiff.bounds
        dataset_transform = geotiff.get_transform()

        # Dataset crs
        # if orientation.lower() != 'north_up':
        #     south = True
        # else:
        #     south = False
        # crs = CRS.from_dict({'proj': map_projection.lower(), 'zone': utm_zone, 'ellps': ellipsoid, 'south': south})

        dataset_metadata = {
            'id': str(uuid.uuid5(uuid.NAMESPACE_URL, dataset)),
            'product_name': odc_product_name,
            'crs': geotiff.crs.to_string(),
            'polygon': [[
                [
                    bbox.left,
                    bbox.bottom
                ],
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
            ]],
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
            ],
            'bands': bands,
            'platform': platform,
            'instrument': instrument,
            'datetime': '2020-08-01T12:00:00.00Z',
            'processing_datetime': '2021-10-12T12:00:00.00Z',
            'file_format': 'GeoTIFF',
            'lineage': {},
            'additions': {
                'keywords': metadata['keywords'],
                'links': metadata['links']
            }
        }

        return dataset_metadata

    def download(self, global_data_folder):
        """
        Download anthroprotect dataset in chunks with given chunk size. Takes ~ >1 hour.
        :param global_data_folder:
        :return: 'True' if anthroprotect folder was successfully created or already exists else 'False'
        """

        # ToDo: do not store temporary download file in global data folder (-> /tmp)
        zip_file = os.path.join(global_data_folder, self.zip_file)
        out_folder = os.path.join(global_data_folder, self.folder)

        # Check existence of anthroprotect folder or zip file
        if self.force_download:
            logger.info("'force_download' is 'True'. Delete folder '{}' recursively "
                        "and delete zip file '{}' if they exist.".format(out_folder, zip_file))
            if os.path.exists(out_folder):
                shutil.rmtree(out_folder)
            if os.path.exists(zip_file):
                os.remove(zip_file)
        elif os.path.exists(out_folder):
            logger.info("Folder '{}' already exists and 'force_download' is 'False'. "
                        "Continue without download.".format(out_folder))
            return True
        elif os.path.exists(zip_file):
            logger.info("Zip file '{}' already exists. Try to unzip.".format(zip_file))
            # Check hash of zip file
            self._check_sha256(zip_file)
            return self._unzip_anthroprotect(global_data_folder, zip_file)

        # Download zip file (takes ~> 1 hour)
        try:
            logger.info("Try to download '{}' from '{}'.".format(zip_file, self.url))
            with requests.get(self.url, stream=True) as r:
                r.raise_for_status()
                with open(zip_file, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=self.chunk_size):
                        f.write(chunk)
            logger.info("Download of '{}' successful".format(zip_file))
            # Check hash of zip file
            self._check_sha256(zip_file)
        except Exception as err:
            logger.error("Could not download AnthroProtect dataset: '{}'".format(err))
            return False

        logger.info("Try to unzip '{}'.".format(zip_file))
        return self._unzip_anthroprotect(global_data_folder, zip_file)

    def _check_sha256(self, zip_file):
        if self.zip_sha256_hash != calc_sha256(zip_file):
            logger.error("SHA256 hash of the downloaded zip file '{}' is wrong. Delete the file and abort."
                         .format(zip_file))
            os.remove(zip_file)
            return False

    def _create_metadata_document(self, odc_product_name):

        # keywords and links are needed for pygeoapi
        keywords = ['AnthroProtect', 'Wilderness', 'Fennoscandia']

        if odc_product_name.startswith('s2_scl'):
            keywords = keywords + ['Sentinel-2 scene classiﬁcation map']
        elif odc_product_name.startswith('s2'):
            keywords = keywords + ['Sentinel-2']
        elif odc_product_name.startswith('lcs'):
            keywords = keywords + ['Land cover data']
        else:
            logger.error("No band information found for product '{}'".format(odc_product_name))

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

    def _unzip_anthroprotect(self, global_data_folder, zip_file):
        try:
            #  Unzip zip file (takes ~6 minutes)
            # anthroprotect.zip contains a folder 'anthroprotect'
            # -> use global_data_folder as unzip target
            unzip(zip_file, global_data_folder)
            os.remove(zip_file)
            logger.info("Unzipping successful. Removed '{}'.".format(zip_file))
            return True
        except Exception as err:
            logger.error("Could not unzip '{}': '{}'".format(zip_file, err))
            return False
