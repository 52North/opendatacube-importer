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
from xml.etree import ElementTree
import tarfile
import datacube
import datacube.index.hl
import datacube.model
from pyproj import CRS
import rasterio
from utils import verify_database_connection, ensure_odc_connection_and_database_initialization

import requests
import zipfile
import re

ODC_PRODUCT_NAMES = ['s2_anthropo', 's2_wdpa_Ia', 's2_wdpa_Ib', 's2_wdpa_II', 'lcs_anthropo',
                     'lcs_wdpa_Ia', 'lcs_wdpa_Ib', 'lcs_wdpa_II', 's2_scl_anthropo', 's2_scl_wdpa_Ia',
                     's2_scl_wdpa_Ib', 's2_scl_wdpa_II', 's2_investigative']
DATACUBE_CONF = 'datacube.conf'

# anthroprotect.zip with 19,5 GB -> unzipped 48,7 GB
ANTHROPROTECT_URL = 'https://uni-bonn.sciebo.de/s/6wrgdIndjpfRJuA/download'
ANTHROPROTECT_ZIP = 'DATA/anthroprotect.zip'
ANTHROPROTECT_FOLDER = 'DATA/anthroprotect'

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


def parse_parameter() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Add Anthroprotect dataset to Open Data '
                                                 'Cube index.')
    parser.add_argument('-p', '--landsat-product-ids',
                        help="A comma separated list of landsat product ids. " +
                             "Defaults to '???'.",
                        required=True, type=str)
    parser.add_argument('-b', '--bands',
                        help="A comma separated list of bands. " +
                             "Defaults to 'blue,green,red,nir,swir1,swir2'.",
                        required=False, type=str, default="blue,green,red,nir,swir1,swir2")
    parser.add_argument('-s', '--host',
                        help="Database host name. Defaults to 'localhost'.",
                        required=False, type=str, default="localhost")
    parser.add_argument('-u', '--user',
                        help="Database user. Defaults to 'opendatacube'.",
                        required=False, type=str, default="opendatacube")
    parser.add_argument('-a', '--password',
                        help="Database password. Defaults to 'opendatacube'.",
                        required=False, type=str, default="opendatacube")
    parser.add_argument('-d', '--db',
                        help="Database name. Defaults to 'opendatacube'.",
                        required=False, type=str, default='opendatacube')
    parser.add_argument('-r', '--max-retries',
                        help="Maximum number of retries while waiting for the database to become available. Defaults to 15.",
                        required=False, type=int, default=15)
    parser.add_argument('-l', '--sleep',
                        help="Number of seconds to sleep between each database connection test. Defaults to 2.",
                        required=False, type=int, default=2)
    parser.add_argument('--ee-user',
                        help="EarthExplorer user.",
                        required=True, type=str)
    parser.add_argument('--ee-password',
                        help="EarthExplorer password.",
                        required=True, type=str)

    args = parser.parse_args()

    #
    #    Post process arguments
    #
    if args.sleep and args.sleep < 0 or args.sleep > 20:
        parser.error("Sleep MUST be > 0 and < 21")

    if args.max_retries and args.max_retries < 0 or args.max_retries > 15:
        parser.error("Max_retries MUST be > 0 and < 16")

    if args.landsat_product_ids:
        args.landsat_product_ids = [s.strip() for s in args.landsat_product_ids.split(",")]

    if args.bands:
        args.bands = [s.strip() for s in args.bands.split(",")]

    logger.info("""
    Starting download and indexing
    ==============================
    - empty values are allowed

    Download filter
    ---------------
    landsat_products_ids    : {}
    bands                   : {}

    General
    -------


    OpenDataCube Connection Configuration
    -------------------------------------
    host        : '{}'
    user        : '{}'
    pass        : '{}'
    max retries : '{}'
    sleep       : '{}'
    """.format(args.landsat_product_ids, args.bands, args.host, args.user,
               len(args.password) * '*', args.max_retries, args.sleep)

    return args


def _create_datetime(date, time):
    # date format: 2021-03-30
    # time format: 10:21:16.7550770Z
    # desired datetime format: 2021-03-30T10:21:16.7550770Z
    return '{}T{}'.format(date, time)


def download_anthroprotect(url, zip_file, chunk_size=8192):
    """
    Download anthroprotect dataset. Takes ~ >1 hour.
    :param url: url of dataset
    :param zip_file: destination of download
    :param chunk_size: chunk size in bytes
    """

    # ToDo: check if zip_file exists

    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(zip_file, 'wb') as f:
            for chunk in r.iter_content(chunk_size=chunk_size):
                f.write(chunk)


def unzip_anthroprotect(zip_file, out_folder):
    """
    Unzip zip file. Takes ~6 minute.
    :param zip_file: name of zip file to be unzipped
    :param out_folder: destination of unzipped content
    """

    # ToDo: check if out_folder exists

    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(out_folder)


def create_anthroprotect_eo3_product_metadata(odc_product_name, nodata=None, dtype='uint16'):
    """
    Create anthroprotect product metadata in eo3 metadata format
    :param odc_product_name:
    :param nodata: no data value that is used for ALL bands. If not provided it will be read from GeoTIFF file (landsat_product_id needs to be provided).
    :param dtype: dtype that is used for ALL bands.
    :returns: `dict` of product metadata
    """

    # get/set/check folders

    # assert odc_product_name

    if odc_product_name.startswith('s2_scl'):
        bands = S2_SCL_BANDS
        description = 'Sentinel-2 scene classification map data from AnthroProtect dataset'
    elif odc_product_name.startswith('s2_'):
        bands = S2_BANDS
        description = 'Sentinel-2 Level-2A data from AnthroProtect dataset'
    elif odc_product_name.startswith('lcs_'):
        bands = LCS_BANDS
        description = 'Land cover data (CORINE, MODIS 1, Copernicus Global Land Service, ESA GlobCover) from AnthroProtect dataset'
    else:
        logger.error("No band information found for product '{}'".format(odc_product_name))

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
        'description': description,
        'metadata': create_metadata_document(),
        'measurements': measurements,
    }
    return product_yaml


def create_metadata_document(odc_product_name):

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


def create_anthroprotect_eo3_dataset_metadata(file_name, odc_product_name):
    """
    Create anthroprotect dataset metadata in eo3 metadata format
    :param file_name: file name with full path
    :param odc_product_name: odc product name
    :returns: `dict` of product metadata
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


def process_datasets(global_data_folder, dc):
    """
    Add anthroprotect dataset to Open Data Cube index
    :param global_data_folder: folder where Landsat 8 scenes and metadata yamls are located
    :param dc: Open Data Cube
    :returns:
    """

    # check if anthroprotect dataset was downloaded and unzipped
    data_folder = Path(global_data_folder).resolve()
    if not os.path.exists(data_folder):
        logging.error("Global data folder '{}' not existing."
                      .format(data_folder))
    elif not os.access(data_folder, os.W_OK):
        logging.error("Global data folder '{}' exists but is not writable".format(data_folder))
        exit(32)
    logger.info("Ensured global data folder existence '{}'".format(data_folder))

    # index products and save product yaml files
    for odc_product in ODC_PRODUCT_NAMES:
        if odc_product not in dc.list_products()["name"].values:
            product_yaml = create_anthroprotect_eo3_product_metadata(odc_product)
            # ToDo: check if yaml files should be saved here
            product_filename = Path(global_data_folder, '{}.odc-product.yaml'.format(odc_product))

            # ToDo: overwrite option?
            if not os.path.exists(product_filename):
                with open(product_filename, 'w') as f:
                    yaml.dump(product_yaml, f, default_flow_style=False, sort_keys=False)

            dc.index.products.add_document(product_yaml)
            logger.info("Added product family '{}' to the index".format(ODC_PRODUCT_NAME))
        else:
            logger.info("Product family '{}' already in index".format(ODC_PRODUCT_NAME))

    # index dataset and save dataset yaml

    # get file names including path for each odc product
    product_files_dict = {}
    for subfolder in ['s2', 's2_scl', 'lcs']:
        files = os.listdir(os.path.join(data_folder, 'tiles', subfolder))
        for category in ['anthropo', 'wdpa-Ia', 'wdpa-Ib', 'wdpa-II']:
            p = re.compile(category)
            files_category = [os.path.join(data_folder, 'tiles', subfolder, element) for element in files if p.match(element)]
            odc_product = '{}_{}'.format(subfolder, category).replace('-', '_')
            product_files_dict.update({odc_product: files_category})
    product_files_dict.update({'s2_investigative': os.listdir(os.path.join(data_folder, 'investigative'))})

    # check odc product names
    for odc_product in ODC_PRODUCT_NAMES:
        if odc_product not in product_files_dict.keys():
            logger.info("Product '{}' is missing in file dictionary!".format(odc_product))

    # check if file names in subfolders are identical (they should be)
    assert product_files_dict['s2_anthropo'] == product_files_dict['s2_scl_anthropo'] == product_files_dict['lcs_anthropo']
    assert product_files_dict['s2_wdpa_Ia'] == product_files_dict['s2_scl_wdpa_Ia'] == product_files_dict['lcs_wdpa_Ia']
    assert product_files_dict['s2_wdpa_Ib'] == product_files_dict['s2_scl_wdpa_Ib'] == product_files_dict['lcs_wdpa_Ib']
    assert product_files_dict['s2_wdpa_II'] == product_files_dict['s2_scl_wdpa_II'] == product_files_dict['lcs_wdpa_II']

    idx_product = 1
    for odc_product in ODC_PRODUCT_NAMES:
        logger.info("[Product {}/{}] Start indexing datasets for product '{}'".format(
            idx_product,
            len(ODC_PRODUCT_NAMES),
            odc_product))
        idx_dataset = 1
        for file_name in product_files_dict[odc_product]:

            dataset_id = str(uuid.uuid5(uuid.NAMESPACE_URL, file_name))

            if not dc.index.datasets.get(id_=dataset_id) is None:
                logger.info("[Dataset {}/{}] Dataset with id '{}' already in the index".format(
                    idx_dataset,
                    len(product_files_dict[odc_product]),
                    dataset_id))
            else:
                dataset_yaml = create_anthroprotect_eo3_dataset_metadata(file_name, odc_product)
                dataset_filename = Path(global_data_folder, '{}.odc-metadata.yaml'.format(file_name)).resolve()
                with open(dataset_filename, 'w') as f:
                    yaml.dump(dataset_yaml, f, default_flow_style=False, sort_keys=False)

                resolver = datacube.index.hl.Doc2Dataset(index=dc.index, eo3=True)
                dataset = resolver(dataset_yaml, dataset_filename.as_uri())
                dc.index.datasets.add(dataset[0])
                logger.info("[{}/{}] Added dataset with id '{}' to the index".format(
                    idx_dataset,
                    len(product_files_dict[odc_product]),
                    dataset_id))

            idx_dataset = idx_dataset + 1
        idx_product = idx_product + 1


def main():
    args = parse_parameter()

    times_sleeped = 0
    db_conn_ok = False
    while times_sleeped < args.max_retries:
        # check "connection" to odc (?) or later
        logger.info("[{}/{}] Check database connection".format(str(times_sleeped + 1), str(args.max_retries)))
        if verify_database_connection(args.db, args.host, args.user, args.password, True):
            db_conn_ok = True
            break
        time.sleep(args.sleep)
        times_sleeped = times_sleeped + 1

    if not db_conn_ok:
        exit(1024)

    ensure_odc_connection_and_database_initialization(args.db,
                                                      args.host,
                                                      args.user,
                                                      args.password,
                                                      '')
    dc = datacube.Datacube(config=DATACUBE_CONF)

    zip_file = DATA_FOLDER + 'anthroprotect.zip'
    out_folder = DATA_FOLDER + 'anthroprotect'

    download_anthroprotect(ANTHROPROTECT_URL, zip_file)
    unzip_anthroprotect(zip_file, out_folder)

    process_datasets(DATA_FOLDER, dc, args.bands, args.landsat_product_ids)


if __name__ == '__main__':
    main()
