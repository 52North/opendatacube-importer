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

# Note: all key-value pairs which are under the 'metadata' key in the product.yaml also have to be in the dataset.yaml
#       (here as top-level elements). If that’s not the case there will be an error thrown while indexing:
#         BadMatch('Error loading lineage dataset: Dataset metadata did not match product signature.
#       Additional elements in the dataset.yaml are possible (no error is thrown), however, they will not appear
#       in the database after indexing.

import argparse
import logging
import os
import time
import uuid

import datacube
import yaml
from datacube.index.hl import Doc2Dataset

from config import BASE_FOLDER, DATACUBE_CONF, DATA_FOLDER, DATASOURCES
from utils import verify_database_connection, ensure_odc_connection_and_database_initialization, check_global_data_folder


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


def parse_parameter() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Add data from defined data sources to Open Data Cube index.')
    parser.add_argument('-s', '--host',
                        help="Database host name. Defaults to 'localhost'.",
                        required=False, type=str, default='localhost')
    parser.add_argument('-p', '--port',
                        help="Database port. Defaults to 5432.",
                        required=False, type=int, default=5432)
    parser.add_argument('-u', '--user',
                        help="Database user. Defaults to 'opendatacube'.",
                        required=False, type=str, default='opendatacube')
    parser.add_argument('-a', '--password',
                        help="Database password. Defaults to 'opendatacube'.",
                        required=False, type=str, default='opendatacube')
    parser.add_argument('-d', '--db',
                        help="Database name. Defaults to 'opendatacube'.",
                        required=False, type=str, default='opendatacube')
    parser.add_argument('-n', '--no-ping',
                        help="Do not try to ping the database host before trying to connect to the correct port.",
                        required=False, type=bool, default=False)
    parser.add_argument('-r', '--max-retries',
                        help="Maximum number of retries while waiting for the database to become available. Defaults to 15.",
                        required=False, type=int, default=15)
    parser.add_argument('-l', '--sleep',
                        help="Number of seconds to sleep between each database connection test. Defaults to 2.",
                        required=False, type=int, default=2)

    args = parser.parse_args()

    #
    #    Post process arguments
    #
    if args.sleep and args.sleep < 0 or args.sleep > 20:
        parser.error("Sleep MUST be > 0 and < 21")

    if args.max_retries and args.max_retries < 0 or args.max_retries > 15:
        parser.error("Max_retries MUST be > 0 and < 16")

    logger.info("""
    Starting download and indexing
    ==============================
    - empty values are allowed

    Datasets
    ---------------
    {}
    
    Dataset loader
    ---------------
    {}

    OpenDataCube Connection Configuration
    -------------------------------------
    host        : '{}'
    port        : '{}'
    user        : '{}'
    pass        : '{}'
    max retries : '{}'
    sleep       : '{}'
    """.format([dataset[0] for dataset in DATASOURCES], [dataset[1] for dataset in DATASOURCES],
               args.host, args.port, args.user, len(args.password) * '*', args.max_retries, args.sleep))

    return args


def add_datasets_to_odc(loader, dc):
    """
    Add datasets to Open Data Cube index

    :param loader: dataset loader
    :param dc: Open Data Cube
    :return: None
    """

    odc_product_dataset_map = loader.get_product_dataset_map()
    odc_product_names = loader.get_product_names()

    # ToDo: add option to overwrite yaml files?

    # Index datasets and save dataset yaml files
    for idx_product, odc_product in enumerate(odc_product_names):
        logger.info("[Product {}/{}] Start indexing datasets for product '{}'".format(
            idx_product+1,
            len(odc_product_names),
            odc_product))
        # 'odc_dataset' can be a file or a folder (e.g. when bands are in separate files like for Landsat 8)
        for idx_dataset, odc_dataset in enumerate(odc_product_dataset_map[odc_product]):
            if not os.path.exists(odc_dataset):
                logger.info(
                    "Dataset '{}' does not exist! Continue with next.".format(odc_dataset))
                continue
            dataset_id = str(uuid.uuid5(uuid.NAMESPACE_URL, odc_dataset))
            if not dc.index.datasets.get(id_=dataset_id) is None:
                logger.info("[Dataset {}/{}] Dataset with id '{}' already in the index".format(
                    idx_dataset+1,
                    len(odc_product_dataset_map[odc_product]),
                    dataset_id))
            else:
                try:
                    dataset_metadata = loader.create_dataset_metadata_eo3(
                        odc_product, odc_dataset)
                    dataset_yaml = create_dataset_yaml_eo3(
                        odc_product, dataset_metadata)
                    # Note: be careful with choosing path and name of dataset yaml file because file or folder
                    # names might exist more than once (this is the case, e.g., for the AnthroProtect dataset)
                    # Put dataset yaml file next to the file/folder
                    file_or_folder_path, file_or_folder_name = os.path.split(
                        os.path.normpath(odc_dataset))
                    dataset_filename = os.path.join(file_or_folder_path,
                                                    '{}.odc-metadata.yaml'.format(file_or_folder_name))
                    with open(dataset_filename, 'w') as f:
                        yaml.dump(dataset_yaml, f, default_flow_style=False, sort_keys=False)
                    resolver = Doc2Dataset(index=dc.index, eo3=True)
                    dataset = resolver(dataset_yaml, 'file://{}'.format(dataset_filename))
                    logger.info(f'{dataset} and {dataset[0]}')
                    # logger.info(dataset[0])
                    dc.index.datasets.add(dataset[0])
                    logger.info("[{}/{}] Added dataset with id '{}' to the index".format(
                        idx_dataset+1,
                        len(odc_product_dataset_map[odc_product]),
                        dataset_id))
                except Exception as err:
                    logger.warning(f"Could not add dataset '{odc_dataset}' to the ODC index: {err}")
                    continue
    return None


def add_products_to_odc(loader, global_data_folder, dc):
    """
    Add product to Open Data Cube index

    :param loader: dataset loader
    :param global_data_folder: folder where data and metadata yamls are located
    :param dc: Open Data Cube
    :return: None
    """

    odc_product_names = loader.get_product_names()

    # ToDo: add option to overwrite yaml files?

    # Index products and save product yaml files
    for odc_product in odc_product_names:
        if odc_product not in dc.list_products()['name'].values:
            product_metadata = loader.create_product_metadata_eo3(odc_product)
            product_yaml = create_product_yaml_eo3(
                odc_product, product_metadata)
            product_filename = os.path.join(global_data_folder,
                                            loader.get_folder(),
                                            '{}.odc-product.yaml'.format(odc_product))
            if not os.path.exists(product_filename):
                with open(product_filename, 'w') as f:
                    yaml.dump(product_yaml, f,
                              default_flow_style=False, sort_keys=False)
            dc.index.products.add_document(product_yaml)
            logger.info(
                "Added product family '{}' to the index".format(odc_product))
        else:
            logger.info(
                "Product family '{}' already in index".format(odc_product))

    return None


def create_dataset_yaml_eo3(odc_product_name, metadata_dict):
    """
    Create dataset metadata in eo3 metadata format

    :param metadata_dict:
    :param odc_product_name: odc product name
    :return: `dict` of dataset metadata
    """

    assert odc_product_name == metadata_dict['metadata']['product']['name']

    # Band information
    measurements = {}
    for band in metadata_dict['bands']:
        d = {'path': metadata_dict['bands'][band]['path']}
        # https://datacube-core.readthedocs.io/en/latest/installation/dataset-documents.html#eo3-format
        # integer, 1-based index into multi-band file
        if 'band' in metadata_dict['bands'][band]:
            d['band'] = metadata_dict['bands'][band]['band']
        # str: netcdf variable to read
        if 'layer' in metadata_dict['bands'][band]:
            d['layer'] = metadata_dict['bands'][band]['layer']
        measurements[band] = d

    # Define yaml file
    dataset_yaml = {
        '$schema': metadata_dict.get('schema', 'https://schemas.opendatacube.org/dataset'),
        'id': metadata_dict['id'],
        'crs': metadata_dict['crs'],
        'geometry': {
            'type': 'Polygon',
            'coordinates': metadata_dict['polygon']
        },
        'grids': {
            'default': {
                'shape': metadata_dict['shape'],
                'transform': metadata_dict['transform']
            }
        },
        'measurements': measurements,
        'properties': {
            'eo:platform': metadata_dict['platform'],
            'eo:instrument': metadata_dict['instrument'],
            'datetime': metadata_dict['datetime'],
            'odc:processing_datetime': metadata_dict['processing_datetime'],
            'odc:file_format': metadata_dict['file_format']
        },
        'lineage': metadata_dict.get('lineage')
    }

    dataset_yaml.update(metadata_dict['metadata'])

    return dataset_yaml


def create_product_yaml_eo3(odc_product_name, metadata_dict):
    """
    Create product metadata in eo3 metadata format

    :param odc_product_name: product name
    :param metadata_dict:
    :return: `dict` of product metadata
    """

    assert odc_product_name == metadata_dict['name'] == metadata_dict['metadata']['product']['name']

    measurements = []
    for measurement in metadata_dict['measurements']:
        measurements.append(
            {
                'name': measurement,
                'dtype': metadata_dict['measurements'][measurement]['dtype'],
                'units': metadata_dict['measurements'][measurement]['units'],
                'nodata': metadata_dict['measurements'][measurement]['nodata'],
                'aliases': metadata_dict['measurements'][measurement]['aliases'],
            }
        )

    product_yaml = {
        'metadata_type': 'eo3',
        'name': metadata_dict['name'],
        'description': metadata_dict['description'],
        'metadata': metadata_dict['metadata'],
        'measurements': measurements,
    }
    return product_yaml


def main():
    args = parse_parameter()

    times_slept = 0
    db_conn_ok = False
    while times_slept < args.max_retries:
        # check "connection" to odc (?) or later
        logger.info(
            "[{}/{}] Check database connection".format(str(times_slept + 1), str(args.max_retries)))
        if verify_database_connection(args.db, args.host, args.port, args.user, args.password, args.no_ping):
            db_conn_ok = True
            break
        time.sleep(args.sleep)
        times_slept = times_slept + 1

    if not db_conn_ok:
        logger.error("Could not connect to database!")
        exit(1024)

    ensure_odc_connection_and_database_initialization(args.db,
                                                      args.host,
                                                      args.port,
                                                      args.user,
                                                      args.password,
                                                      '',
                                                      DATACUBE_CONF)

    # Set and check global data folder
    global_data_folder = os.path.join(BASE_FOLDER, DATA_FOLDER)
    check_global_data_folder(global_data_folder)

    logger.info("Start processing data sources")

    # Process data sources
    for idx, datasource in enumerate(DATASOURCES):

        logger.info("[Data source {}/{}] Start downloading and indexing data from data source '{}'"
                    .format(idx+1, len(DATASOURCES), datasource[0]))

        loader = datasource[1]()

        # Download data from data source
        download_success = loader.download(global_data_folder)
        if not download_success:
            logger.warning(
                "Could not download data from data source '{}'.".format(datasource[0]))
        else:
            if len(loader.product_dataset_map) == 0:
                loader.create_product_dataset_map(global_data_folder)

            # Add products and datasets to Open Data Cube index
            dc = datacube.Datacube(config=DATACUBE_CONF)
            add_products_to_odc(loader, global_data_folder, dc)
            add_datasets_to_odc(loader, dc)

    logger.info("Finished processing data sources")


if __name__ == '__main__':
    main()
