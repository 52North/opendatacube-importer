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
from pathlib import Path
import logging

from config import DATACUBE_CONF, GLOBAL_DATA_FOLDER, DATASETS
from utils import verify_database_connection, ensure_odc_connection_and_database_initialization, check_global_data_folder

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
    parser = argparse.ArgumentParser(description='Add datasets to Open Data Cube index.')
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
    user        : '{}'
    pass        : '{}'
    max retries : '{}'
    sleep       : '{}'
    """.format([dataset[0] for dataset in DATASETS], [dataset[1] for dataset in DATASETS],
               args.host, args.user, len(args.password) * '*', args.max_retries, args.sleep))

    return args


def create_product_yaml_eo3(odc_product_name, metadata_dict):
    """
    Create product metadata in eo3 metadata format

    :param odc_product_name: product name
    :param metadata_dict:
    :return: `dict` of product metadata
    """

    assert odc_product_name == metadata_dict['name'] == metadata_dict['metadata']['product']['name']

    measurements = []
    for band in metadata_dict['bands']:
        measurements.append(
            {
                'name': band,
                'dtype': metadata_dict['bands'][band]['dtype'],
                'units': metadata_dict['bands'][band]['unit'],
                'nodata': metadata_dict['bands'][band]['nodata'],
                'aliases': metadata_dict['bands'][band]['aliases'],
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


def create_dataset_yaml_eo3(odc_product_name, metadata_dict):
    """
    Create dataset metadata in eo3 metadata format

    :param metadata_dict:
    :param odc_product_name: odc product name
    :return: `dict` of dataset metadata
    """

    assert odc_product_name == metadata_dict['name']

    # Band information
    measurements = {}
    band_idx = 1
    for band in metadata_dict['bands']:
        measurements[band] = {
            'path': file_name,
            'band': band_idx
        }
        band_idx = band_idx + 1

    # Define yaml file
    dataset_yaml = {
        '$schema': metadata_dict.get('schema', 'https://schemas.opendatacube.org/dataset'),
        'id': metadata_dict['id'],
        'product': {
            'name': metadata_dict['product_name'],
        },
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

    # Add additional key-value pairs which are not part of the default metadata
    if metadata_dict.get('additions'):
        dataset_yaml.update(metadata_dict['additions'])

    return dataset_yaml


def add_datasets_to_odc(global_data_folder, dc):
    """
    Add datasets to Open Data Cube index

    :param global_data_folder: folder where data and metadata yamls are located
    :param dc: Open Data Cube
    :return: None
    """

    for dataset in DATASETS:
        loader = dataset[1](global_data_folder)
        odc_product_dataset_map = loader.get_product_dataset_map()
        odc_product_names = loader.get_product_names()
        logger.info("""
        ------------------------------------------
        Start indexing dataset '{}'
        """.format(dataset[0]))

        # ToDo: add option to overwrite yaml files?

        # Index products and save product yaml files
        for odc_product in odc_product_names:
            if odc_product not in dc.list_products()['name'].values:
                product_metadata = loader.create_product_metadata_eo3(odc_product)
                product_yaml = create_product_yaml_eo3(odc_product, product_metadata)
                product_filename = os.path.join(global_data_folder,
                                                loader.get_folder(),
                                                '{}.odc-product.yaml'.format(odc_product))
                if not os.path.exists(product_filename):
                    with open(product_filename, 'w') as f:
                        yaml.dump(product_yaml, f, default_flow_style=False, sort_keys=False)
                dc.index.products.add_document(product_yaml)
                logger.info("Added product family '{}' to the index".format(odc_product))
            else:
                logger.info("Product family '{}' already in index".format(odc_product))

        # Index dataset and save dataset yaml files
        idx_product = 1
        for odc_product in odc_product_names:
            logger.info("[Product {}/{}] Start indexing datasets for product '{}'".format(
                idx_product,
                len(odc_product_names),
                odc_product))
            idx_dataset = 1
            # 'odc_dataset' can be a file or a folder (e.g. when bands are in separate files like for Landsat 8)
            for odc_dataset in odc_product_dataset_map[odc_product]:
                if not os.path.exists(odc_dataset):
                    logger.info("Dataset '{}' does not exist! Continue with next.".format(odc_dataset))
                    continue
                dataset_id = str(uuid.uuid5(uuid.NAMESPACE_URL, odc_dataset))
                if not dc.index.datasets.get(id_=dataset_id) is None:
                    logger.info("[Dataset {}/{}] Dataset with id '{}' already in the index".format(
                        idx_dataset,
                        len(odc_product_dataset_map[odc_product]),
                        dataset_id))
                else:
                    dataset_metadata = loader.create_dataset_metadata_eo3(odc_product, odc_dataset)
                    dataset_yaml = create_dataset_yaml_eo3(odc_product, dataset_metadata)
                    # Note: be careful with choosing path and name of dataset yaml file because file or folder
                    # names might exist more than once (this is the case, e.g., for the AnthroProtect dataset)
                    # Put dataset yaml file next to the file/folder
                    file_or_folder_path, file_or_folder_name = os.path.split(os.path.normpath(odc_dataset))
                    dataset_filename = os.path.join(file_or_folder_path,
                                                    '{}.odc-metadata.yaml'.format(file_or_folder_name))
                    with open(dataset_filename, 'w') as f:
                        yaml.dump(dataset_yaml, f, default_flow_style=False, sort_keys=False)
                    resolver = datacube.index.hl.Doc2Dataset(index=dc.index, eo3=True)
                    dataset = resolver(dataset_yaml, dataset_filename.as_uri())
                    dc.index.datasets.add(dataset[0])
                    logger.info("[{}/{}] Added dataset with id '{}' to the index".format(
                        idx_dataset,
                        len(odc_product_dataset_map[odc_product]),
                        dataset_id))
                idx_dataset = idx_dataset + 1
            idx_product = idx_product + 1
    return None


def main():
    args = parse_parameter()

    times_slept = 0
    db_conn_ok = False
    while times_slept < args.max_retries:
        # check "connection" to odc (?) or later
        logger.info("[{}/{}] Check database connection".format(str(times_slept + 1), str(args.max_retries)))
        if verify_database_connection(args.db, args.host, args.user, args.password, args.no_ping):
            db_conn_ok = True
            break
        time.sleep(args.sleep)
        times_slept = times_slept + 1

    if not db_conn_ok:
        exit(1024)

    ensure_odc_connection_and_database_initialization(args.db,
                                                      args.host,
                                                      args.user,
                                                      args.password,
                                                      '')

    global_data_folder = GLOBAL_DATA_FOLDER

    # Check data folder
    check_global_data_folder(global_data_folder)

    # Download datasets
    for dataset in DATASETS:
        loader = dataset[0](global_data_folder)
        loader.download(global_data_folder)

    # Add products and datasets to Open Data Cube index
    dc = datacube.Datacube(config=DATACUBE_CONF)
    add_datasets_to_odc(global_data_folder, dc)


if __name__ == '__main__':
    main()
