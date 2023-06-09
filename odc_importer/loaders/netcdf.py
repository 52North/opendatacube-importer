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
import logging
import os
import uuid

import numpy as np
import rioxarray
import xarray

from .base import BaseLoader

logger = logging.getLogger(__name__)


class NetCDFLoader(BaseLoader):
    """
    Base class for loading NetCDF data.
    """
    # Has to be defined by child classes!
    measurement_dict = {}

    def __init__(self):
        super().__init__()
        if not self.measurement_dict:
            logger.warning("Attribute 'measurement_dict' is empty!")

    def create_product_dataset_map(self, global_data_folder):
        """
        Create a dictionary with product names as keys and lists of datasets (full path to file) as values
        :param global_data_folder:
        """

        data_folder = os.path.join(global_data_folder, self.folder)
        product_dataset_map = {}

        # get file names including path for each odc product
        # include only '.nc' files (there might be yaml files stored next to them)
        files = os.listdir(data_folder)
        datasets = [os.path.join(data_folder, element)
                    for element in files if element.endswith('.nc')]
        product_dataset_map.update({self.product_names[0]: datasets})

        # Check odc product names
        for odc_product in self.product_names:
            if odc_product not in product_dataset_map.keys():
                logger.info(f"Product '{odc_product}' is missing in product-dataset-map!")

        self.product_dataset_map = product_dataset_map

    def create_product_metadata_eo3(self, odc_product_name):
        """
        Create product metadata in eo3 metadata format
        :param odc_product_name: product name
        :return: `dict` of product metadata
        """

        # Metadata dictionary to create product yaml
        product_metadata = {
            'name': odc_product_name,
            'description': odc_product_name,
            'metadata': self._create_metadata_document(odc_product_name),
            'measurements': self.measurement_dict,
        }
        return product_metadata

    def create_dataset_metadata_eo3(self, odc_product_name, dataset):
        """
        Create dataset metadata in eo3 metadata format
        :param odc_product_name: odc product name
        :param dataset: file name with full path
        :return: `dict` of dataset metadata
        """

        metadata = self._create_metadata_document(odc_product_name)

        # Extract geo-information from file
        # Read the dataset with decode_coords='all' to get crs
        ds_nc = xarray.open_dataset(dataset, decode_coords='all')

        # Extract polygon geometry from the dataset using numpy array
        latitudes = np.array(ds_nc.coords['latitude'][:])
        longitudes = np.array(ds_nc.coords['longitude'][:])
        min_lat = float(min(latitudes))
        max_lat = float(max(latitudes))
        min_lon = float(min(longitudes))
        max_lon = float(max(longitudes))
        polygon = [[[min_lat, min_lon], [min_lat, max_lon], [
            max_lat, max_lon], [max_lat, min_lon], [min_lat, min_lon]]]

        # Extract the transformation matrix
        dataset_transform = list(ds_nc.rio.transform())

        # Extract the shape of dataset
        ds_shape = [ds_nc.rio.shape[1], ds_nc.rio.shape[0]]

        # Generate dictionary for all dataset measurements
        measurement_dict = {}
        for var in self.measurement_dict.keys():
            measurement_dict[var] = {
                'path': dataset,
                'layer': var
            }

        # Metadata dictionary to create dataset yaml
        dataset_metadata = {
            'id': str(uuid.uuid5(uuid.NAMESPACE_URL, dataset)),
            'schema': 'https://schemas.opendatacube.org/dataset',
            'product_name': 'waves',
            'crs': str(ds_nc.rio.crs),
            'polygon': polygon,
            'shape': ds_shape,
            'transform': dataset_transform,
            'bands': measurement_dict,
            'platform': 'na',
            'instrument': 'na',
            'datetime': str(ds_nc.time.values[0]),
            'processing_datetime': str(ds_nc.time.values[0]),
            'file_format': 'NETCDF',
            'lineage': {}}

        # 'additions': {
        #     'keywords': metadata['keywords'],
        #     'links': metadata['links']
        # }

        return dataset_metadata

    def download(self, global_data_folder):
        raise NotImplementedError("Implement this method in subclass!")

    def _create_metadata_document(self, odc_product_name):
        return {
            'product': {
                'name': odc_product_name
            }
        }
