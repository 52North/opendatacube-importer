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
class BaseLoader:

    def __init__(self):
        self.product_dataset_map = {}
        self.product_names = []
        self.force_download = False

    def create_dataset_metadata_eo3(self, odc_product_name, dataset):
        """
        :param odc_product_name: str, product name
        :param dataset: str, can be a file or a folder (e.g. when bands are in separate files like for Landsat 8)
        :return: dict
            the dictionary needs to contain several key-value pairs, see odc.create_dataset_yaml_eo3 for details
        """
        raise NotImplementedError("Implement this method in subclass!")

    def create_product_dataset_map(self, global_data_folder):
        """Optional method to create a product-dataset-map"""
        # Method should manipulate object attribute:
        # self.product_dataset_map = product_dataset_map
        pass

    def create_product_metadata_eo3(self, odc_product_name):
        """
        :param odc_product_name: str, product name
        :return: dict
            the dictionary needs to contain several key-value pairs, see odc.create_product_yaml_eo3 for details
        """
        raise NotImplementedError("Implement this method in subclass!")

    def download(self, global_data_folder):
        raise NotImplementedError("Implement this method in subclass!")

    def get_folder(self):
        return self.folder

    def get_product_dataset_map(self):
        """
        :return: dictionary with product names as keys and lists of datasets (full path to file or folder) as values
        """
        return self.product_dataset_map

    def get_product_names(self):
        return self.product_names