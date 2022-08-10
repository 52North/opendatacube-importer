class BasicLoader:

    def __init__(self):
        self.product_dataset_map = {}
        self.product_names = []
        self.force_download = False

    def create_dataset_metadata_eo3(self, odc_product_name, dataset):
        """
        :param odc_product_name:
        :param dataset: str, can be a file or a folder (e.g. when bands are in separate files like for Landsat 8)
        :return:
        """
        raise NotImplementedError("Implement this method in subclass!")

    def create_product_dataset_map(self, global_data_folder):
        """Optional method to create a product-dataset-map"""
        # Method should manipulate object attribute:
        # self.product_dataset_map = product_dataset_map
        pass

    def create_product_metadata_eo3(self, odc_product_name):
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
