class DatasetInfoSourceInterface(object):
    """
    Interface specs for probe to the dataset information source.
    """

    def __init__(self):
        pass

    def set_dataset_details(self, datasets):
        """
        Set detailed information, primarily those that may be updated.
        """
        pass
