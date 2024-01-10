"""Module for preparing datasets.

This module provides functionality to download, merge, and clean datasets.
"""

import os
import logging
import src.utils as utils


class DatasetPreparer:
    """Class responsible for preparing the dataset.

    This class handles the downloading, merging, and cleaning of dataset files.

    Attributes:
        dataset_name (str): The name of the dataset.
        files (list): A list of file URLs to download.
        dataset_dir (str): The directory path where the dataset is stored.
    """
    def __init__(self, dataset_name, files):
        self.dataset_name = dataset_name
        self.files = files
        self.dataset_dir = f"data/{self.dataset_name}/"
        self.filename = "leipzig"
        logging.basicConfig(level=logging.INFO)

    def download(self):
        """Download the dataset files.

        This method checks if each file from the dataset's file list exists locally,
        and if not, it downloads the file. It also checks for empty files and
        re-downloads them if necessary.
        """
        logging.info("Starting download of files for dataset: %s", self.dataset_name)
        os.makedirs(self.dataset_dir, exist_ok=True)
        for file_url in self.files:
            filename = file_url.split('/')[-1]
            filepath = f"{self.dataset_dir}{filename}"
            if not os.path.isfile(filepath) or os.path.getsize(filepath) == 0:
                logging.info("Downloading file: %s", filename)
                utils.download_file(file_url, filepath)
            else:
                logging.info("File already exists and is not empty: %s", filename)

    def merge(self):
        """Merge all CSV files in the dataset directory into a single CSV file."""
        logging.info("Starting merge of CSV files in %s", self.dataset_dir)
        merged_file = f"{self.dataset_dir}{self.dataset_name}.csv"
        utils.merge_csv_files(f"{self.dataset_dir}*.csv", merged_file)
        logging.info("Merge completed for dataset: %s", self.dataset_name)

    def filter(self):
        """Filter the merged dataset file by removing lines with the 'rus' language."""
        logging.info("Starting filtering of dataset: %s", self.filename)
        filtered_file = f"{self.dataset_dir}{self.filename}_filtered.csv"
        utils.filter_and_write_lines(f"{self.dataset_dir}{self.filename}.csv", filtered_file)
        logging.info("Filtering completed for dataset: %s", self.dataset_name)

    def clean(self):
        """Clean the merged dataset file by removing unnecessary content."""
        logging.info("Starting cleaning of dataset: %s", self.dataset_name)
        csv_file = f"{self.dataset_dir}{self.dataset_name}.csv"
        txt_file = f"{self.dataset_dir}{self.dataset_name}.txt"
        utils.clean_file_contents(csv_file, txt_file)
        logging.info("Cleaning completed for dataset: %s", self.dataset_name)

    def run(self):
        """Run the dataset preparation process: download, merge, filter, and clean the dataset."""
        logging.info("Running dataset preparation for: %s", self.dataset_name)
        self.download()
        # self.merge()
        self.filter()
        self.clean()
        logging.info("Dataset preparation completed for: %s", self.dataset_name)
