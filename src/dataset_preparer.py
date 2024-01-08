
import os
import logging
import src.utils as utils


class DatasetPreparer:
    def __init__(self, dataset_name, files):
        self.dataset_name = dataset_name
        self.files = files
        self.dataset_dir = f"data/{self.dataset_name}/"
        logging.basicConfig(level=logging.INFO)

    def download(self):
        logging.info(f"Starting download of files for dataset: {self.dataset_name}")
        os.makedirs(self.dataset_dir, exist_ok=True)
        for file_url in self.files:
            filename = file_url.split('/')[-1]
            filepath = f"{self.dataset_dir}{filename}"
            if not os.path.isfile(filepath) or os.path.getsize(filepath) == 0:
                logging.info(f"Downloading file: {filename}")
                utils.download_file(file_url, filepath)
            else:
                logging.info(f"File already exists and is not empty: {filename}")

    def merge(self):
        logging.info(f"Starting merge of CSV files in {self.dataset_dir}")
        utils.merge_csv_files(f"{self.dataset_dir}*.csv", f"{self.dataset_dir}{self.dataset_name}.csv")
        logging.info(f"Merge completed for dataset: {self.dataset_name}")
    
    def clean(self):
        logging.info(f"Starting cleaning of dataset: {self.dataset_name}")
        utils.clean_file_contents(f"{self.dataset_dir}{self.dataset_name}.csv", f"{self.dataset_dir}{self.dataset_name}.txt")
        logging.info(f"Cleaning completed for dataset: {self.dataset_name}")

    def run(self):
        logging.info(f"Running dataset preparation for: {self.dataset_name}")
        self.download()
        self.merge()
        logging.info(f"Dataset preparation completed for: {self.dataset_name}")
