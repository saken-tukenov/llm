
import os
import src.utils as utils


class DatasetPreparer:
    def __init__(self, dataset_name, files):
        self.dataset_name = dataset_name
        self.files = files
        self.dataset_dir = f"data/{self.dataset_name}/"

    def download(self):
        os.makedirs(self.dataset_dir, exist_ok=True)
        for file_url in self.files:
            filename = file_url.split('/')[-1]
            utils.download_file(file_url, f"{self.dataset_dir}{filename}")

    def merge(self):
        utils.merge_csv_files(f"{self.dataset_dir}*.csv", f"{self.dataset_dir}{self.dataset_name}.csv")

    def run(self):
        self.download()
        self.merge()
