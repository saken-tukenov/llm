from src.dataset_preparer import DatasetPreparer

class MultidomainKazakhDataset(DatasetPreparer):
    def __init__(self):
        files = [
            'https://huggingface.co/datasets/kz-transformers/multidomain-kazakh-dataset/resolve/main/kazakhNews.csv',
            'https://huggingface.co/datasets/kz-transformers/multidomain-kazakh-dataset/resolve/main/kazakhBooks.csv',
            'https://huggingface.co/datasets/kz-transformers/multidomain-kazakh-dataset/resolve/main/leipzig.csv',
            'https://huggingface.co/datasets/kz-transformers/multidomain-kazakh-dataset/resolve/main/oscar.csv',
            'https://huggingface.co/datasets/kz-transformers/multidomain-kazakh-dataset/resolve/main/cc100-monolingual-crawled-data.csv'
        ]
        super().__init__('multidomain-kazakh-dataset', files)

