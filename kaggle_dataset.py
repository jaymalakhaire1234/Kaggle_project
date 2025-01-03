import kaggle
import pandas as pd
import zipfile

def download_dataset_from_kaggle():
    try:
        kaggle.api.authenticate()

        kaggle.api.dataset_download_file('ankitbansal06/retail-orders', 'orders.csv', path='.')
        print("Dataset downloaded and extracted successfully")
    except Exception as e:
        print(f"Failed to download dataset: {e}")
    
def extract_zip_file():
    try:
        zip_ref = zipfile.ZipFile('orders.csv.zip') 
        zip_ref.extractall('.') 
        zip_ref.close()
        print("dataset extracted successfully")
    except Exception as e:
        print(e)

def main():
    download_dataset_from_kaggle()
    extract_zip_file()

if __name__=='__main__':
    main()


