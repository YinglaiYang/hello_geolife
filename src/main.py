import preprocess.geolife_dataset as gldata
import json

if __name__ == '__main__':
    # Load JSON config
    config = {}

    with open('geolife_usr_config.json', 'r') as f:
        config = json.load(f)

    # Crawl through dataset
    dataset = gldata.GeoLifeDataSet(config['GeoLife_Specifics']['dataset_path'])

    dataset.load_data_references()