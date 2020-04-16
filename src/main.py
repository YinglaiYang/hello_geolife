import preprocess.geolife_dataset as gldata
import json
import os

if __name__ == '__main__':
    # Load JSON config
    config = {}

    with open('geolife_usr_config.json', 'r') as f:
        config = json.load(f)

    # Crawl through dataset
    # =====================

    # Setup the crawler first.
    # ------------------------
    dataset = gldata.GeoLifeDataSet(config['GeoLife_Specifics']['dataset_root_path'])

    dataset.load_data_references()

    print(dataset.participants[0].trajectories[0].file_path)
    print( os.path.relpath(dataset.participants[0].trajectories[0].file_path, start=config['GeoLife_Specifics']['dataset_root_path']) )

    # Step through all data to transform to parquet basic.
    # ----------------------------------------------------
    for participant in dataset.participants:
        for trj in participant.trajectories:
            trj.load_data(config)

            # Define the target file path for the converted format:
            #target_file_path = format(config['GeoLife_Specifics']['converted_dataset_path']).

            trj.unbind_data()