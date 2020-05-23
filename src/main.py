import preprocess.geolife_dataset as gldata
import json
import os

# The following import is only for performance measurement during development
from time import perf_counter

# The following imports will need to be capsuled into the target formats.
import pandas as pd

# The following imports are only used for exploratory purposes
import random
import matplotlib.pyplot as plt

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

    # Random numbers to determine which trajectory from which participant to visualize.
    p_nr = random.randrange(0, len(dataset.participants))
    participant = dataset.participants[p_nr]

    trj_nr = random.randrange(0, len(participant.trajectories))
    trj = participant.trajectories[trj_nr]

    print('Data from participant {} trajectory {}'.format(participant.id_key, trj.id_key))

    # Load the data of the trajectory
    trj.load_data(config)

    print(trj.trajectory_df)

    # Visualize with Pandas Scatter Plot
    ax = trj.trajectory_df.plot.scatter(x='longitude', y='latitude', c='excel_days', colormap='winter', marker='2')
    plt.show()
            

'''
    # Step through all data to transform to parquet basic.
    # ----------------------------------------------------
    timer_start = perf_counter()
    counter = 0
    num_participants = len(dataset.participants)

    for participant in dataset.participants:
        for trj in participant.trajectories:
            # Define the target file path for the converted format:
            convert_root_path = config['converted_dataset_root_paths']['parquet_basic']

            file_relative_path          = os.path.relpath( trj.file_path, start=config['GeoLife_Specifics']['dataset_root_path'] )
            file_relative_path_nameonly = os.path.splitext( file_relative_path )[0]
            
            target_file_path = os.path.join( convert_root_path, file_relative_path_nameonly + '.parquet' )

            # Create the file path directories if they do not exist
            target_dir_path = os.path.dirname(target_file_path)

            if not os.path.exists( target_dir_path ):
                os.makedirs( target_dir_path )

            # Get the data into a dataframe and save it in the target file path as a parquet file.
            trj_df = trj.get_data(config)
            trj_df.to_parquet( target_file_path )  

        # Finished all data for one participant, so tell it
        counter = counter + 1
        print('==> Finished transformations for {} participants out of {}'.format(counter, num_participants))

    timer_end = perf_counter()

    print('===============')
    print('The complete conversion took {}s.'.format(timer_end - timer_start))
'''