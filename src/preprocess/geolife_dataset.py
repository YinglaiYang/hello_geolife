import os
import pandas as pd
import json

class GeoLifeDataSet:
    path = ''
    participants = [] #Contains references to all participants

    def __init__(self, path):
        self.path = path

    def load_data(self):
        """ Loads references to all required data (recursively top-down) for further processing.

            For now it will assume the simple structure and fixed names as described in the data 
            manual of the GeoLife dataset.
        """
        with os.scandir(self.path) as dir_iter:
            pot_participants = (entry for entry in dir_iter if entry.is_dir(follow_symlinks = False))

            for entry in pot_participants:
                pot_trajectories_path = os.path.join(entry.path, 'Trajectories')

                if os.path.isdir( pot_trajectories_path ):
                    self.participants.append( GeoLifeParticipant(pot_trajectories_path) )

class GeoLifeParticipant:
    id_key = 999999
    trajectories = []

    def __init__(self, path):
        pass

    def load_data(self):
        pass

class TrajectoryPLTFile:
    trajectory_df = pd.DataFrame()

    def __init(self, path):
        config = dict()

        with open('geolife_usr_config.json', 'r') as f:
            config = json.load(f)

        trajectory_df = pd.read_csv(path, header=config['GeoLife_Specifics']['num_of_lines_to_skip'])

    def write_to_parquet(self, path):
        #pd.write_to_parquet()
        pass