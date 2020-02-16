import os
import pandas as pd
import json

class GeoLifeDataSet:
    folder_path = ''
    participants = [] #Contains references to all participants

    def __init__(self, path):
        self.folder_path = path

    def load_data_references(self):
        """ Loads references to all required data (recursively top-down) for further processing.

            For now it will assume the simple structure and fixed names as described in the data 
            manual of the GeoLife dataset.
        """
        with os.scandir(self.folder_path) as dir_iter:
            pot_participants = (entry for entry in dir_iter if entry.is_dir(follow_symlinks = False))

            for entry in pot_participants:
                print(entry.path)
                if GeoLifeParticipant.is_path_valid(entry.path):
                    self.participants.append( GeoLifeParticipant(entry.path) )

        for pt in self.participants:
            pt.load_data_references()

class GeoLifeParticipant:
    folder_path = ''
    id_key = 999999
    trajectories = []

    def __init__(self, path):
        self.folder_path = path

        pot_id_key = os.path.split(self.folder_path)[1] #Get the last folder name that indicates the participant ID
        try:
            self.id_key = int(pot_id_key)
            print( '-> Participant #{} found.'.format(self.id_key) )
        except ValueError:
            print( '-> Error in referencing potential participant folder: {}'.format(pot_id_key) )

    def load_data_references(self):
        trajectory_path = os.path.join(self.folder_path, 'Trajectory')

        with os.scandir(trajectory_path) as dir_iter:
            plt_file_path_iter = (entry for entry in dir_iter if entry.is_file() and os.path.splitext(entry.path)[1] == '.plt')

            for entry in plt_file_path_iter:
                self.trajectories.append( TrajectoryPLTFile(entry.path) )
    
    @staticmethod
    def is_path_valid(path):
        valid_flag = True

        # Check if path is directory
        if os.path.isdir(path) == False:
            valid_flag = False

        # Check if path contains a "Trajectory"-folder
        trajectory_path = os.path.join(path, 'Trajectory')

        if os.path.isdir( trajectory_path ) == False:
            valid_flag = False

        return valid_flag

class TrajectoryPLTFile:
    file_path = ''
    id_key = 999999
    trajectory_df = pd.DataFrame()

    def __init__(self, path):
        """ Load all the information and set the id_key
        """
        self.file_path = path
        
        file_name = os.path.split(self.file_path)[1]
        pot_id_key = os.path.splitext(file_name)[0]
        try:
            self.id_key = int(pot_id_key)
            print('--> Trajectory with ID {} found'.format(self.id_key))
        except ValueError:
            print('--> Error in referencing trajectory file: {}'.format(pot_id_key))
        pass

    def load_data(self):
        config = dict()

        # Read the config to check how many lines to skip in file
        with open('geolife_usr_config.json', 'r') as f:
            config = json.load(f)

        # Read as CSV
        self.trajectory_df = pd.read_csv(self.file_path, header=config['GeoLife_Specifics']['num_of_lines_to_skip'])

    def unbind_data(self):
        self.trajectory_df = pd.DataFrame()