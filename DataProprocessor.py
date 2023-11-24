import pandas as pd
import numpy as np
import os
import warnings

from util import na_values

class DataPreprocessor:

    """
    Mainly for cleaning and combining all raw data files and combine into 1 file
    """

    def __init__(self):
        # ignore pd warnings when reading large data files
        warnings.filterwarnings('ignore')
        self.raw_data_path = "./data/raw_statistics"
        self.data_path = "./data"

        # get a unique list of season+player record
        self.unique_player_record_df = self.getUniquePlayerRecord()

        # retain only record for years which we have salary data
        salary_df = pd.read_csv(os.path.join(self.data_path, "nba_player_salaries.csv"))
        years = salary_df["Year"].drop_duplicates().to_list()
        year_range = [int(str(year).split("-")[0]) for year in years]

        self.unique_player_record_df = self.unique_player_record_df[self.unique_player_record_df["season"].isin(year_range)]

        # Processing files 1-by-1 as different file requires slightly different processing
        self.addAdvancedRecords()
        self.addAllStarSelection()
        self.addEndOfSeasonTeamsVoting()
        self.addEndOfSeasonTeams()

    def getUniquePlayerRecord(self) -> pd.DataFrame():
        """
        A set of composite primary keys are generated using Advanced.csv and Player Totals.csv
        The columns chosen are season, player_id, player
        Remarks:
        seas_id not chosen as it is not identical across files
        season (year) + player_id = best unique keys
        player (name) = used to identify player if player_id is not present in file
        tm (team) -> same player can play for multiple teams in the same season
        """

        # Read the two files as Dataframes
        advanced_df = pd.read_csv(os.path.join(self.raw_data_path,"Advanced.csv"), na_values=na_values, keep_default_na=False)
        player_total_df = pd.read_csv(os.path.join(self.raw_data_path,"Player Totals.csv"), na_values=na_values, keep_default_na=False)

        # Define composite primary keys
        self.pks = ["season", "player_id", "player", "tm"]

        # filter two Dataframes to the pks
        advanced_df = advanced_df[self.pks]
        player_total_df = player_total_df[self.pks]

        # Concatenate two dfs and select unique values
        return pd.concat([advanced_df, player_total_df]).drop_duplicates(["season", "player_id","tm"], keep="first").reset_index(drop=True)

    def addAdvancedRecords(self):
        file_name = "Advanced.csv"
        df = pd.read_csv(os.path.join(self.raw_data_path, file_name), na_values=na_values, keep_default_na=False)
        
        # Drop unneccessary columns
        df = df.drop(["seas_id", "lg"], axis=1)

        # Fill age year by season - birth_year
        for i, row in df.iterrows():
            if row["birth_year"] != "NA" and row["age"] == "NA":
                df.at[i, "age"] = int(row["season"]) - int(row["birth_year"])

        self.unique_player_record_df = pd.merge(self.unique_player_record_df, df, on=self.pks, how="left")

    def addAllStarSelection(self):
        """
        Fill 1 for all start players and 0 for not all star
        """
        file_name = "All-Star Selections.csv"
        df = pd.read_csv(os.path.join(self.raw_data_path, file_name), na_values=na_values, keep_default_na=False)

        df = df[["player", "season"]]
        df["All Star?"] = 1
        
        # Combining the dataframes, players selected as all star will be tagged 1 and remaining will be NaN
        self.unique_player_record_df = pd.merge(self.unique_player_record_df, df, on=['player', 'season'], how='left')
        self.unique_player_record_df["All Star?"].fillna(0, inplace=True)
    
    def addEndOfSeasonTeamsVoting(self):
        file_name = "End of Season Teams (Voting).csv"
        df = pd.read_csv(os.path.join(self.raw_data_path, file_name), na_values=na_values, keep_default_na=False)
        
        # Drop unneccessary columns
        # number_tm is duplicated with End Of Season Teams so removed
        df = df.drop(["seas_id", "lg", "age", "type", "position", "tm", "pts_won", "pts_max", "number_tm"], axis=1)

        self.unique_player_record_df = pd.merge(self.unique_player_record_df, df, on=["season", "player_id", "player"], how="left")
        # Replace NA with 0 assuming 0 votes received
        self.unique_player_record_df[["share","x1st_tm","x2nd_tm","x3rd_tm"]] = self.unique_player_record_df[["share","x1st_tm","x2nd_tm","x3rd_tm"]].replace({"NA": 0, np.nan: 0})
    

    def addEndOfSeasonTeams(self):
        file_name = "End of Season Teams.csv"
        df = pd.read_csv(os.path.join(self.raw_data_path, file_name), na_values=na_values, keep_default_na=False)
        
        # Drop unneccessary columns
        df = df.drop(["lg","position","seas_id","birth_year","tm","age"], axis=1)
        
        # Replace ABA and BAA by NBA
        df["type"] = df["type"].replace({"All-ABA": "All-NBA", "All-BAA": "All-NBA"})

        # Split into a few columns as 1 player can have mutliple awards
        awards = df["type"].drop_duplicates().to_list()

        temp_dfs = list()

        for award in awards:
            temp_df = df[df["type"]==award]
            temp_df = temp_df.rename(columns={"number_tm": award})
            temp_df = temp_df.drop(["type"], axis=1)
            temp_dfs += [temp_df]
        
        for temp_df in temp_dfs:
            print(temp_df.head(5))

        print(self.unique_player_record_df.head(5))

if __name__ == "__main__":
    dp = DataPreprocessor()
    