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
        # Opponents stats by team is not useful in my opinion, ignorign those files
        self.addPer36Min()
        self.addPer100Pos()
        self.addPlayerAward()

        self.addPlayerPlayByPlay()
        self.addPlayerShooting()
        self.addTeamSummaries()
        self.unique_player_record_df.drop_duplicates(inplace=True)
        self.unique_player_record_df.to_csv(os.path.join(self.data_path,"all_stats.csv"), header=True, index=False)
        
    def getUniquePlayerRecord(self) -> pd.DataFrame():
        
        player_df = pd.read_csv(os.path.join(self.raw_data_path,"Player Season Info.csv"), na_values=na_values, keep_default_na=False)
        player_df = player_df[["season","seas_id","player_id","player","birth_year","pos","age","tm","experience"]]
        
        # Fill age year by season - birth_year
        for i, row in player_df.iterrows():
            if row["birth_year"] != "NA" and row["age"] == "NA":
                player_df.at[i, "age"] = int(row["season"]) - int(row["birth_year"])
                
        return player_df

    def addAdvancedRecords(self):
        file_name = "Advanced.csv"
        df = pd.read_csv(os.path.join(self.raw_data_path, file_name), na_values=na_values, keep_default_na=False)
        
        # Drop unneccessary columns
        df = df.drop(["season","player_id","player","birth_year","pos","age","tm","experience", "lg"], axis=1)

        self.unique_player_record_df = pd.merge(self.unique_player_record_df, df, on=["seas_id"], how="left")

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
        df = df.drop(["season","player_id","player", "lg", "age", "type", "position", "tm", "pts_won", "pts_max", "number_tm"], axis=1)

        self.unique_player_record_df = pd.merge(self.unique_player_record_df, df, on=["seas_id"], how="left")
        # Replace NA with 0 assuming 0 votes received
        self.unique_player_record_df[["share","x1st_tm","x2nd_tm","x3rd_tm"]] = self.unique_player_record_df[["share","x1st_tm","x2nd_tm","x3rd_tm"]].replace({"NA": 0, np.nan: 0})


    def addEndOfSeasonTeams(self):
        file_name = "End of Season Teams.csv"
        df = pd.read_csv(os.path.join(self.raw_data_path, file_name), na_values=na_values, keep_default_na=False)
        
        # Drop unneccessary columns
        df = df.drop(["season","lg","player_id","player","position","birth_year","tm","age"], axis=1)
        
        # Replace ABA and BAA by NBA
        df["type"] = df["type"].replace({"All-ABA": "All-NBA", "All-BAA": "All-NBA"})

        
        awards = df["type"].drop_duplicates().to_list()
        temp_dfs = list()
        # Create a temp df for each award
        # Split into a few columns as 1 player can have mutliple awards
        for award in awards:
            temp_df = df[df["type"]==award]
            temp_df = temp_df.rename(columns={"number_tm": award})
            temp_df = temp_df.drop(["type"], axis=1)
            temp_df = temp_df.reset_index(drop=True)
            temp_dfs += [temp_df]
        
        # Add each result back to main df
        for temp_df in temp_dfs:
            self.unique_player_record_df = pd.merge(self.unique_player_record_df, temp_df, on=["seas_id"], how="left")

    def addPer36Min(self):
        file_name = "Per 36 Minutes.csv"
        df = pd.read_csv(os.path.join(self.raw_data_path, file_name), na_values=na_values, keep_default_na=False)
        
        # Drop unneccessary columns
        df = df.drop(["season","player_id", "player", "birth_year", "pos", "age", "experience", "lg", "g", "gs", "mp", "tm"], axis=1)
        
        advanced_stats = [
            "fg_per_36_min","fga_per_36_min","fg_percent","x3p_per_36_min","x3pa_per_36_min","x3p_percent","x2p_per_36_min",
            "x2pa_per_36_min",'x2p_percent',"ft_per_36_min","fta_per_36_min","ft_percent","orb_per_36_min","drb_per_36_min",
            "trb_per_36_min","ast_per_36_min","stl_per_36_min","blk_per_36_min","tov_per_36_min","pf_per_36_min","pts_per_36_min"
        ]
        
        # Replace NA with 0s
        df[advanced_stats] = df[advanced_stats].replace("NA", 0)
        df.rename(columns={
                                "fg_percent": "fg_percent_per_36_min", 
                                "ft_percent": "ft_percent_per_36_min",
                                "x2p_percent": "x2p_percent_per_36_min",
                                "x3p_percent": "x3p_percent_per_36_min"
                           })
        
        self.unique_player_record_df = pd.merge(self.unique_player_record_df, df, on=["seas_id"], how="left")
        
    def addPer100Pos(self):
        file_name = "Per 100 Poss.csv"
        df = pd.read_csv(os.path.join(self.raw_data_path, file_name), na_values=na_values, keep_default_na=False)
        
        # Drop unneccessary columns
        df = df.drop(["season","player_id", "player", "birth_year", "pos", "age", "experience", "lg", "g", "gs", "mp","tm"], axis=1)
        
        advanced_stats = [
            "fg_per_100_poss","fga_per_100_poss","fg_percent","x3p_per_100_poss","x3pa_per_100_poss","x3p_percent",'x2p_per_100_poss',
            "x2pa_per_100_poss","x2p_percent","ft_per_100_poss","fta_per_100_poss","ft_percent","orb_per_100_poss","drb_per_100_poss",
            "trb_per_100_poss","ast_per_100_poss","stl_per_100_poss","blk_per_100_poss","tov_per_100_poss","pf_per_100_poss","pts_per_100_poss",
            "o_rtg","d_rtg"
        ]
        
        # Replace NA with 0s
        df[advanced_stats] = df[advanced_stats].replace("NA", 0)
        df.rename(columns={
            "fg_percent": "fg_percent_per_100_poss", 
            "ft_percent": "ft_percent_per_100_poss",
            "x2p_percent": "x2p_percent_per_100_poss",
            "x3p_percent": "x3p_percent_per_100_poss",
            "o_rtg": "o_rtg_per_100_poss",
            "d_rtg": "d_rtg_per_100_poss"
        })

        self.unique_player_record_df = pd.merge(self.unique_player_record_df, df, on=["seas_id"], how="left")
        
    def addPlayerAward(self):
        file_name = "Player Award Shares.csv"
        df = pd.read_csv(os.path.join(self.raw_data_path, file_name), na_values=na_values, keep_default_na=False)
        
        # Drop unneccessary columns
        df = df.drop(["season","player", "age", "tm", "first", "pts_won", "pts_max", "player_id"], axis=1)
        
        df["award"] = df["award"].replace({"aba mvp": "nba mvp", "aba roy": "nba roy"})
        
        awards = df["award"].drop_duplicates().to_list()
        
        temp_dfs = list()
        # Create a temp df for each award
        # Split into a few columns as 1 player can have mutliple awards
        for award in awards:
            temp_df = df[df["award"]==award]
            temp_df = temp_df.rename(columns={"share": f"{award}_share", "winner": f"{award}_winner"})
            temp_df = temp_df.drop(["award"], axis=1)
            temp_df = temp_df.reset_index(drop=True)
            temp_dfs += [temp_df]
        
        # Add each result back to main df
        for temp_df in temp_dfs:
            self.unique_player_record_df = pd.merge(self.unique_player_record_df, temp_df, on=["seas_id"], how="left")
    
    def addPlayerPerGame(self):
        file_name = "Player Per Game.csv"
        df = pd.read_csv(os.path.join(self.raw_data_path, file_name), na_values=na_values, keep_default_na=False)
        
        # Drop unneccessary columns
        df = df.drop(["season","player_id","player","birth_year","pos","age","experience","lg","tm","g","gs"], axis=1)

        self.unique_player_record_df = pd.merge(self.unique_player_record_df, df, on=["seas_id"], how="left")
    
    def addPlayerPlayByPlay(self):
        file_name = "Player Play By Play.csv"
        df = pd.read_csv(os.path.join(self.raw_data_path, file_name), na_values=na_values, keep_default_na=False)
        
        # Drop unneccessary columns
        df = df.drop(["season","player_id","player","birth_year","pos","age","experience","lg","g","mp","tm"], axis=1)
        
        numerical_cols = [
            "pg_percent","sg_percent","sf_percent","pf_percent","c_percent","on_court_plus_minus_per_100_poss","net_plus_minus_per_100_poss",
            "bad_pass_turnover","lost_ball_turnover","shooting_foul_committed","offensive_foul_committed","shooting_foul_drawn",
            "offensive_foul_drawn","points_generated_by_assists","and1","fga_blocked"
        ]
        
        df[numerical_cols] = df[numerical_cols].replace("NA", 0)
        self.unique_player_record_df = pd.merge(self.unique_player_record_df, df, on=["seas_id"], how="left")
    
    def addPlayerShooting(self):
        file_name = "Player Shooting.csv"
        df = pd.read_csv(os.path.join(self.raw_data_path, file_name), na_values=na_values, keep_default_na=False)
        
        # Drop unneccessary columns
        df = df.drop(["season","player_id","player","birth_year","pos","age","experience","lg","g","mp","tm"], axis=1)
        
        numerical_cols = df.columns.tolist()
        numerical_cols.remove("seas_id")
        
        df[numerical_cols] = df[numerical_cols].replace("NA", 0)
        
        self.unique_player_record_df = pd.merge(self.unique_player_record_df, df, on=["seas_id"], how="left")

    def addTeamSummaries(self):
        file_name = "Team Summaries.csv"
        df = pd.read_csv(os.path.join(self.raw_data_path, file_name), na_values=na_values, keep_default_na=False)
        
        # Drop unneccessary columns
        df = df.drop(["lg","team","age","arena","attend","attend_g"], axis=1)
    
        numerical_cols = df.columns.tolist()
        numerical_cols.remove("season")
        numerical_cols.remove("playoffs")
        numerical_cols.remove("abbreviation")
        
        df[numerical_cols] = df[numerical_cols].replace("NA", 0)
        
        df = df.rename(columns={"abbreviation": "tm"})
        
        self.unique_player_record_df = pd.merge(self.unique_player_record_df, df, on=["season", "tm"], how="left")
    

    
if __name__ == "__main__":
    
    dp = DataPreprocessor()
    