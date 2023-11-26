import pandas as pd
import os
import numpy as np
from fuzzywuzzy import fuzz

class SalaryStatsMatcher:
    def __init__(self):
        self.raw_data_path = "./data/raw_statistics"
        self.data_path = "./data"

        self.all_stats_df = pd.read_csv(os.path.join(self.data_path,"all_stats.csv"))
        self.salary_df =pd.read_csv(os.path.join(self.data_path, "nba_player_salaries.csv")) 

        # match format with all_stats_df
        self.salary_df["season"] = self.salary_df["Year"].apply(lambda x: int(str(x).split("-")[0]))
        self.salary_df.rename(columns={"Player Name": "player", "Salary (Adjusted)": "salary"}, inplace=True)
        self.salary_df.drop(["Year","Salary (Unadjusted)"], axis=1, inplace=True)
        # change back to float instead of str
        self.salary_df["salary"] = self.salary_df["salary"].apply(lambda x: float(str(x).replace(",","").replace("$","")))
        
        self.nameMatching()
        # remove player column which is not used as pk
        self.salary_df.drop(["player"], axis=1, inplace=True)
        
        all_stats_df = pd.read_csv(os.path.join(self.data_path, "all_stats.csv"))
        final_df = pd.merge(all_stats_df, self.salary_df, on=["season", "player_id"], how="left")
        # drop na salary if as salary is the target
        final_df = final_df.dropna(subset=["salary"])
        
        final_df.to_csv(os.path.join(self.data_path, "overall_stats_salary.csv"), header=True, index=False)
        
    def nameMatching(self):
        # This is a unique list from original raw data source
        stat_players = pd.read_csv(os.path.join(self.raw_data_path,"Player Season Info.csv"))
        stat_players = stat_players[["season", "player_id","player"]]
        
        # using only data from 1990 to match salary
        stat_players = stat_players[stat_players["season"] >= 1990]
        
        # standardize name formatting
        stat_players["player"] = stat_players["player"].apply(lambda x: str(x).strip().lower())
        self.salary_df["player"] = self.salary_df["player"].apply(lambda x: str(x).strip().lower())
        
        repeated_dict = dict()
        # Filter out players with identical names
        for name in stat_players["player"].drop_duplicates().to_list():
            temp_df = stat_players[stat_players["player"]==name]
            if temp_df["player_id"].nunique() > 1:
                repeated_dict[name] = temp_df["player_id"].drop_duplicates().to_list()
        print(len(repeated_dict))
        print(repeated_dict)
        
        ####### Prepare the distinguishing years repeated
        career_info = pd.read_csv(os.path.join(self.raw_data_path, "Player Career Info.csv"))
        career_info = career_info[career_info["last_seas"] >= 1990]
        repeated_players_distinguishing_years = dict()
        
        # Loop through the repeated names to collect distinguishing years
        for player_name, player_ids in repeated_dict.items():
            temp_dict_id = dict()
            distinguishing_years = dict()
            
            for index, player_id in enumerate(player_ids):
                # filter career info to this person
                temp_df = career_info[career_info["player_id"]==player_id]
                # Set career time as range
                first_seas = temp_df["first_seas"].values[0]
                last_seas = temp_df["last_seas"].values[0]
                if index == 0:
                    min_year = first_seas
                    max_year = last_seas = last_seas
                else:
                    min_year = first_seas if first_seas < min_year else min_year
                    max_year = last_seas if last_seas > max_year else max_year
                    
                temp_dict_id[player_id] = range(first_seas, last_seas+1)

                # Now that all years player played in are collected, select distinguishing years for each person
                distinguishing_years[player_id] = list()
            
            # From min year to max year, check if the year only exists in one of the ranges
            for year in range(min_year, max_year+1):
                matching_ids = list()
                for player_id in player_ids:
                    if year in temp_dict_id[player_id]:
                        matching_ids += [player_id]
                # Add year record if that year has only 1 person with repeated name playing
                if len(matching_ids) == 1:
                    distinguishing_years[matching_ids[0]] += [year]
            repeated_players_distinguishing_years[player_name] = distinguishing_years
        
        # Initialize player_id column for filling if found
        self.salary_df["player_id"] = np.nan
        need_fuzzy_names = list()
        confirmed_ids = list()
        
        for i, row in self.salary_df.iterrows():
            salary_name, salary_season = row["player"], row["season"]
            
            # Case 1: duplicated names
            if salary_name in repeated_dict.keys():
                # logic is to check the years their playing time can be distinguished, e.g. A played in 1990 - 1994 and B played in 1994 - 2003
                # The distinguishing years are 1990 - 1993 and 1995 - 2023
                
                for id, years in repeated_players_distinguishing_years[salary_name].items():
                    if salary_season in years:
                        self.salary_df.at[i, "player_id"] = id
                        confirmed_ids = confirmed_ids + [id] if id not in confirmed_ids else confirmed_ids
                        break
            
            # Case 2: direct match
            elif salary_name in stat_players["player"].values:
                id = stat_players[stat_players["player"] == salary_name]["player_id"].values[0]
                self.salary_df.at[i, "player_id"] = id
                confirmed_ids = confirmed_ids + [id] if id not in confirmed_ids else confirmed_ids
            
            # Case 3: no direct match - saved for fuzzy matching
            else:
                need_fuzzy_names += [(salary_name, salary_season)]
        
        # Performing fuzzy matching for remaining results
        not_confirmed_stat_player = stat_players[~stat_players["player_id"].isin(confirmed_ids)]
        # avoid duplicated selection of names
        fuzzy_matched_ids = list()
            
        for name, season in need_fuzzy_names:
            scores = dict()
            # Filter to that season
            temp_not_confirmed_stat_player = not_confirmed_stat_player[not_confirmed_stat_player["season"]==season]
            
            for i, row in temp_not_confirmed_stat_player.iterrows():
                # performing fuzzy matching for all unconfirmed names
                fuzz_score = fuzz.ratio(name, row["player"])
                scores[(row["player"], row["player_id"])] = fuzz_score
            
            # sort in descending order to show player with highest score
            scores = dict(sorted(scores.items(), key=lambda x: x[1], reverse=True))
            
            # set logic to check if the result is accepted
            
            np_scores = np.array(list(scores.values()))
            first_id = list(scores.keys())[0][1]
            # Case 0: nothing inside
            if np_scores.size == 0:
                pass
            # Case 1: only 1 name
            elif len(np_scores) == 1:
                print(np_scores)
                if np_scores[0] > 50:
                    self.salary_df.loc[self.salary_df['player'] == name, "player_id"] = first_id
                    fuzzy_matched_ids += [first_id]
            # Case 2: more than 1 name
            elif len(np_scores) > 1: 
                if np_scores[0] > 50 and (np_scores[0] - np_scores[1]) > np_scores.std() and first_id not in fuzzy_matched_ids:
                    self.salary_df.loc[self.salary_df['player'] == name, "player_id"] = first_id
                    fuzzy_matched_ids += [first_id]
        
if __name__ == "__main__":
    SSM = SalaryStatsMatcher()