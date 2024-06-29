# 
# Script to process mentorship data from CSV files and save to CSV
# # TODO: NOT TESTED YET (getting credentials)
# 
# 1. Set up /data with Application from different waves and their respective files
# 2. Create env file with your credentials for Elastic Cloud
# - CLOUD_ID=<CLOUD_ID>
# - PASSWORD=<PASSWORD>
# - USER=<USER>
# 3. Run script, which produces mentors.csv and mentors_per_application files in /data
# 
# Written by: Jolene
# 
# everyth before the "Analysis on basic counts of application data"


import pandas as pd
import numpy as np
from elasticsearch import Elasticsearch
import re
from dotenv import load_dotenv
import os

class ProcessApplicationData:
    def __init__(self, user, cloud_id, password):
        self.es = Elasticsearch(
            cloud_id=cloud_id,
            basic_auth=(user, password)
        )
        self.mentors = {}
        self.unknown_mentors = []

    def search_documents(self, index, query_body):
        result = self.es.search(index=index, body=query_body)
        return result

    def get_mentor_info_by_name(self, name):#Get information for the processed mentor
        search_options = {
            'query': {
                'bool': {
                    'should': []
                }
            }
        }

        # Add match query for the name
        search_options['query']['bool']['should'].append({
            'match': {
                'name': name
            }
        })

        # Search by organization if name includes organization information in brackets
        organization = re.search(r'\((.*?)\)', name)
        if organization:
            organization_name = organization.group(1)
            # Add match query for the organisation
            search_options['query']['bool']['should'].append({
                'match': {
                    'organisation': organization_name
                }
            })

        result = self.search_documents('enterprise-search-engine-mentorship-page', search_options)
        exact_matches = [doc for doc in result['hits']['hits']]

        if len(exact_matches) == 0:
            return None

        if 'organisation' in exact_matches[0]['_source']:
            return {
                'name': exact_matches[0]['_source']['name'],
                'industries': exact_matches[0]['_source']['industries'],
                'organisation': exact_matches[0]['_source']['organisation']
            }
        else:
            return {
                'name': exact_matches[0]['_source']['name'],
                'industries': exact_matches[0]['_source']['industries'],
                'organisation': None
            }

    def check_same_name(self, name):
        search_options = {
            'query': {
                'match': {
                    'name': name
                }
            }
        }
        result = self.search_documents('enterprise-search-engine-mentorship-page', search_options)
        documents = result['hits']['hits']
        if len(documents) > 1:
            return True
        else:
            return False

    def process_mentors_data(self, dataframes, years): 
        print("Processing Mentors Data")
        print("Preparing Data from CSV")
        # combine all 3 columns into 1
        df = [pd.concat([df['mentor_1'],df['mentor_2'],df['mentor_3']]) for df in dataframes]
            
        #df_2020w3 = pd.concat([df_2020w3['mentor_1'], df_2020w3['mentor_2'], df_2020w3['mentor_3']])
        #df_2021w1 = pd.concat([df_2021w1['mentor_1'], df_2021w1['mentor_2'], df_2021w1['mentor_3']])
        #df_2022 = pd.concat([df_2022['mentor_1'], df_2022['mentor_2'], df_2022['mentor_3']])
        
        for i in range (len(dataframes)):
            df[i] = df[i].to_frame().drop_duplicates()
            df[i] = df[i].assign(year=np.full(len(df[i]), years[i]))
        df = pd.concat(df)
        df = df.reset_index(drop=True)

        # combine all 3 dataframes into 1 and add a column for the year        
        #df = pd.concat(dataframes)
        #for i in range(len(years)):
        #    df['year'] = np.concatenate([np.full(len(df[i]), years[i])]) # TODO: make dataframes into dictionary {2020: df_2020w3, 2021: df_2021w1, 2022: df_2022w1}, so that it contains both info of years and respective waves
        
        # rename first column to mentor_name
        df.columns = ['mentor_name', 'year']

        # remove the row with [INSERT NAME LIST OF WAVE 3 MENTORS]
        df = df[df['mentor_name'] != '[INSERT NAME LIST OF WAVE 3 MENTORS]']

        # Clean data (remove NaN rows)
        df = df.dropna()

        df['industries'] = ""
        df['organisation'] = ""

        print("Retriving Mentor Data")
        for index, row in df.iterrows():
            name = row['mentor_name'].lower()

            if name in self.mentors:
                mentor = self.mentors[name]
            else:
                mentor = self.get_mentor_info_by_name(name)

            if mentor is not None:
                df.at[index, 'industries'] = mentor['industries']
                df.at[index, 'organisation'] = mentor['organisation']
            else:
                self.unknown_mentors.append(name)
        print("Function Complete")
        
        return df
    
    def process_mentors_data_per_application(self, dataframes, years): 
        print("Processing Mentors Data Per Appplication")
        print("Preparing Data from CSV")
        df = dataframes
        # Add in the year column and populate it with the year
        for i in range (len(dataframes)):
            df[i] = df[i].assign(year=np.full(len(df[i]), years[i]))
        df = pd.concat(df)
        df = df.reset_index(drop=True)
        
        # rename first 3 columns to mentor_1, mentor_2 and mentor_3
        df.columns = ['mentor_1', 'mentor_2', 'mentor_3', 'year']

        # remove the row with [INSERT NAME LIST OF WAVE 3 MENTORS]
        df = df[df['mentor_1'] != '[INSERT NAME LIST OF WAVE 3 MENTORS]']

        # Clean data (remove NaN rows)
        df = df.dropna()

        df['mentor_1_industries'] = ""
        df['mentor_1_organisation'] = ""
        df['mentor_2_industries'] = ""
        df['mentor_2_organisation'] = ""
        df['mentor_3_industries'] = ""
        df['mentor_3_organisation'] = ""

        print("Retriving Mentor Data")
        for index, row in df.iterrows():
            for i in range(1, 4):
                name = row[f'mentor_{i}'].lower()

                if name in self.mentors:
                    mentor = self.mentors[name]
                else:
                    mentor = self.get_mentor_info_by_name(name)

                if mentor is not None:
                    df.at[index, f"mentor_{i}_industries"] = mentor["industries"]
                    df.at[index, f"mentor_{i}_organisation"] = mentor["organisation"]
                else:
                    self.unknown_mentors.append(name)
        
        
        print("Function Complete")
        return df

    def save_data_to_csv(self, df, filename):
        print("Saving data to CSV")
        df.to_csv(filename, index=False)

if __name__ == '__main__':
    # Load dotenv variables
    load_dotenv('03AnalysisApplicationData/.env')
    CLOUD_ID = os.getenv('CLOUD_ID')
    PASSWORD = os.getenv('PASSWORD')
    USER = os.getenv('USER')
    assert CLOUD_ID is not None, "CLOUD_ID is not set"
    assert PASSWORD is not None, "PASSWORD is not set"
    assert USER is not None, "USER is not set"

    mentorship_system = ProcessApplicationData(USER, CLOUD_ID, PASSWORD)    
    
    # CSV to read CHANGE HERE TO UPDATE NEW CSV
    csv_filename = ["2020w3", "2021w1","2022"]

    # Load CSV dataframes
    dataframes = [pd.read_csv(f'03AnalysisApplicationData/data/{filename}.csv') for filename in csv_filename]

    # get first 3 columns of all files and rename them to mentor_1, mentor_2, mentor_3
    dataframes = [df.iloc[:, 0:3] for df in dataframes]
    for df in dataframes:
        df.columns = ['mentor_1', 'mentor_2', 'mentor_3']

    mentors_df = mentorship_system.process_mentors_data(dataframes, csv_filename)
    mentorship_system.save_data_to_csv(mentors_df, '03AnalysisApplicationData/data/mentors.csv')
    mentors_df = mentorship_system.process_mentors_data_per_application(dataframes, csv_filename)
    mentorship_system.save_data_to_csv(mentors_df, '03AnalysisApplicationData/data/mentors_per_application.csv')