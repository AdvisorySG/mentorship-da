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

    def process_mentors_data(self, dataframes): 
        df = pd.concat(dataframes)
        df['year'] = np.concatenate([np.full(len(df_i), year_i) for df_i, year_i in dataframes]) # TODO: make dataframes into dictionary {2020: df_2020w3, 2021: df_2021w1, 2022: df_2022w1}, so that it contains both info of years and respective waves
        df.columns = ['mentor_name', 'year']
        df = df[df['mentor_name'] != '[INSERT NAME LIST OF WAVE 3 MENTORS]']

        df['industries'] = ""
        df['organisation'] = ""

        for index, row in df.iterrows():
            name = row['mentor_name'].lower()
            year = row['year']

            if name in self.mentors:
                mentor = self.mentors[name]
            else:
                mentor = self.get_mentor_info_by_name(name)

            if mentor is not None:
                row['industries'] = mentor['industries']
                row['organisation'] = mentor['organisation']
            else:
                self.unknown_mentors.append(name)

        return df

    def save_data_to_csv(self, df, filename):
        df.to_csv(filename, index=False)

if __name__ == '__main__':
    load_dotenv('03AnalysisApplicationData/.env')
    CLOUD_ID = os.getenv('CLOUD_ID')
    PASSWORD = os.getenv('PASSWORD')
    USER = os.getenv('USER')

    mentorship_system = ProcessApplicationData(USER, CLOUD_ID, PASSWORD)    

    # Load CSV dataframes
    df_2020w3 = pd.read_csv('03AnalysisApplicationData/data/2020w3.csv')
    df_2021w1 = pd.read_csv('03AnalysisApplicationData/data/2021w1.csv')
    df_2022 = pd.read_csv('03AnalysisApplicationData/data/2022.csv')

    # Process and save mentor data to CSV
    dataframes = [df_2020w3, df_2021w1, df_2022]
    mentors_df = mentorship_system.process_mentors_data(dataframes)
    mentorship_system.save_data_to_csv(mentors_df, '03AnalysisApplicationData/data/mentors.csv')