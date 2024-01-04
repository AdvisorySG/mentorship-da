import pandas as pd
import numpy as np
from urllib.parse import urlparse, parse_qs, unquote


class parsing_urls:
    def __init__(self, input_csv_file):
        original_df = pd.read_csv(input_csv_file)

    def helper_extract_query_params(url):
        url = unquote(url) # make it human readable, not percentages
        query_params = parse_qs(urlparse(url).query)
        
        return query_params
    
    def extract_query_params(self):
        self.df = self.original_df.copy(deep=True)
        self.df['url'] = self.df['url'].astype(str)
        self.df['query_params'] = self.df['url'].apply(self.helper_extract_query_params) # gets all the query params and makes it a dictionary 

    def helper_process_query_params(params):
        '''
        Parameters:
        params -> {'size': ['n_20_n'], 'filters[0][field]': ['industries'], 'filters[0][values][0]': ['Information and Communications Technology'], 'filters[0][type]': ['all']}

        Returns:
        {
            "search_query": "search term",
            "filter_name": ["filter value 1", "filter value 2"]
        }

        Note:
        - The filter_name is the name of the filter, e.g. industries, school etc.
        - size and type are ignored
        '''
        result = {}
        current_field = ''

        for key, value in params.items():
            if 'filters' in key:
                parts = key.split('[')
                field_or_value = parts[2].strip(']')

                if field_or_value == 'field':
                    # if its a field then use it as a key
                    current_field = value[0]
                    result[current_field] = []
                elif field_or_value == 'type':
                    # there's a type of all in all queries, not sure what that is and whether its relevant
                    pass
                else:
                    # if its a value then add it to the list by using the last saved field
                    result[current_field].extend(value)
            elif key == "q":
                result["search_query"] = value[0]

        result = dict(result)
        return result
    
    def process_query_params(self):
        self.df_processed = self.df.copy(deep=True)
        self.df_processed['query_params'] = self.df_processed['query_params'].apply(self.helper_process_query_params) # use only 1 for loop
        self.df_processed = self.pd.DataFrame(self.df_processed['query_params'].values.tolist())
    
    def drop_unused_tables(self):
        # drop unused columns ['i', 'ind', 'cou', 'indust', 'o', 'sch', 'course_of']
        self.df_processed = self.df_processed.drop(['i', 'ind', 'cou', 'indust', 'o', 'sch', 'course_of'], axis=1)
        self.df_processed.info()

    def to_csv(self, output_csv_file):
        df_processed.to_csv(output_csv_file, index=False)
        

