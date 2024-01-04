import pandas as pd
import numpy as np
from urllib.parse import urlparse, parse_qs, unquote



def helper_extract_query_params(url):
    url = unquote(url) # make it human readable, not percentages
    query_params = parse_qs(urlparse(url).query)
    
    return query_params
    
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

input_csv_file = input('enter the input csv file name')
output_csv_file = input('enter the output csv file name')

original_df = pd.read_csv(input_csv_file)
df = original_df.copy(deep=True)
df['url'] = df['url'].astype(str)
df['query_params'] = df['url'].apply(extract_query_params) # gets all the query params and makes it a dictionary 
df_processed = df.copy(deep=True)
df_processed['query_params'] = df_processed['query_params'].apply(process_query_params) # use only 1 for loop
df_processed = pd.DataFrame(df_processed['query_params'].values.tolist())
df_processed = df_processed.drop(['i', 'ind', 'cou', 'indust', 'o', 'sch', 'course_of'], axis=1) #drop unused columns
df_processed.to_csv(output_csv_file, index=False)