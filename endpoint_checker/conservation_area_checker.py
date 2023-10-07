import os, time, sys,shutil
import pandas as pd
from digital_land.collection import Collection

from functions import run_endpoint_workflow
from sqlite_query_functions import DatasetSqlite
from convert_functions import convert_resource


def conservation_area_checker(url):
    collection_name = 'listed-building-collection'
    dataset = 'listed-building-outline'
    organisation = 'local-authority-eng:CAM'
    endpoint_url = url
    plugin = None
    additional_column_mappings=None
    additional_concats=None
    data_dir = 'data/endpoint_checker_' + str(int(time.time()*10000)) 

    run_endpoint_workflow(
        collection_name,
        dataset,
        organisation,
        endpoint_url,
        plugin,
        data_dir,
        additional_col_mappings=additional_column_mappings,
        additional_concats=additional_concats
    )

    collection = Collection(os.path.join(data_dir,'collection'))
    collection.load(directory=os.path.join(data_dir,'collection'))
    logs = collection.log.entries
    logs = pd.DataFrame.from_records(logs)

    http_status = logs['status'].values[0]

    if(http_status != 200): process_state = "Could not access URL"

    try: 
        dataset_db = DatasetSqlite(os.path.join(data_dir,'dataset',f'{dataset}.sqlite3'))
        results = dataset_db.get_issues()
        display(results)
        row_count = results.shape[0]

        if(row_count == 0):
            process_state = "No rows were processed, check you have created a valid file."
        else:
            process_state = f"{row_count} rows have been processed"

        issues = results.loc[(results.issue_type != 'default-value')]

    except:
        issues = pd.DataFrame()

   
    ret = {'process_state': process_state, 'issues': issues.to_dict('records')}
    
    shutil.rmtree(data_dir)

    return ret

