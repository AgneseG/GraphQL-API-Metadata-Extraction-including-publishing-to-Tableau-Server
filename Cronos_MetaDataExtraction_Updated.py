from distutils.log import error
import pandas as pd
from pandas.io.json import json_normalize
from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils.querying import get_projects_dataframe
import json
import os
import pantab
from datetime import datetime

############################################################################################################################################
# 1. Setting up the Tableau API connection
############################################################################################################################################

TABLEAU_SITE = 'YOUR TABLEAU SITE'
TOKEN_NAME = 'YOUR TOKEN NAME'
TOKEN_VALUE = 'YOUR TOKEN NAME'

ts_config = {
        'my_env': {
                'server': TABLEAU_SITE,
                'api_version': '3.12',
                'personal_access_token_name': TOKEN_NAME,
                'personal_access_token_secret': TOKEN_VALUE,
                'site_name': '',
                'site_url': ''
        }
}

conn = TableauServerConnection(ts_config, env='my_env')
conn.sign_in()


############################################################################################################################################
# 2. Defininig the GraphQL queries and extracting the info
############################################################################################################################################

### 1st table: Workbooks --> Embedded Datasources --> Calculated fields

query_ds_calculations = """
{ 
    workbooks {
      embeddedDatasources {
        downstreamDashboards{
            sheets{    
                worksheetFields      {
                FieldID: id
                FieldName: name
                isHidden
                    ... on CalculatedField     {
                        formula
                                }
                }
            SheetName: name
            SheetId: id
                         }
          DashboardName: name
          DashboardID: id                   
                         }
      DSName: name
      DSId: id
      hasExtracts
     }
    WorkbookName: name
    WorkbookID: id  
    projectName                                                                                        }
}
"""

# 2nd table: Workbooks --> Parameters --> Referenced by Calculations

query_parameters = """
{
  workbooks {
      parameters {
        ParameterName: name
        ParameterID: id
            referencedByCalculations{
                CalculationName: name
                CalculationID: id
            }
                         }
    WorkbookName: name
    WorkbookID: id
    projectName                                                                                          }
}
"""

# 3rd table: Workbooks --> EMbedded datasources --> Datasource filters 

query_ds_filters = """
{
    workbooks {
      embeddedDatasources {
        datasourceFilters {
            id
            field{
                id
                name
            }
        }
    DSName: name
    DSId: id
                         }
    WorkbookName: name
    WorkbookID: id  
    projectName
    projectVizportalUrlId                                                                                         }
}
"""

# 4th table: Database --> Tables --> Columns --> Dashboards 

query_tables = """{
    databases {
        DBName: name
        DBID: id
        DBConnectionType: connectionType
        DBDescription: description
        tables {
            TableName: name
            TableID: id
            TableDescription: description
            columns {
                ColumnID: id
                ColumnName: name
                ColumnRemoteType: remoteType
                downstreamDashboards{
                    DashboardName: name
                    DashboardID: id
                    DashboardLuid: luid
                    DashboardPath: path
                }
            }
        }
        }
  }
"""

# Function to extract the Server info based on a graphQL query
def run_query(query_syntax):
    query_results = conn.metadata_graphql_query(query_syntax)
    return(query_results.json())

# Run the function and save results
ds_calculations = run_query(query_ds_calculations)
parameters = run_query(query_parameters)
ds_filters = run_query(query_ds_filters)
ds_tables = run_query(query_tables)

# Check json structure
print(ds_tables)
print(ds_calculations)   

############################################################################################################################################
# We will now transform the nested json structure to a dataframe using the pandas json_normalize module
############################################################################################################################################

ds_calculations_flat = pd.json_normalize(ds_calculations['data'], 
                record_path = ['workbooks', 'embeddedDatasources', 'downstreamDashboards', 'sheets', 'worksheetFields'], 
                meta = [['workbooks','WorkbookID'], 
                        ['workbooks','WorkbookName'],
                        ['workbooks','projectName'],
                        ['workbooks', 'embeddedDatasources', 'DSId'],
                        ['workbooks', 'embeddedDatasources', 'DSName'],
                        ['workbooks', 'embeddedDatasources', 'hasExtracts'],
                        ['workbooks', 'embeddedDatasources', 'downstreamDashboards', 'DashboardName'],
                        ['workbooks', 'embeddedDatasources', 'downstreamDashboards', 'DashboardID'],
                        ['workbooks', 'embeddedDatasources', 'downstreamDashboards', 'sheets', 'SheetName'],
                        ['workbooks', 'embeddedDatasources', 'downstreamDashboards', 'sheets', 'SheetID']
                ],
                errors='ignore')

# Check columns                
print(list(ds_calculations_flat.columns))
ds_calculations_flat["workbooks.embeddedDatasources.hasExtracts"] = ds_calculations_flat["workbooks.embeddedDatasources.hasExtracts"].astype(str)       

ds_parameter_flat = pd.json_normalize(parameters['data'], 
                record_path = ['workbooks', 'parameters', 'referencedByCalculations'],
                meta = [['workbooks','WorkbookID'], 
                        ['workbooks','WorkbookName'],
                        ['workbooks','projectName'],
                        ['workbooks', 'parameters', 'ParameterName'],
                        ['workbooks', 'parameters', 'ParameterID']],
                errors='ignore')

ds_filters_flat = pd.json_normalize(ds_filters['data'], 
                record_path = ['workbooks', 'embeddedDatasources','datasourceFilters'], 
                meta = [['workbooks','WorkbookID'], 
                        ['workbooks','WorkbookName'],
                        ['workbooks','projectName'],
                        ['workbooks', 'embeddedDatasources', 'DSId'],
                        ['workbooks', 'embeddedDatasources', 'DSName']],
                errors='ignore')


db_info_flat = pd.json_normalize(ds_tables['data'], 
                record_path = ['databases', 'tables', 'columns', 'downstreamDashboards'],
                meta = [['databases','DBName'], 
                        ['databases','DBID'],
                        ['databases','DBConnectionType'],
                        ['databases','DBDescription'],
                        ['databases', 'tables', 'TableName'],
                        ['databases', 'tables', 'TableID'],
                        ['databases', 'tables', 'TableDescription'],
                        ['databases', 'tables', 'columns', 'ColumnName'],
                        ['databases', 'tables', 'columns', 'ColumnID'],
                        ['databases', 'tables', 'columns', 'ColumnRemoteType'],
                        ],
                errors='ignore'        
                )                            

# Check structure of the dataframe                
print(db_info_flat) 

############################################################################################################################################
# Save locally the file created as json, csv, and hyper 
############################################################################################################################################

df_list = [ds_calculations_flat,
            ds_parameter_flat,
            ds_filters_flat,
            db_info_flat]

print('The files will be saved here: ' + os.getcwd())

## As csv
df_list[0].to_csv('calculations_workbooks_ds.csv')
df_list[1].to_csv('calculations_parameters.csv') 
df_list[2].to_csv('workbooks_ds_filters.csv') 
df_list[3].to_csv('db_table_dashboard.csv') 

## As hyper file: we will create both 
# 1) One hyper file with multiple tables 
# 2) One individual hyper file for each table (this will be needed to publish the tables as published datasources)

# Before that, I will add a column to each df with the current date and time, so to track when this Python script was run
for ids, x in enumerate(df_list):
    df_list[ids].insert(0, 'Last Python Run', datetime.now())

# Defining the hyper file names
single_hyper = "Cronos_MetaDataExtraction.hyper"
hyper_names = ['WB_DS_Dashboards_Sheets_Calculations.hyper',
                'WB_DS_DSFilters.hyper',
                'WB_Parameters.hyper',
                'DB_ConnectionInfo_to_usage_in_Dashboards.hyper']

# Setting up the table names for the multi-table extract
dict_df = {"wb_ds_calculations": ds_calculations_flat, 
            "wb_parameters": ds_parameter_flat,
            "wb_ds_filters": ds_filters_flat,
            "db_table_dashboard": db_info_flat}

# Multi-table Extract
pantab.frames_to_hyper(dict_df, single_hyper)     

# Single-table Extracts
pantab.frame_to_hyper(ds_calculations_flat, hyper_names[0], table = 'wb_ds_calculations')
pantab.frame_to_hyper(ds_filters_flat, hyper_names[1], table = 'wb_ds_filters')
pantab.frame_to_hyper(ds_parameter_flat, hyper_names[2], table = 'wb_parameters')
pantab.frame_to_hyper(db_info_flat, hyper_names[3], table = 'db_table_dashboard')


## This is only if you also want to have them as json files
with open('calculations_workbooks_ds.json', 'w') as f:
    json.dump(ds_calculations, f)

with open('calculations_parameters.json', 'w') as f:
    json.dump(parameters, f)  

with open('workbooks_ds_filters.json', 'w') as f:
    json.dump(ds_filters, f)  

with open('db_tables_workbooks.json', 'w') as f:
    json.dump(ds_tables, f)

############################################################################################################################################
# Publish the hyper files as published datasources on the Server, so we will get updated data everytime we run this script
############################################################################################################################################

# Project list from the Server
projects_df = get_projects_dataframe(conn)
print(list(projects_df.columns)) 
# Keep only Project Name and ID
project_id_list = projects_df[['name', 'id']]

print(f"Signing into Tableau Server and publishing on " + ts_config['my_env']['server'])

# Name for our published datasources
ds_names = ['GraphQL | WB DS Dashboards Sheets Calculations', 
            'GraphQL | WB DS Filters',
            'GraphQL | WB Parameters',
            'GraphQL | DB Tables Dashboards']

# Define the project name. If we want the DSs to be published in different Project Folders, we need to create a list for the ds_project object too (like ds_names)
ds_project = 'Full Data Usage Pipeline'

# Extract the Project ID 
project_id = project_id_list.loc[project_id_list['name'] == ds_project, 'id'].iloc[0]

# Publishing to Server
for ids, x in enumerate(ds_names):
    response = conn.publish_data_source(
                    datasource_file_path = hyper_names[ids],
                    datasource_name = ds_names[ids],
                    project_id = project_id)
    if response.status_code==201:
        print("Datasource '{0}' published. Datasource ID: {1}".format(
                                                                    response.json()['datasource']['name'], 
                                                                    response.json()['datasource']['id']
                                                                    )
        )
    else:
        print("{0} FAILED with error {1} ({2}). \nDetails: {3}".format(
                                                                        db_table_dashboard, 
                                                                        str(response.status_code),
                                                                        response.json()['error']['summary'],
                                                                        response.json()['error']['detail']
                                                                        )
        )




