# Cronos Metadata GraphQL API 

## The script is structured in 5 main parts:

### 1. Setting up the Tableau API connection
### 2. Defininig the GraphQL queries and extracting the info
    I have created 4 main Queries:
    - Workbooks --> Datasources --> Dashboards --> Worksheets --> Fields
    - Workbooks --> Parameters --> referenced by Calculations
    - Workbooks --> Datasources --> Datasource Filters
    - Databases --> Tables --> DB Columns --> Dashboards
### 3. Transform the nested json structure to a dataframe using the pandas json_normalize module
### 4. Save locally the file created as json, csv, and hyper 
### 5. Publish the hyper files as published datasources on the Server, so we will get updated data everytime we run this script
    This creates 4 Published Datasources on the Server
    - GraphQL | WB DS Dashboards Sheets Calculations
    - GraphQL | WB DS Filters
    - GraphQL | WB Parameters
    - GraphQL | DB Tables Dashboards

    If the Publishing Process doesn't complete successfully, the user will get details on the error that arose, including error code and error description.
    I have added a loop that adds an extra column in each of the Published Datasources with the info on when the Python script was run, so to know how updated that information is. 
    Re-running the script without altering the Datasource Name and the Project Folder will replace the current datasources with updated data.