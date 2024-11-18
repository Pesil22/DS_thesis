import pandas as pd
from datetime import datetime, timedelta
import os
import dash
import dash_bootstrap_components as dbc
from dash import html, dcc, Input, Output, State, dash_table, no_update
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go
from google.cloud import storage
import io

# -------------------- Configuration --------------------

# Google Cloud Storage Configuration
# Replace these with your actual bucket names
PRIMARY_BUCKET = "primary-data-bucket"
OUTPUT_BUCKET = "output-data-bucket"
MANUAL_OUTPUT_BUCKET = "manual-output-bucket"

# Initialize Google Cloud Storage client
storage_client = storage.Client(project='noble-radio-442111-d5')

# Reference to the buckets
primary_bucket = storage_client.bucket(PRIMARY_BUCKET)
output_bucket = storage_client.bucket(OUTPUT_BUCKET)
manual_output_bucket = storage_client.bucket(MANUAL_OUTPUT_BUCKET)

# Starting date for manual variables (should be changed based on process data start potentially?)
start_date_str = "2023-12-11"  # Format: YYYY-MM-DD
start_date = datetime.strptime(start_date_str, "%Y-%m-%d")

# List CSV files in the primary bucket
def list_csv_files(bucket, prefix=""):
    blobs = bucket.list_blobs(prefix=prefix)
    return [blob.name for blob in blobs if blob.name.endswith('.csv')]

# Filter files by date
def filter_files_by_date(files, start_date, end_date):
    filtered_files = []
    for file in files:
        try:
            # Assuming the date is the third part separated by '_'
            file_parts = file.split('_')
            if len(file_parts) < 3:
                continue
            file_date_str = file_parts[2]
            file_date = datetime.strptime(file_date_str, "%Y%m%d")
            if start_date <= file_date <= end_date:
                filtered_files.append(file)
        except Exception as e:
            print(f"Error processing file {file}: {e}")
    return filtered_files

# Function to sanitize filenames
def sanitize_filename(filename):
    """
    Sanitize the filename by removing or replacing invalid characters.
    """
    return "".join(c if c.isalnum() or c in (' ', '_', '-') else '_' for c in filename).rstrip()

# Function to get variables by type from manual_output_bucket
def get_variables_by_type(data_type):
    """
    Retrieve variable names from manual_output_bucket based on the data type.
    """
    suffix = f"_{data_type}.csv"
    files = [f for f in list_csv_files(manual_output_bucket) if f.endswith(suffix)]
    variables = [f[:-len(suffix)] for f in files]
    return variables

# Function to extract prefixes from saved files in output_bucket
def extract_prefixes_from_saved_files(bucket):
    prefixes = set()
    files = list_csv_files(bucket)
    for file in files:
        if file.endswith('.csv'):
            prefix = file.split('_')[0]
            prefixes.add(prefix)
    return sorted(list(prefixes))

# Function to remove outliers using the Interquartile Range (IQR) method
def remove_outliers(df, column):
    if column in skip_variables:
        # Skip outlier removal for variables in the skip list
        return df
    
    # Apply IQR outlier removal for other variables
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    return df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]

# Function to list manual variables excluding '_binary' and '_string'
def list_manual_variables(bucket):
    variables = []
    files = list_csv_files(bucket)
    for file in files:
        if file.endswith('.csv') and not (file.endswith('_binary.csv') or file.endswith('_string.csv')):
            var_name = file.split('/')[-1][:-4]  # Remove directory prefix and .csv
            variables.append(var_name)
    return variables

# Function to List Gantt Manual Variables
def list_gantt_manual_variables(bucket):
    """
    List variables ending with '_binary' and '_string' from the manual output bucket.
    """
    variables = set()
    files = list_csv_files(bucket)
    for file in files:
        if file.endswith('.csv') and (file.endswith('_binary.csv') or file.endswith('_string.csv')):
            var_name = file.split('/')[-1][:-11]  # Remove '_binary.csv' or '_string.csv'
            variables.add(var_name)
    return sorted(list(variables))

# -------------------- Data Loading and Processing Offline Analytical Data --------------------

# Path to your CSV file in the primary bucket (assuming this is for initial data)
initial_csv_file = "2024-10-04_Results_Cell-Content_Medium_Tech_RS-FV-New.csv"

def read_csv_from_gcs(bucket, file_name):
    blob = bucket.blob(file_name)
    data = blob.download_as_text()
    return pd.read_csv(io.StringIO(data), sep=';', header=None).T

# Read the CSV without headers and transpose
df_transposed = read_csv_from_gcs(primary_bucket, initial_csv_file)

# Set the first row as column headers
df_transposed.columns = df_transposed.iloc[0]
df_transposed = df_transposed[1:].reset_index(drop=True)

# Convert all columns to string to ensure consistency
df_transposed = df_transposed.astype(str)

# Split the DataFrame into table1 and table2
table1 = df_transposed.iloc[:, 1:20].copy().reset_index(drop=True)
table2 = df_transposed.iloc[:, 21:35].copy().reset_index(drop=True)

# Function to add DateTime column
def add_datetime_column(table, start_date):
    if 'Sample Day' in table.columns:
        sample_day_col = table['Sample Day'].str.replace(',', '.').astype(float)
        sample_day_timedelta = pd.to_timedelta(sample_day_col, unit='D')
        datetime_col = pd.to_datetime(start_date) + sample_day_timedelta + pd.to_timedelta('00:00:00')
        datetime_col_formatted = datetime_col.dt.strftime('%d-%m-%Y %H:%M:%S')
        table['DateTime'] = datetime_col_formatted
    else:
        print("Error: 'Sample Day' column not found in the table.")
    return table

# Apply the function to both tables
table1 = add_datetime_column(table1, start_date)
table2 = add_datetime_column(table2, start_date)

# Convert 'DateTime' to datetime objects
table1['DateTime'] = pd.to_datetime(table1['DateTime'], format='%d-%m-%Y %H:%M:%S')
table2['DateTime'] = pd.to_datetime(table2['DateTime'], format='%d-%m-%Y %H:%M:%S')

# Exclude columns we don't want to process
exclude_columns = ['Sample Day', 'SAMPLE I.D', 'DateTime']

# Process numeric columns in table1
data_columns_table1 = [col for col in table1.columns if col not in exclude_columns and pd.notnull(col)]
for col in data_columns_table1:
    table1[col] = table1[col].str.replace(',', '.')
    table1[col] = pd.to_numeric(table1[col], errors='coerce')

# Process numeric columns in table2
data_columns_table2 = [col for col in table2.columns if col not in exclude_columns and pd.notnull(col)]
for col in data_columns_table2:
    table2[col] = table2[col].str.replace(',', '.')
    table2[col] = pd.to_numeric(table2[col], errors='coerce')

# Now select numeric columns
numeric_columns_table1 = table1.select_dtypes(include=['float64', 'int64'])
numeric_columns_table2 = table2.select_dtypes(include=['float64', 'int64'])

# Get the list of columns for dropdown options
# We prefix the column names with the table name to avoid duplicates
columns_table1_prefixed = [f"Table1: {col}" for col in numeric_columns_table1.columns if col not in exclude_columns]
columns_table2_prefixed = [f"Table2: {col}" for col in numeric_columns_table2.columns if col not in exclude_columns]

# Combine all columns
all_columns = columns_table1_prefixed + columns_table2_prefixed

# Create a mapping from display names to actual column names and tables
column_mapping = {}
for display_name, col_name in zip(columns_table1_prefixed, numeric_columns_table1.columns):
    column_mapping[display_name] = ('table1', col_name)
for display_name, col_name in zip(columns_table2_prefixed, numeric_columns_table2.columns):
    column_mapping[display_name] = ('table2', col_name)

# List of variable names to filter
variable_names = [
    "AI Values_78TT001 - Analog input",
    "AI Values_78TT002 - Analog input",
    "AI Values_10TT002 - Analog input",
    "AI Values_20TTC001 - Analog input",
    "AI Values_20FTC003 - analog input",
    "AI Values_78FT001 - Analog input",
    "AI Values_20FTC002 - Analog input",
    "AI Values_20XTC001 - Analog input",
    "AI Values_20XTC002 - Analog input",
    "AI Values_20XT004 - Analog input",
    "AI Values_20XTC003 - Analog input",
    "AI Values_10PT001 - Analog input",
    "30P001.HMI.DATA_2",
    "30P002.HMI.DATA_2",
    "30P001.HMI.STATUS",
    "AO Values_10R001",
    "AO Values_10R002",
    "AO Values_10R003",
    "AI Values_20PT004 - Analog input",
    "AI Values_78PT002 - Analog input",
    "AI Values_78PT001 - Analog input",
]

# Unit mappings for variables
variable_units = {
    "AI Values_78TT001 - Analog input": "Temperature (°C)",
    "AI Values_78TT002 - Analog input": "Temperature (°C)",
    "AI Values_10TT002 - Analog input": "Temperature (°C)",
    "AI Values_20TTC001 - Analog input": "Temperature (°C)",
    "AI Values_20FTC003 - analog input": "Flowrate normal L/min",
    "AI Values_78FT001 - Analog input": "Flowrate m3/h",
    "AI Values_20FTC002 - Analog input": "Flowrate m3/h",
    "AI Values_20XTC001 - Analog input": "pH",
    "AI Values_20XTC002 - Analog input": "pH",
    "AI Values_20XT004 - Analog input": "Dissolved oxygen (%)",
    "AI Values_20XTC003 - Analog input": "Dissolved oxygen (%)",
    "AI Values_10PT001 - Analog input": "Pressure (bar?)",
    "30P001.HMI.DATA_2": "Pump state (ON/OFF)",
    "30P002.HMI.DATA_2": "Pump state (ON/OFF)",
    "30P001.HMI.STATUS": "Pump state (?)",
    "AO Values_10R001": "Light intensity (%)",
    "AO Values_10R002": "Light intensity (%)",
    "AO Values_10R003": "Light intensity (%)",
    "AI Values_20PT004 - Analog input": "Pressure (bar)",
    "AI Values_78PT002 - Analog input": "Pressure (bar)",
    "AI Values_78PT001 - Analog input": "Pressure (bar)",
}

# Display names for variables
variable_display_names = {
    "AI Values_78TT001 - Analog input": "Cooling circuit, before PBR (°C)",
    "AI Values_78TT002 - Analog input": "Before the PBR (°C)",
    "AI Values_10TT002 - Analog input": "Cooling circuit, after PBR (°C)",
    "AI Values_20TTC001 - Analog input": "After the PBR (°C)",
    "AI Values_20FTC003 - analog input": "CO2 inlet (normal L/min)",
    "AI Values_78FT001 - Analog input": "Cooling fluid  (Flowrate m3/h)",
    "AI Values_20FTC002 - Analog input": "From degasser to PBR (Flowrate m3/h)",
    "AI Values_20XTC001 - Analog input": "After the PBR1 (pH)",
    "AI Values_20XTC002 - Analog input": "After the PBR2 (pH)",
    "AI Values_20XT004 - Analog input": "Before the PBR (Dissolved oxygen %)",
    "AI Values_20XTC003 - Analog input": "After the PBR (Dissolved oxygen %)",
    "AI Values_10PT001 - Analog input": "Inside the PBR? Pressure (bar?)",
    "30P001.HMI.DATA_2": "Nutrient drum (ON/OFF)",
    "30P002.HMI.DATA_2": "IBC (ON/OFF)",
    "30P001.HMI.STATUS": "Nutrient drum (Pump state ?)",
    "AO Values_10R001": "Top layer (Light intensity %)",
    "AO Values_10R002": "Middle layer (Light intensity %)",
    "AO Values_10R003": "Bottom layer (Light intensity %)",
    "AI Values_20PT004 - Analog input": "After the PBR Pressure (bar)",
    "AI Values_78PT002 - Analog input": "Cooling circuit, after PBR Pressure (bar)",
    "AI Values_78PT001 - Analog input": "Cooling circuit, before PBR Pressure (bar)",
}

# New variable units for offline analytics
new_variable_units = {
    "Table1: % CARBOHYDRATE": "%",
    "Table1: % PROTEIN": "%",
    "Table1: % OIL": "%",
    "Table1: EPA % DM": "%",
    "Table1: EPA % FA": "%",
    "Table1: Total saturated %": "%",
    "Table1: Total saturated mg. 100g-1 ": "mg. 100g-1",
    "Table1: Total monounsaturated % total fatty acids": "%",
    "Table1: Total monounsaturated mg. 100g-1": "mg. 100g-1",
    "Table1: Total n-6 PUFA % total fatty acids": "%",
    "Table1: Total n-6 PUFA mg. 100g-1": "mg. 100g-1",
    "Table1: Total n-3 PUFA % total fatty acids": "%",
    "Table1: Total n-3 PUFA mg. 100g-1": "mg. 100g-1",
    "Table1: Total PUFA % total fatty acids": "%",
    "Table1: Total PUFA mg. 100g-1": "mg. 100g-1",
    "Table1: Total FA % DM": "%",
    "Table2: Sodium": "mg/kg",
    "Table2: Magnesium": "mg/kg",
    "Table2: Phosphorus": "mg/kg",
    "Table2: Potassium": "mg/kg",
    "Table2: Calcium": "mg/kg",
    "Table2: Mangan": "mg/kg",
    "Table2: Iron": "mg/kg",
    "Table2: Copper": "mg/kg",
    "Table2: Zinc": "mg/kg",
    "Table2: Arsenic": "mg/kg",
    "Table2: Lead": "mg/kg",
}

# List of variables to skip for outlier removal
skip_variables = [
    "30P001.HMI.DATA_2",  # Binary variable
    "30P002.HMI.DATA_2",  # Binary variable
    "30P001.HMI.STATUS",  # Binary variable
    "AO Values_10R001",   # Percentage variable
    "AO Values_10R002",   # Percentage variable 
    "AO Values_10R003",   # Percentage variable 
]

# Dictionary to store merged DataFrames for all variables
merged_dataframes = {var: pd.DataFrame() for var in variable_names}

# Store filename prefixes entered by the user
filename_prefixes = []

# Function to process a single CSV file and extract variables into DataFrames
def process_csv_file(bucket, file_name, variable_names):
    blob = bucket.blob(file_name)
    data = blob.download_as_text()
    df = pd.read_csv(io.StringIO(data), delimiter=';', on_bad_lines='skip')  
    dataframes = {}
    for var_name in variable_names:
        filtered_df = df[df['VarName'] == var_name]
        if not filtered_df.empty:
            dataframes[var_name] = filtered_df
            print(f"Extracted {var_name} with {len(filtered_df)} rows.")
    return dataframes

# Initialize the Dash app with Bootstrap stylesheet
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.title = "Algiecel Pilot Dashboard"
server = app.server

# Function to get manual variable options
def get_manual_variable_options():
    manual_vars = list_manual_variables(manual_output_bucket)
    return [{'label': var, 'value': var} for var in manual_vars]

# Layout of the Dash app
app.layout = dbc.Container([
    # Hidden stores to keep track of variables and data entries
    dcc.Store(id='variables-store', data={}),  # Stores variables with their attributes
    dcc.Store(id='data-entries-store', data=[]),  # Stores data entries
    dcc.Store(id='current-graph-data', data=[]),  # Stores graph data for export

    dbc.Row([], style={'height': '5px'}),  # Halved gap between the top and the content

    dbc.Row([
        # Left Column: Batch & Variable Selection, ButtonGroup for Toggles, Define Batch, Create Variable, and Data Entry
        dbc.Col([
            # Batch & Variable Selection Section
            dbc.Card([
                dbc.CardHeader(html.H4('Batch & Variable Selection')),
                dbc.CardBody([
                    # Prefix Dropdown
                    dbc.FormGroup([
                        dbc.Label('Select Batch'),
                        dcc.Dropdown(
                            id='prefix-dropdown',
                            placeholder='Select batch',
                            multi=True
                        ),
                    ]),
                    # Variable Dropdown (process)
                    dbc.FormGroup([
                        dbc.Label('Select Process Data Variable(s)'),
                        dcc.Dropdown(
                            id='variable-dropdown',
                            placeholder='Select variable',
                            multi=True
                        ),
                    ]),
                    # Variables Dropdown (analytics)
                    dbc.FormGroup([
                        dbc.Label('Select Offline Data Variable(s)'),
                        dcc.Dropdown(
                            id='new-variable-dropdown',
                            options=[{'label': col, 'value': col} for col in new_variable_units.keys()],
                            placeholder='Select variable',
                            multi=True
                        ),
                    ]),
                    # Variable Dropdown (manual)
                    dbc.FormGroup([
                        dbc.Label('Select Manual Variable(s)'),
                        dcc.Dropdown(
                            id='manual-variable-dropdown',
                            options=get_manual_variable_options(),
                            placeholder='Select manual variable',
                            multi=True
                        ),
                    ]),
                    # Time Mode Switch
                    dbc.FormGroup([
                        dbc.Label('Time Mode'),
                        dbc.RadioItems(
                            id='time-mode-switch',
                            options=[
                                {'label': 'Absolute Time', 'value': 'absolute'},
                                {'label': 'Elapsed Time', 'value': 'elapsed'},
                            ],
                            value='absolute',
                            inline=True
                        ),
                    ]),
                ])
            ], className='mb-2'),  

            # ButtonGroup for Toggling Sections
            dbc.ButtonGroup([
                dbc.Button(
                    "Show Define Batch",
                    id='toggle-define-batch-button',
                    color='primary',  # Changed from 'success' to 'primary'
                    className='mr-1',  
                    n_clicks=0,
                ),
                dbc.Button(
                    "Show Create Variable",
                    id='toggle-create-variable-button',
                    color='primary',  # Changed from 'success' to 'primary'
                    className='mr-1',  
                    n_clicks=0,
                ),
                dbc.Button(
                    "Show Data Entry",
                    id='toggle-data-entry-button',
                    color='primary',  # Changed from 'success' to 'primary'
                    className='mr-1',  
                    n_clicks=0,
                ),
            ], className='mb-1'),  

            # Define Batch Section (Initially Hidden)
            dbc.Card([
                dbc.CardHeader(html.H3('Define Batch')),
                dbc.CardBody([
                    dbc.FormGroup([
                        dbc.Label('Select Date Range'),
                        dcc.DatePickerRange(
                            id='date-picker-range',
                            start_date=datetime(2024, 1, 1),
                            end_date=datetime(2024, 12, 31),
                            display_format='YYYY-MM-DD',
                            style={'width': '100%'}
                        ),
                    ]),
                    dbc.FormGroup([
                        dbc.Label('Enter Batch Name'),
                        dbc.Input(
                            id='filename-input',
                            type='text',
                            placeholder='Enter batch name',
                        ),
                    ]),
                    dbc.Button('Confirm', id='confirm-button', color='primary', block=True, className='mt-1'),  
                    html.Div(id='file-list', style={'whiteSpace': 'pre-line', 'marginTop': '5px'}),  
                    html.Div(id='file-save-status', style={'marginTop': '5px', 'color': 'green'}),  
                ])
            ], id='define-batch-card', className='mb-2', style={'display': 'none'}),  

            # Create Variable Section (Initially Hidden)
            dbc.Card([
                dbc.CardHeader(html.H4('Create Variable')),
                dbc.CardBody([
                    dbc.FormGroup([
                        dbc.Label('Variable'),
                        dbc.Input(
                            id='variable-name-input',
                            type='text',
                            placeholder='Enter variable name',
                        ),
                    ]),
                    dbc.FormGroup([
                        dbc.Label('Data Type'),
                        dcc.Dropdown(
                            id='data-type-dropdown',
                            options=[
                                {'label': 'Float', 'value': 'float'},
                                {'label': 'Percentage', 'value': 'percentage'},
                                {'label': 'String', 'value': 'string'},
                                {'label': 'Binary', 'value': 'binary'},
                            ],
                            placeholder='Select data type',
                        ),
                    ]),
                    dbc.Button('Create Variable', id='create-variable-button', color='primary', block=True, className='mt-1'),  
                    html.Div(id='create-variable-message', className='mt-1'),  
                ])
            ], id='create-variable-card', className='mb-2', style={'display': 'none'}),  

            # Data Entry Tabs Section (Initially Hidden)
            dbc.Card([
                dbc.CardHeader(html.H4('Data Entry')),
                dbc.CardBody([
                    dbc.Tabs([
                        # 1. Float Data Tab
                        dbc.Tab(label="Add Float Data", children=[
                            dbc.FormGroup([
                                dbc.Label('Select Variable'),
                                dcc.Dropdown(
                                    id='float-variable-dropdown',
                                    placeholder='Select variable',
                                    options=[{'label': var, 'value': var} for var in get_variables_by_type('float')],
                                ),
                            ]),
                            dbc.FormGroup([
                                dbc.Label('Enter Value'),
                                dbc.Input(
                                    id='float-value-input',
                                    type='number',
                                    placeholder='Enter float value',
                                ),
                            ]),
                            dbc.FormGroup([
                                dbc.Label('Select Units'),
                                dcc.Dropdown(
                                    id='float-units-dropdown',
                                    options=[
                                        {'label': 'g·L-1 (gram/liter)', 'value': 'g·L-1'},
                                        {'label': 'mg·L-1 (milligram/liter)', 'value': 'mg·L-1'},
                                        {'label': 'ºd (carbon hardness)', 'value': 'ºd'},
                                        {'label': 'L (liter)', 'value': 'L'},
                                        {'label': 'h (hours)', 'value': 'h'},
                                        {'label': 'cell/10 muL', 'value': 'cell/10 muL'},
                                    ],
                                    placeholder='Select units',
                                ),
                            ]),
                            dbc.FormGroup([
                                dbc.Label('Days Since Inoculation'),
                                dbc.Input(
                                    id='float-days-input',
                                    type='number',
                                    placeholder='Enter number of days',
                                ),
                            ]),
                            dbc.FormGroup([
                                dbc.Label('Extra Notes'),
                                dbc.Textarea(
                                    id='float-notes-input',
                                    placeholder='Enter any additional notes',
                                    style={"width": "100%"},
                                ),
                            ]),
                            dbc.Button('Submit Float Data', id='submit-float-button', color='primary', className='mt-1'),  
                            html.Div(id='float-submit-message', className='mt-1'),  
                        ]),

                        # 2. Percentage Data Tab
                        dbc.Tab(label="Add Percentage Data", children=[
                            dbc.FormGroup([
                                dbc.Label('Select Variable'),
                                dcc.Dropdown(
                                    id='percentage-variable-dropdown',
                                    placeholder='Select variable',
                                    options=[{'label': var, 'value': var} for var in get_variables_by_type('percentage')],
                                ),
                            ]),
                            dbc.FormGroup([
                                dbc.Label('Enter Percentage Value'),
                                dbc.Input(
                                    id='percentage-value-input',
                                    type='number',
                                    placeholder='Enter percentage value',
                                    min=0,
                                    max=100,
                                    step=0.01
                                ),
                            ]),
                            dbc.FormGroup([
                                dbc.Label('Days Since Inoculation'),
                                dbc.Input(
                                    id='percentage-days-input',
                                    type='number',
                                    placeholder='Enter number of days',
                                ),
                            ]),
                            dbc.FormGroup([
                                dbc.Label('Extra Notes'),
                                dbc.Textarea(
                                    id='percentage-notes-input',
                                    placeholder='Enter any additional notes',
                                    style={"width": "100%"},
                                ),
                            ]),
                            dbc.Button('Submit Percentage Data', id='submit-percentage-button', color='primary', className='mt-1'),  
                            html.Div(id='percentage-submit-message', className='mt-1'),  
                        ]),

                        # 3. String Data Tab
                        dbc.Tab(label="Add String Data", children=[
                            dbc.FormGroup([
                                dbc.Label('Select Variable'),
                                dcc.Dropdown(
                                    id='string-variable-dropdown',
                                    placeholder='Select variable',
                                    options=[{'label': var, 'value': var} for var in get_variables_by_type('string')],
                                ),
                            ]),
                            dbc.FormGroup([
                                dbc.Label('Start Day'),
                                dcc.DatePickerSingle(
                                    id='string-start-day-picker',
                                    date=datetime.today(),
                                    display_format='YYYY-MM-DD'
                                ),
                            ]),
                            dbc.FormGroup([
                                dbc.Label('End Day'),
                                dcc.DatePickerSingle(
                                    id='string-end-day-picker',
                                    date=datetime.today(),
                                    display_format='YYYY-MM-DD'
                                ),
                            ]),
                            dbc.FormGroup([
                                dbc.Label('Category'),
                                dcc.Dropdown(
                                    id='string-category-dropdown',
                                    options=[
                                        {'label': 'yellow', 'value': 'yellow'},
                                        {'label': 'yellowish green', 'value': 'yellowish green'},
                                        {'label': 'green', 'value': 'green'},
                                        {'label': 'dark green', 'value': 'dark green'},
                                        {'label': 'normal', 'value': 'normal'},
                                        {'label': 'many', 'value': 'many'},
                                        {'label': 'none', 'value': 'none'},
                                    ],
                                    placeholder='Select category',
                                ),
                            ]),
                            dbc.FormGroup([
                                dbc.Label('Extra Notes'),
                                dbc.Textarea(
                                    id='string-notes-input',
                                    placeholder='Enter any additional notes',
                                    style={"width": "100%"},
                                ),
                            ]),
                            dbc.Button('Submit String Data', id='submit-string-button', color='primary', className='mt-1'),  
                            html.Div(id='string-submit-message', className='mt-1'),  
                        ]),

                        # 4. Binary Data Tab
                        dbc.Tab(label="Add Binary Data", children=[
                            dbc.FormGroup([
                                dbc.Label('Select Variable'),
                                dcc.Dropdown(
                                    id='binary-variable-dropdown',
                                    placeholder='Select variable',
                                    options=[{'label': var, 'value': var} for var in get_variables_by_type('binary')],
                                ),
                            ]),
                            dbc.FormGroup([
                                dbc.Label('Start Day'),
                                dcc.DatePickerSingle(
                                    id='binary-start-day-picker',
                                    date=datetime.today(),
                                    display_format='YYYY-MM-DD'
                                ),
                            ]),
                            dbc.FormGroup([
                                dbc.Label('End Day'),
                                dcc.DatePickerSingle(
                                    id='binary-end-day-picker',
                                    date=datetime.today(),
                                    display_format='YYYY-MM-DD'
                                ),
                            ]),
                            dbc.FormGroup([
                                dbc.Label('Category'),
                                dcc.Dropdown(
                                    id='binary-category-dropdown',
                                    options=[
                                        {'label': 'yes', 'value': 'yes'},
                                        {'label': 'no', 'value': 'no'},
                                    ],
                                    placeholder='Select category',
                                ),
                            ]),
                            dbc.FormGroup([
                                dbc.Label('Extra Notes'),
                                dbc.Textarea(
                                    id='binary-notes-input',
                                    placeholder='Enter any additional notes',
                                    style={"width": "100%"},
                                ),
                            ]),
                            dbc.Button('Submit Binary Data', id='submit-binary-button', color='primary', className='mt-1'),  
                            html.Div(id='binary-submit-message', className='mt-1'),  
                        ]),
                    ])  # End of Tabs
                ])
            ], id='data-entry-card', className='mb-2', style={'display': 'none'}),  
        ], width=3),  

        # Center Column: Variable Graph and Gantt Chart
        dbc.Col([
            # Variable Graph Section
            dbc.Card([
                dbc.CardHeader(html.H3('Variable Graph')),
                dbc.CardBody([
                    dcc.Graph(id='variable-graph', style={'width': '100%', 'height': 'auto'}),  
                    dbc.Button("Download Data", id="download-button", color="secondary", className="mt-1"),  
                    dcc.Download(id="download-dataframe-csv")
                ])
            ], className='mb-2'),  

            # Gantt Chart Section
            dbc.Card([
                dbc.CardHeader(html.H4('Gantt Chart')),
                dbc.CardBody([
                    dbc.FormGroup([
                        dbc.Label('Select Variable(s) for Gantt Chart'),
                        dcc.Dropdown(
                            id='gantt-variable-dropdown',
                            options=[],  # To be populated via callback
                            multi=True,
                            placeholder='Select manual variable'
                        ),
                    ]),
                    dcc.Graph(id='gantt-chart')
                ])
            ], className='mb-2'),  
        ], width=9),  

    ])
], fluid=True, style={'backgroundColor': '#095040'})  # background color

# Callback to toggle the visibility and color of the Define Batch section
@app.callback(
    Output('define-batch-card', 'style'),
    Output('toggle-define-batch-button', 'children'),
    Output('toggle-define-batch-button', 'color'),
    Input('toggle-define-batch-button', 'n_clicks'),
    State('define-batch-card', 'style')
)
def toggle_define_batch(n_clicks, current_style):
    if n_clicks is None:
        n_clicks = 0
    if n_clicks % 2 == 1:
        # Show the Define Batch card
        new_style = {'display': 'block'}
        button_text = "Hide Define Batch"
        button_color = "secondary"  # Changed from 'danger' to 'secondary'
    else:
        # Hide the Define Batch card
        new_style = {'display': 'none'}
        button_text = "Show Define Batch"
        button_color = "primary"  # Changed from 'success' to 'primary'
    return new_style, button_text, button_color

# Callback to toggle the visibility and color of the Create Variable section
@app.callback(
    Output('create-variable-card', 'style'),
    Output('toggle-create-variable-button', 'children'),
    Output('toggle-create-variable-button', 'color'),
    Input('toggle-create-variable-button', 'n_clicks'),
    State('create-variable-card', 'style')
)
def toggle_create_variable(n_clicks, current_style):
    if n_clicks is None:
        n_clicks = 0
    if n_clicks % 2 == 1:
        # Show the Create Variable card
        new_style = {'display': 'block'}
        button_text = "Hide Create Variable"
        button_color = "secondary"  # Changed from 'danger' to 'secondary'
    else:
        # Hide the Create Variable card
        new_style = {'display': 'none'}
        button_text = "Show Create Variable"
        button_color = "primary"  # Changed from 'success' to 'primary'
    return new_style, button_text, button_color

# Callback to toggle the visibility and color of the Data Entry section
@app.callback(
    Output('data-entry-card', 'style'),
    Output('toggle-data-entry-button', 'children'),
    Output('toggle-data-entry-button', 'color'),
    Input('toggle-data-entry-button', 'n_clicks'),
    State('data-entry-card', 'style')
)
def toggle_data_entry(n_clicks, current_style):
    if n_clicks is None:
        n_clicks = 0
    if n_clicks % 2 == 1:
        # Show the Data Entry card
        new_style = {'display': 'block'}
        button_text = "Hide Data Entry"
        button_color = "secondary"  # Changed from 'danger' to 'secondary'
    else:
        # Hide the Data Entry card
        new_style = {'display': 'none'}
        button_text = "Show Data Entry"
        button_color = "primary"  # Changed from 'success' to 'primary'
    return new_style, button_text, button_color

# -------------------- Callbacks --------------------

# Callback to handle file selection, processing, and saving 
@app.callback(
    Output('file-list', 'children'),
    Output('file-save-status', 'children'),
    Output('prefix-dropdown', 'options'),
    [Input('confirm-button', 'n_clicks')],
    [State('date-picker-range', 'start_date'),
     State('date-picker-range', 'end_date'),
     State('filename-input', 'value')]
)
def update_file_list(n_clicks, start_date, end_date, filename_prefix):
    if n_clicks and start_date and end_date and filename_prefix:
        try:
            start_date_dt = datetime.fromisoformat(start_date)
            end_date_dt = datetime.fromisoformat(end_date)
        except Exception as e:
            return f"Invalid date format: {e}", "", []

        # List CSV files and process them (existing logic)
        csv_files = list_csv_files(primary_bucket)
        selected_files = filter_files_by_date(csv_files, start_date_dt, end_date_dt)

        if not selected_files:
            return "No files selected.", "", []

        # Process and save files (existing logic)
        global merged_dataframes
        merged_dataframes = {var: pd.DataFrame() for var in variable_names}

        for file in selected_files:
            dataframes = process_csv_file(primary_bucket, file, variable_names)

            for var_name, df in dataframes.items():
                if not df.empty:
                    merged_dataframes[var_name] = pd.concat([merged_dataframes[var_name], df], ignore_index=True)

        saved_files = []
        for var_name, df in merged_dataframes.items():
            if not df.empty:
                sanitized_var_name = sanitize_filename(var_name)  # Sanitize filename
                output_file = f"{filename_prefix}_{sanitized_var_name}.csv"
                blob = output_bucket.blob(output_file)
                # Save DataFrame to CSV in memory
                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False)
                blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
                saved_files.append(output_file)
                print(f"Saved {output_file}")

        # Ensure filename_prefixes is updated
        if filename_prefix not in filename_prefixes:
            filename_prefixes.append(filename_prefix)

        # Update prefix options
        all_prefixes = sorted(list(set(filename_prefixes + extract_prefixes_from_saved_files(output_bucket))))
        prefix_options = [{'label': prefix, 'value': prefix} for prefix in all_prefixes]

        # File display
        file_display = f"Selected Files:\n" + "\n".join(selected_files)
        # Save status
        save_status = f"Files processed and saved with prefix: {filename_prefix}. Saved {len(saved_files)} files."

        return file_display, save_status, prefix_options

    # Return default values when no action is taken
    existing_prefixes = extract_prefixes_from_saved_files(output_bucket)
    return "No files selected.", "", [{'label': prefix, 'value': prefix} for prefix in existing_prefixes]

# Callback to update the variable dropdown based on selected prefixes 
@app.callback(
    Output('variable-dropdown', 'options'),
    [Input('prefix-dropdown', 'value')]
)
def update_variable_dropdown(selected_prefixes):
    if selected_prefixes:
        variable_options = []
        for prefix in selected_prefixes:
            for var_name in variable_names:
                # Concatenate the prefix with the variable name for display
                labeled_var_name = f"{prefix}_{var_name}"
                # Get the display name if it exists, otherwise use the raw variable name
                display_name = variable_display_names.get(var_name, var_name)
                # Include the batch (prefix) in the display name
                labeled_display_name = f"{prefix}: {display_name}"
                # Add the option with the concatenated display name
                variable_options.append({'label': labeled_display_name, 'value': labeled_var_name})
        return variable_options
    return []

# Combined Callback to refresh manual variable dropdown options
@app.callback(
    Output('manual-variable-dropdown', 'options'),
    [
        Input('create-variable-button', 'n_clicks'),
        Input('submit-float-button', 'n_clicks'),
        Input('submit-percentage-button', 'n_clicks'),
        Input('submit-string-button', 'n_clicks'),
        Input('submit-binary-button', 'n_clicks')
    ]
)
def refresh_manual_variable_dropdown(*args):
    # Regardless of which button was clicked, refresh the manual variables
    manual_vars = list_manual_variables(manual_output_bucket)
    return [{'label': var, 'value': var} for var in manual_vars]

# Callback to update the variable dropdowns for each data type dynamically
@app.callback(
    [
        Output('float-variable-dropdown', 'options'),
        Output('percentage-variable-dropdown', 'options'),
        Output('string-variable-dropdown', 'options'),
        Output('binary-variable-dropdown', 'options'),
    ],
    [Input('create-variable-button', 'n_clicks'),
     Input('submit-float-button', 'n_clicks'),
     Input('submit-percentage-button', 'n_clicks'),
     Input('submit-string-button', 'n_clicks'),
     Input('submit-binary-button', 'n_clicks')]
)
def update_variable_dropdowns(*args):
    float_vars = get_variables_by_type('float')
    percentage_vars = get_variables_by_type('percentage')
    string_vars = get_variables_by_type('string')
    binary_vars = get_variables_by_type('binary')

    float_options = [{'label': var, 'value': var} for var in float_vars]
    percentage_options = [{'label': var, 'value': var} for var in percentage_vars]
    string_options = [{'label': var, 'value': var} for var in string_vars]
    binary_options = [{'label': var, 'value': var} for var in binary_vars]

    return float_options, percentage_options, string_options, binary_options

# Callback to handle Y-axis scaling and downsample data
@app.callback(
    [Output('variable-graph', 'figure'),
     Output('current-graph-data', 'data')],  # Store the graph data for export
    [Input('variable-dropdown', 'value'),  # Existing variables
     Input('new-variable-dropdown', 'value'),  # Offline Analytics variables
     Input('manual-variable-dropdown', 'value'),  # Manual data variables
     Input('time-mode-switch', 'value')]
)
def update_graph(selected_variables, new_variables, manual_variables, time_mode):
    if selected_variables is None:
        selected_variables = []
    if new_variables is None:
        new_variables = []
    if manual_variables is None:
        manual_variables = []

    if not selected_variables and not new_variables and not manual_variables:
        return px.line(title='Please select variables to display.'), {}

    all_data = pd.DataFrame()
    variable_unit_map = variable_units.copy()  # Start with existing units

    # Process existing (Process Data) variables
    for var in selected_variables:
        try:
            prefix, var_name = var.split('_', 1)
        except ValueError:
            continue
        file_path = f"{prefix}_{var_name}.csv"

        blob = output_bucket.blob(file_path)
        if blob.exists():
            data = blob.download_as_text()
            df = pd.read_csv(io.StringIO(data))

            if 'TimeString' in df.columns and 'VarValue' in df.columns:
                df['TimeString'] = pd.to_datetime(df['TimeString'], format='%d-%m-%Y %H:%M:%S', errors='coerce')
                if df['TimeString'].isnull().all():
                    continue

                df['ElapsedTime'] = (df['TimeString'] - df['TimeString'].min()).dt.total_seconds() / 60

                if df['VarValue'].dtype == 'object':
                    df['VarValue'] = df['VarValue'].str.replace(',', '.').astype(float)

                df = remove_outliers(df, 'VarValue')

                if var_name not in skip_variables:
                    df.set_index('TimeString', inplace=True)
                    df_resampled = df[['VarValue', 'ElapsedTime']].resample('1T').mean().fillna(method='ffill').reset_index()
                else:
                    df_resampled = df.reset_index()

                df_resampled['Variable'] = var_name
                all_data = pd.concat([all_data, df_resampled], ignore_index=True)

    # Process offline analytical variables (excluded from export)
    for display_name in new_variables:
        if display_name not in column_mapping:
            continue  # Skip if mapping is not defined
        table_name, col_name = column_mapping[display_name]

        if table_name == 'table1':
            df = table1
        else:
            df = table2

        if 'DateTime' not in df.columns or col_name not in df.columns:
            continue  # Skip if necessary columns are missing

        df_var = df[['DateTime', col_name, 'SAMPLE I.D']].copy()
        df_var.rename(columns={'DateTime': 'TimeString', col_name: 'VarValue'}, inplace=True)
        df_var['Variable'] = display_name

        df_var['TimeString'] = pd.to_datetime(df_var['TimeString'], errors='coerce')
        df_var = df_var.dropna(subset=['TimeString'])  # Drop rows with invalid datetime
        df_var['ElapsedTime'] = (df_var['TimeString'] - df_var['TimeString'].min()).dt.total_seconds() / 60
        all_data = pd.concat([all_data, df_var], ignore_index=True)

        # Assign the correct unit to the variable
        unit = new_variable_units.get(display_name, 'Value')  # Replace 'Value' with a default if needed
        variable_unit_map[display_name] = unit

    # Process Manual Variables float and percentage
    for manual_var in manual_variables:
        file_path = f"{manual_var}.csv"
        blob = manual_output_bucket.blob(file_path)
        if blob.exists():
            try:
                data = blob.download_as_text()
                df_manual = pd.read_csv(io.StringIO(data))

                # Ensure required columns exist
                required_columns = {'variable_name', 'value', 'units', 'days_since_inoculation'}
                if not required_columns.issubset(df_manual.columns):
                    print(f"One or more required columns missing in {file_path}. Skipping.")
                    continue

                # Skip if variable name ends with '_binary' or '_string'
                if manual_var.endswith('_binary') or manual_var.endswith('_string'):
                    continue

                # Add DateTime column
                df_manual['days_since_inoculation'] = df_manual['days_since_inoculation'].astype(int)
                df_manual['DateTime'] = start_date + pd.to_timedelta(df_manual['days_since_inoculation'], unit='d') + pd.to_timedelta('00:00:00')
                df_manual['DateTime'] = df_manual['DateTime'].dt.strftime('%d-%m-%Y %H:%M:%S')
                df_manual['DateTime'] = pd.to_datetime(df_manual['DateTime'], format='%d-%m-%Y %H:%M:%S', errors='coerce')

                # Drop rows with invalid DateTime
                df_manual = df_manual.dropna(subset=['DateTime'])

                # Assign Variable name
                df_manual['Variable'] = manual_var

                # Rename columns to match existing structure
                df_manual.rename(columns={'DateTime': 'TimeString', 'value': 'VarValue'}, inplace=True)

                # Assign units
                variable_unit_map[manual_var] = df_manual['units'].iloc[0] if not df_manual['units'].isnull().all() else 'Value'

                # Ensure ElapsedTime is calculated
                df_manual['ElapsedTime'] = (df_manual['TimeString'] - df_manual['TimeString'].min()).dt.total_seconds() / 60

                all_data = pd.concat([all_data, df_manual[['TimeString', 'VarValue', 'ElapsedTime', 'Variable']]], ignore_index=True)
            except Exception as e:
                print(f"Error processing manual variable {manual_var}: {e}")
                continue

    # Sort all_data by Variable and TimeString to ensure chronological plotting lines
    if not all_data.empty:
        all_data = all_data.sort_values(by=['Variable', 'TimeString'])

    if not all_data.empty:
        if time_mode == 'elapsed':
            x_axis = 'ElapsedTime'
            x_label = 'Elapsed Time (minutes)'
            x_type = "linear"
        else:
            x_axis = 'TimeString'
            x_label = 'Time (Absolute)'
            x_type = "date"

        # Prepare data for export (only Process Data Variables for now)
        export_data = all_data.copy()
        export_data = export_data[export_data['Variable'].isin([var.split('_',1)[1] for var in selected_variables])]
        if time_mode == 'absolute':
            export_data = export_data[['TimeString', 'Variable', 'VarValue']]
            export_data.rename(columns={'TimeString': 'Time'}, inplace=True)
        else:
            export_data = export_data[['ElapsedTime', 'Variable', 'VarValue']]
            export_data.rename(columns={'ElapsedTime': 'Elapsed Time (minutes)'}, inplace=True)

        # Create the figure
        fig = make_subplots(specs=[[{"secondary_y": True}]])

        # Track used y-axes
        yaxes_used = {}

        for var_name, df_group in all_data.groupby('Variable'):
            # Get the display name from the dictionary, default to the raw variable name if not found
            display_name = variable_display_names.get(var_name, var_name)
            yaxis_title = variable_unit_map.get(var_name, 'Value')

            if yaxis_title not in yaxes_used:
                yaxes_used[yaxis_title] = len(yaxes_used) + 1

            yaxis_id = f'yaxis{"" if yaxes_used[yaxis_title] == 1 else yaxes_used[yaxis_title]}'
            secondary_y = yaxes_used[yaxis_title] > 1

            if var_name in manual_variables:
                # Plot Manual Variables as Scatter Plots
                fig.add_trace(
                    go.Scatter(
                        x=df_group[x_axis],
                        y=df_group['VarValue'],
                        mode='markers+lines',
                        name=display_name,  # Use the display name for the trace
                    ),
                    secondary_y=secondary_y
                )
            elif var_name in new_variables:
                # Plot Offline Analytical Data Variables as Bar Charts
                fig.add_trace(
                    go.Bar(
                        x=df_group[x_axis],
                        y=df_group['VarValue'],
                        name=display_name,  # Use the display name for the trace
                        hovertext=df_group['SAMPLE I.D'] if 'SAMPLE I.D' in df_group.columns else None,
                        opacity=0.5
                    ),
                    secondary_y=secondary_y
                )
            else:
                # Plot Process Data Variables as Line Charts
                fig.add_trace(
                    go.Scatter(
                        x=df_group[x_axis],
                        y=df_group['VarValue'],
                        mode='lines',
                        name=display_name,  # Use the display name for the trace
                    ),
                    secondary_y=secondary_y
                )

            fig.update_yaxes(title_text=yaxis_title, secondary_y=secondary_y)

        fig.update_xaxes(title_text=x_label, type=x_type)
        fig.update_layout(
            title='Selected Variables Over Time',
            xaxis=dict(rangeslider=dict(visible=True), type=x_type),
            bargap=0.9,
            # Adjusting margins to reduce side space
            margin=dict(l=20, r=20, t=50, b=20),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,        # Adjust this value as needed to position the legend above the graph
                xanchor="center",
                x=0.5
            )
        )

        # Convert export_data to records for storage
        export_records = export_data.to_dict('records')

        return fig, export_records
    else:
        return px.line(title='No data to display.'), {}

# Callback to handle downloading the graph data
@app.callback(
    Output("download-dataframe-csv", "data"),
    [Input("download-button", "n_clicks")],
    [State('current-graph-data', 'data')],
    prevent_initial_call=True,
)
def download_graph_data(n_clicks, graph_data):
    if n_clicks and graph_data:
        # Convert the list of records back to a DataFrame
        df = pd.DataFrame(graph_data)
        
        # Optional: Reorder columns or format as needed
        # For example, if using absolute time:
        if 'Time' in df.columns:
            df = df[['Time', 'Variable', 'VarValue']]
        elif 'Elapsed Time (minutes)' in df.columns:
            df = df[['Elapsed Time (minutes)', 'Variable', 'VarValue']]

        # Optional: Add a timestamp to the filename
        now = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"exported_graph_data_{now}.csv"
        
        # Convert DataFrame to CSV
        return dcc.send_data_frame(df.to_csv, filename, index=False)
    return dash.no_update

# -------------------- Callbacks for Gantt Chart --------------------

# Callback to refresh Gantt variable dropdown options
@app.callback(
    Output('gantt-variable-dropdown', 'options'),
    [
        Input('create-variable-button', 'n_clicks'),
        Input('submit-float-button', 'n_clicks'),
        Input('submit-percentage-button', 'n_clicks'),
        Input('submit-string-button', 'n_clicks'),
        Input('submit-binary-button', 'n_clicks')
    ]
)
def refresh_gantt_variable_dropdown(*args):
    gantt_vars = list_gantt_manual_variables(manual_output_bucket)
    return [{'label': var, 'value': var} for var in gantt_vars]

# Callback to generate the Gantt chart based on selected variables
@app.callback(
    Output('gantt-chart', 'figure'),
    [Input('gantt-variable-dropdown', 'value')]
)
def update_gantt_chart(selected_vars):
    if not selected_vars:
        return go.Figure()

    all_entries = []

    for var in selected_vars:
        # Possible files: var_binary.csv and var_string.csv
        for file_suffix in ['_binary.csv', '_string.csv']:
            file_path = f"{var}{file_suffix}"
            blob = manual_output_bucket.blob(file_path)
            if blob.exists():
                data = blob.download_as_text()
                df = pd.read_csv(io.StringIO(data))
                # Ensure required columns exist
                required_cols = {'variable_name', 'start_day', 'end_day', 'category', 'notes'}
                if not required_cols.issubset(df.columns):
                    print(f"One or more required columns missing in {file_path}. Skipping.")
                    continue
                for _, row in df.iterrows():
                    # Parse dates: take first 10 characters of 'start_day' and 'end_day' (due to format problems - not sure it is a consistent issue)
                    start_date_str = str(row['start_day'])[:10]
                    end_date_str = str(row['end_day'])[:10]
                    category = row['category']
                    # Assign a task label combining variable name and category if needed
                    task_label = f"{var} - {category}"
                    all_entries.append({
                        'Variable': var,
                        'Category': category,
                        'Start': start_date_str,
                        'Finish': end_date_str
                    })

    if not all_entries:
        return go.Figure()

    gantt_df = pd.DataFrame(all_entries)

    # Convert Start and Finish to datetime
    gantt_df['Start'] = pd.to_datetime(gantt_df['Start'], format='%Y-%m-%d', errors='coerce')
    gantt_df['Finish'] = pd.to_datetime(gantt_df['Finish'], format='%Y-%m-%d', errors='coerce')

    # Drop rows with invalid dates
    gantt_df = gantt_df.dropna(subset=['Start', 'Finish'])

    if gantt_df.empty:
        return go.Figure()

    # Create a task name if needed
    gantt_df['Task'] = gantt_df['Variable'] + ": " + gantt_df['Category']

    # Plotly Express Gantt (timeline)
    fig = px.timeline(gantt_df, x_start="Start", x_end="Finish", y="Category", color="Variable", title="Gantt Chart", labels={"Category": "Category"})

    fig.update_layout(
        xaxis_title='Date',
        yaxis_title='Category',
        height=400,
        legend_title_text='Variable',
        margin=dict(l=20, r=20, t=50, b=20)  # Reduced side margins
    )

    return fig

# -------------------- Callback for Creating Variables --------------------

@app.callback(
    Output('create-variable-message', 'children'),
    [Input('create-variable-button', 'n_clicks')],
    [State('variable-name-input', 'value'),
     State('data-type-dropdown', 'value')]
)
def create_variable(n_clicks, variable_name, data_type):
    if n_clicks:
        # Validate inputs
        if not variable_name or not data_type:
            return dbc.Alert("Please provide both Variable Name and Data Type.", color="danger")
        
        # Sanitize the variable name for filename usage
        sanitized_name = sanitize_filename(variable_name)
        
        # Define the filename based on data type
        if data_type in ['float', 'percentage']:
            filename = f"{sanitized_name}_{data_type}.csv"
            columns = ['variable_name', 'value', 'units', 'notes', 'days_since_inoculation']
        elif data_type in ['string', 'binary']:
            filename = f"{sanitized_name}_{data_type}.csv"
            columns = ['variable_name', 'start_day', 'end_day', 'category', 'notes']
        else:
            return dbc.Alert("Invalid Data Type selected.", color="danger")
        
        file_path = filename  # GCS blobs use the full path within the bucket

        # Check if file already exists
        blob = manual_output_bucket.blob(file_path)
        if blob.exists():
            return dbc.Alert(f"A template named '{filename}' already exists.", color="warning")
        
        # Create an empty DataFrame with the specified columns
        df_empty = pd.DataFrame(columns=columns)
        
        try:
            # Save the empty DataFrame to CSV in memory
            csv_buffer = io.StringIO()
            df_empty.to_csv(csv_buffer, index=False)
            # Upload to GCS
            blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
            return dbc.Alert(f"Variable '{variable_name}' of type '{data_type}' created successfully and saved to manual output bucket.", color="success")
        except Exception as e:
            return dbc.Alert(f"Error creating variable: {e}", color="danger")
    
    return ""

# -------------------- Callbacks for Adding Data --------------------

# Callback to handle Float Data submission
@app.callback(
    Output('float-submit-message', 'children'),
    [Input('submit-float-button', 'n_clicks')],
    [
        State('float-variable-dropdown', 'value'),
        State('float-value-input', 'value'),
        State('float-units-dropdown', 'value'),
        State('float-days-input', 'value'),
        State('float-notes-input', 'value'),
    ]
)
def submit_float_data(n_clicks, variable, value, units, days, notes):
    if n_clicks:
        # Validation
        if not all([variable, value is not None, units, days is not None]):
            return dbc.Alert("Please fill out all required fields for Float Data.", color="danger")
        try:
            # Ensure value and days are numeric
            value = float(value)
            days = int(days)
        except ValueError:
            return dbc.Alert("Value must be a number and Days Since Inoculation must be an integer.", color="danger")
        
        # Prepare the data row
        new_row = {
            'variable_name': variable,
            'value': value,
            'units': units,
            'notes': notes if notes else '',
            'days_since_inoculation': days
        }

        # Append to the corresponding CSV in GCS
        filename = f"{sanitize_filename(variable)}_float.csv"
        blob = manual_output_bucket.blob(filename)

        try:
            if not blob.exists():
                # If the file doesn't exist, create it with headers
                df_new = pd.DataFrame([new_row])
                csv_buffer = io.StringIO()
                df_new.to_csv(csv_buffer, index=False)
                blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
            else:
                # Append without headers
                data = blob.download_as_text()
                csv_buffer = io.StringIO(data)
                df_existing = pd.read_csv(csv_buffer)
                df_updated = df_existing.append(new_row, ignore_index=True)
                updated_csv = df_updated.to_csv(index=False)
                blob.upload_from_string(updated_csv, content_type='text/csv')
            return dbc.Alert("Float data submitted successfully.", color="success")
        except Exception as e:
            return dbc.Alert(f"Error submitting float data: {e}", color="danger")
    return ""

# Callback to handle Percentage Data submission
@app.callback(
    Output('percentage-submit-message', 'children'),
    [Input('submit-percentage-button', 'n_clicks')],
    [
        State('percentage-variable-dropdown', 'value'),
        State('percentage-value-input', 'value'),
        State('percentage-days-input', 'value'),
        State('percentage-notes-input', 'value'),
    ]
)
def submit_percentage_data(n_clicks, variable, value, days, notes):
    if n_clicks:
        # Validation
        if not all([variable, value is not None, days is not None]):
            return dbc.Alert("Please fill out all required fields for Percentage Data.", color="danger")
        try:
            # Ensure value is between 0 and 100
            value = float(value)
            if not (0 <= value <= 100):
                return dbc.Alert("Percentage value must be between 0 and 100.", color="danger")
            days = int(days)
        except ValueError:
            return dbc.Alert("Value must be a number and Days Since Inoculation must be an integer.", color="danger")
        
        # Prepare the data row
        new_row = {
            'variable_name': variable,
            'value': value,
            'units': '%',  # To automatically assign '%' 
            'notes': notes if notes else '',
            'days_since_inoculation': days
        }

        # Append to the corresponding CSV in GCS
        filename = f"{sanitize_filename(variable)}_percentage.csv"
        blob = manual_output_bucket.blob(filename)

        try:
            if not blob.exists():
                # If the file doesn't exist, create it with headers
                df_new = pd.DataFrame([new_row])
                csv_buffer = io.StringIO()
                df_new.to_csv(csv_buffer, index=False)
                blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
            else:
                # Append without headers
                data = blob.download_as_text()
                csv_buffer = io.StringIO(data)
                df_existing = pd.read_csv(csv_buffer)
                df_updated = df_existing.append(new_row, ignore_index=True)
                updated_csv = df_updated.to_csv(index=False)
                blob.upload_from_string(updated_csv, content_type='text/csv')
            return dbc.Alert("Percentage data submitted successfully.", color="success")
        except Exception as e:
            return dbc.Alert(f"Error submitting percentage data: {e}", color="danger")
    return ""

# Callback to handle String Data submission
@app.callback(
    Output('string-submit-message', 'children'),
    [Input('submit-string-button', 'n_clicks')],
    [
        State('string-variable-dropdown', 'value'),
        State('string-start-day-picker', 'date'),
        State('string-end-day-picker', 'date'),
        State('string-category-dropdown', 'value'),
        State('string-notes-input', 'value'),
    ]
)
def submit_string_data(n_clicks, variable, start_day, end_day, category, notes):
    if n_clicks:
        # Validation
        if not all([variable, start_day, end_day, category]):
            return dbc.Alert("Please fill out all required fields for String Data.", color="danger")
        try:
            # Ensure start_day is before or equal to end_day
            start_date_var = datetime.fromisoformat(start_day)
            end_date_var = datetime.fromisoformat(end_day)
            if start_date_var > end_date_var:
                return dbc.Alert("Start Day cannot be after End Day.", color="danger")
        except ValueError:
            return dbc.Alert("Invalid dates provided.", color="danger")
        
        # Prepare the data row
        new_row = {
            'variable_name': variable,
            'start_day': start_day,
            'end_day': end_day,
            'category': category,
            'notes': notes if notes else ''
        }

        # Append to the corresponding CSV in GCS
        filename = f"{sanitize_filename(variable)}_string.csv"
        blob = manual_output_bucket.blob(filename)

        try:
            if not blob.exists():
                # If the file doesn't exist, create it with headers
                df_new = pd.DataFrame([new_row])
                csv_buffer = io.StringIO()
                df_new.to_csv(csv_buffer, index=False)
                blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
            else:
                # Append without headers
                data = blob.download_as_text()
                csv_buffer = io.StringIO(data)
                df_existing = pd.read_csv(csv_buffer)
                df_updated = df_existing.append(new_row, ignore_index=True)
                updated_csv = df_updated.to_csv(index=False)
                blob.upload_from_string(updated_csv, content_type='text/csv')
            return dbc.Alert("String data submitted successfully.", color="success")
        except Exception as e:
            return dbc.Alert(f"Error submitting string data: {e}", color="danger")
    return ""

# Callback to handle Binary Data submission
@app.callback(
    Output('binary-submit-message', 'children'),
    [Input('submit-binary-button', 'n_clicks')],
    [
        State('binary-variable-dropdown', 'value'),
        State('binary-start-day-picker', 'date'),
        State('binary-end-day-picker', 'date'),
        State('binary-category-dropdown', 'value'),
        State('binary-notes-input', 'value'),
    ]
)
def submit_binary_data(n_clicks, variable, start_day, end_day, category, notes):
    if n_clicks:
        # Validation
        if not all([variable, start_day, end_day, category]):
            return dbc.Alert("Please fill out all required fields for Binary Data.", color="danger")
        try:
            # Ensure start_day is before or equal to end_day
            start_date_var = datetime.fromisoformat(start_day)
            end_date_var = datetime.fromisoformat(end_day)
            if start_date_var > end_date_var:
                return dbc.Alert("Start Day cannot be after End Day.", color="danger")
        except ValueError:
            return dbc.Alert("Invalid dates provided.", color="danger")
        
        # Prepare the data row
        new_row = {
            'variable_name': variable,
            'start_day': start_day,
            'end_day': end_day,
            'category': category,
            'notes': notes if notes else ''
        }

        # Append to the corresponding CSV in GCS
        filename = f"{sanitize_filename(variable)}_binary.csv"
        blob = manual_output_bucket.blob(filename)

        try:
            if not blob.exists():
                # If the file doesn't exist, create it with headers
                df_new = pd.DataFrame([new_row])
                csv_buffer = io.StringIO()
                df_new.to_csv(csv_buffer, index=False)
                blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
            else:
                # Append without headers
                data = blob.download_as_text()
                csv_buffer = io.StringIO(data)
                df_existing = pd.read_csv(csv_buffer)
                df_updated = df_existing.append(new_row, ignore_index=True)
                updated_csv = df_updated.to_csv(index=False)
                blob.upload_from_string(updated_csv, content_type='text/csv')
            return dbc.Alert("Binary data submitted successfully.", color="success")
        except Exception as e:
            return dbc.Alert(f"Error submitting binary data: {e}", color="danger")
    return ""

# -------------------- Run the Dash App --------------------

# Run the Dash app
if __name__ == '__main__':
    # Initialize filename_prefixes from saved files in the output bucket
    filename_prefixes = extract_prefixes_from_saved_files(output_bucket)
    app.run_server(debug=True)