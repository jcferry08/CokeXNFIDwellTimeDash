import pandas as pd
import streamlit as st
import duckdb
from datetime import datetime, timedelta

st.set_page_config(
    page_title="Coke x NFI",
    page_icon=":bar_chart:",
    layout="wide")

st.title('Dwell and On Time Compliance Reporting')
st.markdown('_Prototype V. 0.1.0')

@st.cache_data
def load_data(file):
    data = pd.read_csv(file, parse_dates=True)
    return data

# File uploaders for the three CSV files
trailer_activity = st.sidebar.file_uploader("Upload Trailer Activity File", type=['csv'])
appt_view = st.sidebar.file_uploader("Upload Appointment View File", type=['csv'])
order_view = st.sidebar.file_uploader("Upload Order View File", type=['csv'])

# Stop if any of the files are missing
if trailer_activity is None or appt_view is None or order_view is None:
    st.info("Please upload all three files through the sidebar")
    st.stop()

# Load the data into pandas DataFrames
ta_df = load_data(trailer_activity)
av_df = load_data(appt_view)
ov_df = load_data(order_view)

# Convert datetime columns to appropriate datetime format
datetime_columns_ta = ['CHECKOUT DATE TIME', 'Date/Time']
datetime_columns_av = ['Appointment Date', 'Check In DateTime']
datetime_columns_ov = ['Appointment Date', 'Check In DateTime']

for col in datetime_columns_ta:
    if col in ta_df.columns:
        ta_df[col] = pd.to_datetime(ta_df[col], errors='coerce').dt.strftime('%m-%d-%Y %H:%M')

for col in datetime_columns_av:
    if col in av_df.columns:
        av_df[col] = pd.to_datetime(av_df[col], errors='coerce').dt.strftime('%m-%d-%Y %H:%M')

for col in datetime_columns_ov:
    if col in ov_df.columns:
        ov_df[col] = pd.to_datetime(ov_df[col], errors='coerce').dt.strftime('%m-%d-%Y %H:%M')

# Preview the uploaded CSV files
with st.expander('Preview Trailer Activity CSV'):
    st.dataframe(ta_df)

with st.expander('Preview Appointment View CSV'):
    st.dataframe(av_df)

with st.expander('Preview Order View CSV'):
    st.dataframe(ov_df)

# Creating a DuckDB connection
con = duckdb.connect()

# Register the dataframes as DuckDB tables
con.register('ta_df', ta_df)
con.register('av_df', av_df)
con.register('ov_df', ov_df)

# Step 1: Filter Trailer Activity for specific visit types
filtered_query = """
    SELECT *
    FROM ta_df
    WHERE "VISIT TYPE" IN ('Live Load', 'Pickup Load', 'Pickup Empty')
    AND "SHIPMENT_ID" IS NOT NULL
    AND "ACTIVITY TYPE " = 'CLOSED'
"""
filtered_ta_df = con.execute(filtered_query).df()

# Step 2: Create a map-like join with Appointment View and Order View
merge_query = """
    WITH filtered_ta AS (
        SELECT *
        FROM ta_df
        WHERE "VISIT TYPE" IN ('Live Load', 'Pickup Load', 'Pickup Empty')
        AND "SHIPMENT_ID" IS NOT NULL
        AND "ACTIVITY TYPE " = 'CLOSED'
    )
    
    , ranked_ta AS (
        SELECT 
            *, 
            ROW_NUMBER() OVER (PARTITION BY "SHIPMENT_ID" ORDER BY "Date/Time" DESC) AS rn
        FROM filtered_ta
    )
    
    SELECT 
        REGEXP_REPLACE(TRIM(TRAILING '.0' FROM CAST(ROUND(ta."SHIPMENT_ID") AS VARCHAR)), ',', '') as "Shipment ID", 
        av."Appointment Type", 
        av."Order Status", 
        av."Carrier", 
        ov."Appointment Date", 
        ov."Check In DateTime", 
        ta."CHECKOUT DATE TIME" as "Check Out DateTime", 
        ta."Date/Time" AS "Loaded DateTime", 
        ta."VISIT TYPE" as "Visit Type", 
        ta."ACTIVITY TYPE " as "Activity Type"
    FROM ranked_ta ta
    LEFT JOIN av_df av
    ON ta."SHIPMENT_ID" = av."Shipment Nbr"
    LEFT JOIN ov_df ov
    ON ta."SHIPMENT_ID" = ov."Shipment #"
    WHERE ta.rn = 1
"""

merged_df = con.execute(merge_query).df()

# Remove rows without a carrier or appointment date time
merged_df = merged_df.dropna(subset=['Carrier', 'Appointment Date'])

# Calculated Fields
current_time = datetime.now()
required_time = []
compliance = []
dwell_duration = []
scheduled_date = []
month = []
week = []

for index, row in merged_df.iterrows():
    appointment_type = row['Appointment Type']
    appt_datetime = pd.to_datetime(row['Appointment Date'], errors='coerce') if row['Appointment Date'] else None
    check_in_datetime = pd.to_datetime(row['Check In DateTime'], errors='coerce') if row['Check In DateTime'] else None
    loaded_datetime = pd.to_datetime(row['Loaded DateTime'], errors='coerce') if row['Loaded DateTime'] else None

    req_time = ''
    comp = ''
    dwell = ''
    sched_date = ''
    mon = ''
    wk = ''

    if pd.notna(appt_datetime):
        req_time = appt_datetime + timedelta(minutes=15) if appointment_type == 'Live Load' else appt_datetime + timedelta(minutes=1440)
        if pd.notna(check_in_datetime):
            comp = 'Late' if check_in_datetime > req_time else 'On Time'
        else:
            comp = 'Late' if current_time > req_time else 'On Time'

        if pd.notna(loaded_datetime):
            if comp == 'On Time':
                dwell = (loaded_datetime - appt_datetime).total_seconds() / 3600
            elif pd.notna(check_in_datetime):
                dwell = (loaded_datetime - check_in_datetime).total_seconds() / 3600
            dwell = max(dwell, 0)
            dwell = round(dwell, 2)
        
        sched_date = appt_datetime.strftime('%m-%d-%Y')
        mon = appt_datetime.strftime('%b')
        wk = appt_datetime.isocalendar()[1]

    required_time.append(req_time)
    compliance.append(comp)
    dwell_duration.append(dwell)
    scheduled_date.append(sched_date)
    month.append(mon)
    week.append(wk)

merged_df['Required Time'] = required_time
merged_df['Compliance'] = compliance
merged_df['Dwell Duration'] = dwell_duration
merged_df['Scheduled Date'] = scheduled_date
merged_df['Month'] = month
merged_df['Week'] = week

# Display the merged dataset with calculated fields
with st.expander('Merged Dataset with Calculated Fields'):
    st.dataframe(merged_df)

# Optionally, allow the user to download the merged data
@st.cache_data
def convert_df(df):
    return df.to_csv(index=False).encode('utf-8')

csv = convert_df(merged_df)

st.download_button(
    label="Download Merged Data as CSV",
    data=csv,
    file_name='merged_data.csv',
    mime='text/csv'
)