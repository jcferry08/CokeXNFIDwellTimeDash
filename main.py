import pandas as pd
import streamlit as st
import duckdb

st.set_page_config(
    page_title= "Coke x NFI",
    page_icon = ":bar_chart:",
    layout="wide")

st.title('Dwell and On Time Compliance Reporting')
st.markdown('_Prototype V. 0.1.0')

@st.cache_data
def load_data(file):
    data = pd.read_csv(file)
    return data

trailer_activity = st.sidebar.file_uploader("Upload Trailer Activity File", type=['csv'])

if trailer_activity is None:
    st.info("Upload a file through config")
    st.stop()

ta_df = load_data(trailer_activity)
with st.expander('Preview Trailer Activity CSV'):
    st.dataframe(
        ta_df, 
        column_config={
            "APPOINTMENT": st.column_config.NumberColumn(format="%d"),
            "ASN_ID": st.column_config.NumberColumn(format="%d")
        })

appt_view = st.sidebar.file_uploader("Upload Appointment View File", type=['csv'])

if appt_view is None:
    st.info("Upload a file through config")
    st.stop()

av_df = load_data(appt_view)
with st.expander('Preview Appointment View CSV'):
    st.dataframe(
        av_df,
        column_config={
            "Shipment Nbr": st.column_config.NumberColumn(format="%d"),
            "REF Shipment Nbr": st.column_config.NumberColumn(format="%d"),
            "ASN": st.column_config.NumberColumn(format="%d")
        })

order_view = st.sidebar.file_uploader("Upload Order View File", type=['csv'])

if order_view is None:
    st.info("Upload a file through config")
    st.stop()


ov_df = load_data(order_view)
with st.expander('Preview Order View CSV'):
    st.dataframe(
        ov_df,
        column_config={
            "Ref Shipment Nbr": st.column_config.NumberColumn(format="%d"),
            "Shipment #": st.column_config.NumberColumn(format="%d"),
            "SAP Delivery # (Order#)": st.column_config.NumberColumn(format="%d"),
            "Appointment": st.column_config.NumberColumn(format="%d"),
            "Wave #": st.column_config.NumberColumn(format="%d")
        })



