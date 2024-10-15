import pandas as pd
import streamlit as st
import duckdb
from datetime import datetime, timedelta
import numpy as np
import plotly.graph_objects as go

st.set_page_config(
    page_title="Coke x NFI",
    page_icon=":bar_chart:",
    layout="wide")

st.title('Dwell and On Time Compliance Reporting')
st.markdown('_Prototype V. 0.1.0')

# Create tabs for Data Upload and Dashboard
tabs = st.tabs(["Data Upload", "Daily Dashboard", "Weekly Dashboard", "Monthly Dashboard", "YTD Dashboard"])

with tabs[0]:
    st.header("Data Upload")
    st.write("Please Upload the CSV files for Trailer Activity, Appointment View, and Order View Here.")

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
            ta."ACTIVITY TYPE "
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
    dwell_time = []
    scheduled_date = []
    month = []
    week = []

    for index, row in merged_df.iterrows():
        appointment_type = row['Appointment Type']
        appt_datetime = pd.to_datetime(row['Appointment Date'], errors='coerce') if row['Appointment Date'] else None
        check_in_datetime = pd.to_datetime(row['Check In DateTime'], errors='coerce') if row['Check In DateTime'] else None
        loaded_datetime = pd.to_datetime(row['Loaded DateTime'], errors='coerce') if row['Loaded DateTime'] else None
        check_out_datetime = pd.to_datetime(row['Check Out DateTime'], errors='coerce') if row['Check Out DateTime'] else None

        req_time = ''
        comp = ''
        dwell = 0
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
                if comp == 'Late' and pd.notna(check_in_datetime):
                    dwell = (loaded_datetime - check_in_datetime).total_seconds() / 3600
                elif comp == 'On Time':
                    dwell = (loaded_datetime - appt_datetime).total_seconds() / 3600

            dwell = round(max(dwell, 0), 2)
            
            sched_date = appt_datetime.strftime('%m-%d-%Y')
            mon = appt_datetime.strftime('%b')
            wk = appt_datetime.isocalendar()[1]

        required_time.append(req_time)
        compliance.append(comp)
        dwell_time.append(dwell)
        scheduled_date.append(sched_date)
        month.append(mon)
        week.append(wk)

    merged_df['Required Time'] = required_time
    merged_df['Compliance'] = compliance
    merged_df['Dwell Time'] = dwell_time
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

with tabs[1]:
    st.header("Daily Dashboard")
    selected_date = st.date_input("Select Date for Daily Report:")
    if selected_date:
        selected_date_str = selected_date.strftime('%m-%d-%Y')
        filtered_df = merged_df[merged_df['Scheduled Date'] == selected_date_str]

        # Create two columns for layout
        col1, col2 = st.columns([1, 1])  # Column 1 is wider than Column 2

        # Pivot: On Time Compliance by Date (left column)
        with col1:
            with st.expander('On Time Compliance by Date'):
                compliance_pivot = filtered_df.pivot_table(
                    values='Shipment ID', 
                    index='Scheduled Date',
                    columns='Compliance',
                    aggfunc='count',
                    fill_value=0
                ).reset_index()
                compliance_pivot['Grand Total'] = compliance_pivot.select_dtypes(include=[np.number]).sum(axis=1)
                compliance_pivot['On Time %'] = round((compliance_pivot.get('On Time', 0) / compliance_pivot['Grand Total']) * 100, 2)
                compliance_pivot.style.format({'On Time %': lambda x: '{:.2f}%'.format(x).rstrip('0').rstrip('.')})
                st.subheader('On Time Compliance by Date')
                st.table(compliance_pivot)

        # Pivot: On Time Compliance by Carrier (right column)
        with col1:
            with st.expander('On Time Compliance by Carrier'):
                # Creating the pivot table
                carrier_pivot = filtered_df.pivot_table(
                    values='Shipment ID',
                    index='Carrier',
                    columns='Compliance',
                    aggfunc='count',
                    fill_value=0
                ).reset_index()
                
                # Calculating Grand Total and On Time %
                carrier_pivot['Grand Total'] = carrier_pivot.select_dtypes(include=[np.number]).sum(axis=1)
                carrier_pivot['On Time %'] = round((carrier_pivot.get('On Time', 0) / carrier_pivot['Grand Total']) * 100, 2)
                
                # Sorting carriers by On Time % in descending order
                carrier_pivot = carrier_pivot.sort_values(by='On Time %', ascending=False)
                
                # Displaying the pivot table in the expander
                st.subheader('On Time Compliance by Carrier')
                st.table(carrier_pivot)

            # Creating the heat map below the expander in col1
            # Prepare the data for the heat map
            heatmap_data = carrier_pivot.set_index('Carrier')[['On Time %']]
            
            # Plotting the heat map using Plotly to blend better with Streamlit
            fig = go.Figure(data=go.Heatmap(
                z=heatmap_data['On Time %'].values.reshape(-1, 1),
                x=['On Time %'],
                y=heatmap_data.index,
                colorscale='RdYlGn',  # Red to Green color map
                colorbar=dict(title="On Time %"),
                text=heatmap_data['On Time %'].values.reshape(-1, 1),
                texttemplate="%{text:.2f}%",
                showscale=True
            ))
            
            # Customizing the plot layout
            fig.update_layout(
                title='On Time Compliance Percentage by Carrier',
                xaxis_title='',
                yaxis_title='Carrier',
                yaxis_autorange='reversed',
                height=len(heatmap_data) * 40 + 100  # Dynamic height based on number of carriers
            )
            
            # Displaying the heat map using Streamlit
            st.plotly_chart(fig, use_container_width=True, key="daily_heatmap")

        # Assuming 'dwell_count_pivot' DataFrame is already calculated
        with col2:
            with st.expander('Daily Count by Dwell Time'):
                # Check if 'Dwell Time' column exists in filtered_df to avoid KeyError
                if 'Dwell Time' in filtered_df.columns:
                    # Creating 'Dwell Time Category' column if it doesn't exist
                    filtered_df['Dwell Time Category'] = pd.cut(
                        filtered_df['Dwell Time'],
                        bins=[0, 2, 3, 4, 5, np.inf],
                        labels=['less than 2 hours', '2 to 3 hours', '3 to 4 hours', '4 to 5 hours', '5 or more hours']
                    )
                else:
                    st.error("'Dwell Time' column is missing from the dataset.")

                # Assuming dwell_count_pivot already exists
                dwell_count_pivot = filtered_df.pivot_table(
                    values='Shipment ID',
                    index='Dwell Time Category',
                    columns='Compliance',
                    aggfunc='count',
                    fill_value=0
                ).reset_index()
                dwell_count_pivot['Grand Total'] = dwell_count_pivot.select_dtypes(include=[np.number]).sum(axis=1)
                dwell_count_pivot['Late % of Total'] = round((dwell_count_pivot.get('Late', 0) / dwell_count_pivot['Grand Total']) * 100, 2)
                dwell_count_pivot['On Time % of Total'] = round((dwell_count_pivot.get('On Time', 0) / dwell_count_pivot['Grand Total']) * 100, 2)
                
                # Also showing the table for additional clarity
                st.subheader('Daily Count by Dwell Time')
                st.table(dwell_count_pivot)

            # Creating a 100% stacked bar chart using Plotly
            categories = dwell_count_pivot['Dwell Time Category']
            late_percentages = dwell_count_pivot['Late % of Total']
            on_time_percentages = dwell_count_pivot['On Time % of Total']
            
            # Plotting with Plotly
            fig = go.Figure()
            
            # Add On Time bars
            fig.add_trace(go.Bar(
                x=categories,
                y=on_time_percentages,
                name='On Time',
                marker_color='green',
                text=on_time_percentages,
                textposition='inside'
            ))
            
            # Add Late bars
            fig.add_trace(go.Bar(
                x=categories,
                y=late_percentages,
                name='Late',
                marker_color='red',
                text=late_percentages,
                textposition='inside'
            ))
            
            # Update layout for 100% stacked bar chart
            fig.update_layout(
                barmode='stack',
                title='100% Stacked Bar Chart: Late vs On Time by Dwell Time Category',
                xaxis_title='Dwell Time Category',
                yaxis_title='% of Total Shipments',
                legend_title='Compliance',
                xaxis_tickangle=-45
            )
            
            # Displaying in Streamlit
            st.plotly_chart(fig, use_container_width=True, key="daily_100%_stacked")

        # Pivot: Average Dwell Time by Appointment Type (left column)
        with col2:
            with st.expander('Average Dwell Time by Appointment Type'):
                dwell_average_pivot = filtered_df.pivot_table(
                    values='Dwell Time',
                    index='Appointment Type',
                    columns='Compliance',
                    aggfunc='mean',
                    fill_value=np.nan
                ).reset_index()

                dwell_average_pivot['Grand Average'] = dwell_average_pivot.select_dtypes(include=[np.number]).mean(axis=1)

                # Calculate Grand Average row
                grand_avg_row = dwell_average_pivot.select_dtypes(include=[np.number]).mean().to_frame().T
                grand_avg_row['Appointment Type'] = 'Grand Average'
                dwell_average_pivot = pd.concat([dwell_average_pivot, grand_avg_row], ignore_index=True)

                # Make sure the rendering part is inside the expander
                st.subheader('Average Dwell Time by Appointment Type')
                st.table(dwell_average_pivot)

            # Create a grouped bar chart to visualize dwell time by compliance (Late and On Time)
            if 'Late' in dwell_average_pivot.columns and 'On Time' in dwell_average_pivot.columns:
                fig = go.Figure()
                
                # Add bars for Late
                fig.add_trace(go.Bar(
                    x=dwell_average_pivot['Appointment Type'],
                    y=dwell_average_pivot['Late'],
                    name='Late',
                    marker=dict(color='rgba(255, 0, 0, 0.7)'),  # Red color with transparency
                    text=[f'{val:.1f}%' for val in dwell_average_pivot['Late']],  # Add percentages as text
                    textposition='auto',
                    textfont=dict(color='white')  # Set text color to white
                ))
                
                # Add bars for On Time
                fig.add_trace(go.Bar(
                    x=dwell_average_pivot['Appointment Type'],
                    y=dwell_average_pivot['On Time'],
                    name='On Time',
                    marker=dict(color='rgba(0, 128, 0, 0.7)'),  # Green color with transparency
                    text=[f'{val:.1f}%' for val in dwell_average_pivot['On Time']],  # Add percentages as text
                    textposition='auto',
                    textfont=dict(color='white')  # Set text color to white
                ))

                # Update layout for better readability
                fig.update_layout(
                    title='Average Dwell Time by Appointment Type and Compliance',
                    xaxis_title='Appointment Type',
                    yaxis_title='Average Dwell Time',
                    barmode='group',
                    xaxis_tickangle=-45,
                    legend_title='Compliance',
                    height=500,
                    width=800
                )

                # Display the chart in Streamlit
                st.plotly_chart(fig, use_container_width=True, key="daily_grouped_bar")


with tabs[2]:
    st.header("Weekly Dashboard")
    selected_week = st.number_input("Select Week Number for Weekly Report:", min_value=1, max_value=52, step=1)
    if selected_week:
        # Filter data based on the selected week number
        filtered_df = merged_df[merged_df['Week'] == selected_week]

        # Create two columns for layout
        col1, col2 = st.columns([1, 1])  # Column 1 is wider than Column 2
        # Pivot: On Time Compliance by Week (left column)
        with col1:
            with st.expander('On Time Compliance by Week'):
                compliance_pivot = filtered_df.pivot_table(
                    values='Shipment ID', 
                    index='Week',
                    columns='Compliance',
                    aggfunc='count',
                    fill_value=0
                ).reset_index()
                compliance_pivot['Grand Total'] = compliance_pivot.select_dtypes(include=[np.number]).sum(axis=1)
                compliance_pivot['On Time %'] = round((compliance_pivot.get('On Time', 0) / compliance_pivot['Grand Total']) * 100, 2)
                compliance_pivot.style.format({'On Time %': lambda x: '{:.2f}%'.format(x).rstrip('0').rstrip('.')})
                st.subheader('On Time Compliance by Week')
                st.table(compliance_pivot)

            # Add Line Chart for Compliance Trend within the Week
            
            # Aggregating the data by date and compliance status
            trend_data = filtered_df.groupby(['Scheduled Date', 'Compliance']).size().unstack(fill_value=0).reset_index()

            # Create line chart
            fig = go.Figure()

            # Add 'On Time' line to the chart
            if 'On Time' in trend_data.columns:
                fig.add_trace(go.Scatter(
                    x=trend_data['Scheduled Date'], 
                    y=trend_data['On Time'], 
                    mode='lines+markers+text',
                    name='On Time',
                    line=dict(color='green'),
                    text=trend_data['On Time'],  # Add counts as text labels
                    textposition='top center',  # Positioning the text above the points
                    textfont=dict(color='white'),  # Make the text color white
                ))

            # Add 'Late' line to the chart
            if 'Late' in trend_data.columns:
                fig.add_trace(go.Scatter(
                    x=trend_data['Scheduled Date'], 
                    y=trend_data['Late'], 
                    mode='lines+markers+text',
                    name='Late',
                    line=dict(color='red'),
                    text=trend_data['Late'],  # Add counts as text labels
                    textposition='top center',  # Positioning the text above the points
                    textfont=dict(color='white'),  # Make the text color white
                ))

            # Update layout for better readability
            fig.update_layout(
                title='Compliance Trend Over the Selected Week',
                xaxis_title='Scheduled Date',
                yaxis_title='Number of Shipments',
                xaxis=dict(type='category'),  # Ensures dates are shown correctly even if sparse
                template='plotly_white'
            )

            st.plotly_chart(fig, use_container_width=True, key="weekly_line_chart")

        # Pivot: On Time Compliance by Carrier (right column)
        with col1:
            with st.expander('On Time Compliance by Carrier'):
                # Creating the pivot table
                carrier_pivot = filtered_df.pivot_table(
                    values='Shipment ID',
                    index='Carrier',
                    columns='Compliance',
                    aggfunc='count',
                    fill_value=0
                ).reset_index()
                
                # Calculating Grand Total and On Time %
                carrier_pivot['Grand Total'] = carrier_pivot.select_dtypes(include=[np.number]).sum(axis=1)
                carrier_pivot['On Time %'] = round((carrier_pivot.get('On Time', 0) / carrier_pivot['Grand Total']) * 100, 2)
                
                # Sorting carriers by On Time % in descending order
                carrier_pivot = carrier_pivot.sort_values(by='On Time %', ascending=False)
                
                # Displaying the pivot table in the expander
                st.subheader('On Time Compliance by Carrier')
                st.table(carrier_pivot)

            # Creating the heat map below the expander in col1
            # Prepare the data for the heat map
            heatmap_data = carrier_pivot.set_index('Carrier')[['On Time %']]
            
            # Plotting the heat map using Plotly to blend better with Streamlit
            fig = go.Figure(data=go.Heatmap(
                z=heatmap_data['On Time %'].values.reshape(-1, 1),
                x=['On Time %'],
                y=heatmap_data.index,
                colorscale='RdYlGn',  # Red to Green color map
                colorbar=dict(title="On Time %"),
                text=heatmap_data['On Time %'].values.reshape(-1, 1),
                texttemplate="%{text:.2f}%",
                showscale=True
            ))
            
            # Customizing the plot layout
            fig.update_layout(
                title='On Time Compliance Percentage by Carrier',
                xaxis_title='',
                yaxis_title='Carrier',
                yaxis_autorange='reversed',
                height=len(heatmap_data) * 40 + 100  # Dynamic height based on number of carriers
            )
            
            # Displaying the heat map using Streamlit
            st.plotly_chart(fig, use_container_width=True, key="weekly_heatmap")

        # Daily Count by Dwell Time (right column)
        with col2:
            with st.expander('Weekly Count by Dwell Time'):
                # Check if 'Dwell Time' column exists in filtered_df to avoid KeyError
                if 'Dwell Time' in filtered_df.columns:
                    # Creating 'Dwell Time Category' column if it doesn't exist
                    filtered_df['Dwell Time Category'] = pd.cut(
                        filtered_df['Dwell Time'],
                        bins=[0, 2, 3, 4, 5, np.inf],
                        labels=['less than 2 hours', '2 to 3 hours', '3 to 4 hours', '4 to 5 hours', '5 or more hours']
                    )
                else:
                    st.error("'Dwell Time' column is missing from the dataset.")

                # Assuming dwell_count_pivot already exists
                dwell_count_pivot = filtered_df.pivot_table(
                    values='Shipment ID',
                    index='Dwell Time Category',
                    columns='Compliance',
                    aggfunc='count',
                    fill_value=0
                ).reset_index()
                dwell_count_pivot['Grand Total'] = dwell_count_pivot.select_dtypes(include=[np.number]).sum(axis=1)
                dwell_count_pivot['Late % of Total'] = round((dwell_count_pivot.get('Late', 0) / dwell_count_pivot['Grand Total']) * 100, 2)
                dwell_count_pivot['On Time % of Total'] = round((dwell_count_pivot.get('On Time', 0) / dwell_count_pivot['Grand Total']) * 100, 2)
                
                # Also showing the table for additional clarity
                st.subheader('Weekly Count by Dwell Time')
                st.table(dwell_count_pivot)

            # Creating a 100% stacked bar chart using Plotly
            categories = dwell_count_pivot['Dwell Time Category']
            late_percentages = dwell_count_pivot['Late % of Total']
            on_time_percentages = dwell_count_pivot['On Time % of Total']
            
            # Plotting with Plotly
            fig = go.Figure()
            
            # Add On Time bars
            fig.add_trace(go.Bar(
                x=categories,
                y=on_time_percentages,
                name='On Time',
                marker_color='green',
                text=on_time_percentages,
                textposition='inside'
            ))
            
            # Add Late bars
            fig.add_trace(go.Bar(
                x=categories,
                y=late_percentages,
                name='Late',
                marker_color='red',
                text=late_percentages,
                textposition='inside'
            ))
            
            # Update layout for 100% stacked bar chart
            fig.update_layout(
                barmode='stack',
                title='100% Stacked Bar Chart: Late vs On Time by Dwell Time Category',
                xaxis_title='Dwell Time Category',
                yaxis_title='% of Total Shipments',
                legend_title='Compliance',
                xaxis_tickangle=-45
            )
            
            # Displaying in Streamlit
            st.plotly_chart(fig, use_container_width=True, key="weekly_100%_stacked")

        # Pivot: Average Dwell Time by Appointment Type (left column)
        with col2:
            with st.expander('Average Dwell Time by Appointment Type'):
                dwell_average_pivot = filtered_df.pivot_table(
                    values='Dwell Time',
                    index='Appointment Type',
                    columns='Compliance',
                    aggfunc='mean',
                    fill_value=np.nan
                ).reset_index()

                dwell_average_pivot['Grand Average'] = dwell_average_pivot.select_dtypes(include=[np.number]).mean(axis=1)

                # Calculate Grand Average row
                grand_avg_row = dwell_average_pivot.select_dtypes(include=[np.number]).mean().to_frame().T
                grand_avg_row['Appointment Type'] = 'Grand Average'
                dwell_average_pivot = pd.concat([dwell_average_pivot, grand_avg_row], ignore_index=True)

                # Make sure the rendering part is inside the expander
                st.subheader('Average Dwell Time by Appointment Type')
                st.table(dwell_average_pivot)

            # Create a grouped bar chart to visualize dwell time by compliance (Late and On Time)
            if 'Late' in dwell_average_pivot.columns and 'On Time' in dwell_average_pivot.columns:
                fig = go.Figure()
                
                # Add bars for Late
                fig.add_trace(go.Bar(
                    x=dwell_average_pivot['Appointment Type'],
                    y=dwell_average_pivot['Late'],
                    name='Late',
                    marker=dict(color='rgba(255, 0, 0, 0.7)'),  # Red color with transparency
                    text=[f'{val:.1f}%' for val in dwell_average_pivot['Late']],  # Add percentages as text
                    textposition='auto',
                    textfont=dict(color='white')  # Set text color to white
                ))
                
                # Add bars for On Time
                fig.add_trace(go.Bar(
                    x=dwell_average_pivot['Appointment Type'],
                    y=dwell_average_pivot['On Time'],
                    name='On Time',
                    marker=dict(color='rgba(0, 128, 0, 0.7)'),  # Green color with transparency
                    text=[f'{val:.1f}%' for val in dwell_average_pivot['On Time']],  # Add percentages as text
                    textposition='auto',
                    textfont=dict(color='white')  # Set text color to white
                ))

                # Update layout for better readability
                fig.update_layout(
                    title='Average Dwell Time by Appointment Type and Compliance',
                    xaxis_title='Appointment Type',
                    yaxis_title='Average Dwell Time',
                    barmode='group',
                    xaxis_tickangle=-45,
                    legend_title='Compliance',
                    height=500,
                    width=800
                )

                # Display the chart in Streamlit
                st.plotly_chart(fig, use_container_width=True, key="weekly_grouped_bar")

with tabs[3]:
    st.header("Monthly Dashboard")
    selected_month = st.selectbox("Select Month for Monthly Report:", options=["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"])
    if selected_month:
        # Filter data based on the selected month
        filtered_df = merged_df[merged_df['Month'] == selected_month]

        # Create two columns for layout
        col1, col2 = st.columns([1, 1])
        
        # Pivot: On Time Compliance by Month (left column)
        with col1:
            with st.expander('On Time Compliance by Month'):
                compliance_pivot = filtered_df.pivot_table(
                    values='Shipment ID', 
                    index='Month',
                    columns='Compliance',
                    aggfunc='count',
                    fill_value=0
                ).reset_index()
                compliance_pivot['Grand Total'] = compliance_pivot.select_dtypes(include=[np.number]).sum(axis=1)
                compliance_pivot['On Time %'] = round((compliance_pivot.get('On Time', 0) / compliance_pivot['Grand Total']) * 100, 2)
                compliance_pivot.style.format({'On Time %': lambda x: '{:.2f}%'.format(x).rstrip('0').rstrip('.')})
                st.subheader('On Time Compliance by Month')
                st.table(compliance_pivot)

            # Add Line Chart for Compliance Trend within the Month
            
            # Aggregating the data by date and compliance status
            trend_data = filtered_df.groupby(['Scheduled Date', 'Compliance']).size().unstack(fill_value=0).reset_index()

            # Create line chart
            fig = go.Figure()

            # Add 'On Time' line to the chart
            if 'On Time' in trend_data.columns:
                fig.add_trace(go.Scatter(
                    x=trend_data['Scheduled Date'], 
                    y=trend_data['On Time'], 
                    mode='lines+markers+text',
                    name='On Time',
                    line=dict(color='green'),
                    text=trend_data['On Time'],  # Add counts as text labels
                    textposition='top center',  # Positioning the text above the points
                    textfont=dict(color='white'),  # Make the text color white
                ))

            # Add 'Late' line to the chart
            if 'Late' in trend_data.columns:
                fig.add_trace(go.Scatter(
                    x=trend_data['Scheduled Date'], 
                    y=trend_data['Late'], 
                    mode='lines+markers+text',
                    name='Late',
                    line=dict(color='red'),
                    text=trend_data['Late'],  # Add counts as text labels
                    textposition='top center',  # Positioning the text above the points
                    textfont=dict(color='white'),  # Make the text color white
                ))

            # Update layout for better readability
            fig.update_layout(
                title='Compliance Trend Over the Selected Month',
                xaxis_title='Scheduled Date',
                yaxis_title='Number of Shipments',
                xaxis=dict(type='category'),  # Ensures dates are shown correctly even if sparse
                template='plotly_white'
            )

            st.plotly_chart(fig, use_container_width=True, key="monthly_line_chart")

        # Pivot: On Time Compliance by Carrier (right column)
        with col1:
            with st.expander('On Time Compliance by Carrier'):
                # Creating the pivot table
                carrier_pivot = filtered_df.pivot_table(
                    values='Shipment ID',
                    index='Carrier',
                    columns='Compliance',
                    aggfunc='count',
                    fill_value=0
                ).reset_index()
                
                # Calculating Grand Total and On Time %
                carrier_pivot['Grand Total'] = carrier_pivot.select_dtypes(include=[np.number]).sum(axis=1)
                carrier_pivot['On Time %'] = round((carrier_pivot.get('On Time', 0) / carrier_pivot['Grand Total']) * 100, 2)
                
                # Sorting carriers by On Time % in descending order
                carrier_pivot = carrier_pivot.sort_values(by='On Time %', ascending=False)
                
                # Displaying the pivot table in the expander
                st.subheader('On Time Compliance by Carrier')
                st.table(carrier_pivot)

            # Creating the heat map below the expander in col1
            # Prepare the data for the heat map
            heatmap_data = carrier_pivot.set_index('Carrier')[['On Time %']]
            
            # Plotting the heat map using Plotly to blend better with Streamlit
            fig = go.Figure(data=go.Heatmap(
                z=heatmap_data['On Time %'].values.reshape(-1, 1),
                x=['On Time %'],
                y=heatmap_data.index,
                colorscale='RdYlGn',  # Red to Green color map
                colorbar=dict(title="On Time %"),
                text=heatmap_data['On Time %'].values.reshape(-1, 1),
                texttemplate="%{text:.2f}%",
                showscale=True
            ))
            
            # Customizing the plot layout
            fig.update_layout(
                title='On Time Compliance Percentage by Carrier',
                xaxis_title='',
                yaxis_title='Carrier',
                yaxis_autorange='reversed',
                height=len(heatmap_data) * 40 + 100  # Dynamic height based on number of carriers
            )
            
            # Displaying the heat map using Streamlit
            st.plotly_chart(fig, use_container_width=True, key="monthly_heatmap")

        # Daily Count by Dwell Time (right column)
        with col2:
            with st.expander('Monthly Count by Dwell Time'):
                # Check if 'Dwell Time' column exists in filtered_df to avoid KeyError
                if 'Dwell Time' in filtered_df.columns:
                    # Creating 'Dwell Time Category' column if it doesn't exist
                    filtered_df['Dwell Time Category'] = pd.cut(
                        filtered_df['Dwell Time'],
                        bins=[0, 2, 3, 4, 5, np.inf],
                        labels=['less than 2 hours', '2 to 3 hours', '3 to 4 hours', '4 to 5 hours', '5 or more hours']
                    )
                else:
                    st.error("'Dwell Time' column is missing from the dataset.")

                # Assuming dwell_count_pivot already exists
                dwell_count_pivot = filtered_df.pivot_table(
                    values='Shipment ID',
                    index='Dwell Time Category',
                    columns='Compliance',
                    aggfunc='count',
                    fill_value=0
                ).reset_index()
                dwell_count_pivot['Grand Total'] = dwell_count_pivot.select_dtypes(include=[np.number]).sum(axis=1)
                dwell_count_pivot['Late % of Total'] = round((dwell_count_pivot.get('Late', 0) / dwell_count_pivot['Grand Total']) * 100, 2)
                dwell_count_pivot['On Time % of Total'] = round((dwell_count_pivot.get('On Time', 0) / dwell_count_pivot['Grand Total']) * 100, 2)
                
                # Also showing the table for additional clarity
                st.subheader('Monthly Count by Dwell Time')
                st.table(dwell_count_pivot)

            # Creating a 100% stacked bar chart using Plotly
            categories = dwell_count_pivot['Dwell Time Category']
            late_percentages = dwell_count_pivot['Late % of Total']
            on_time_percentages = dwell_count_pivot['On Time % of Total']
            
            # Plotting with Plotly
            fig = go.Figure()
            
            # Add On Time bars
            fig.add_trace(go.Bar(
                x=categories,
                y=on_time_percentages,
                name='On Time',
                marker_color='green',
                text=on_time_percentages,
                textposition='inside'
            ))
            
            # Add Late bars
            fig.add_trace(go.Bar(
                x=categories,
                y=late_percentages,
                name='Late',
                marker_color='red',
                text=late_percentages,
                textposition='inside'
            ))
            
            # Update layout for 100% stacked bar chart
            fig.update_layout(
                barmode='stack',
                title='100% Stacked Bar Chart: Late vs On Time by Dwell Time Category',
                xaxis_title='Dwell Time Category',
                yaxis_title='% of Total Shipments',
                legend_title='Compliance',
                xaxis_tickangle=-45
            )
            
            # Displaying in Streamlit
            st.plotly_chart(fig, use_container_width=True, key="monthly_100%_stacked")

        # Pivot: Average Dwell Time by Appointment Type (left column)
        with col2:
            with st.expander('Average Dwell Time by Appointment Type'):
                dwell_average_pivot = filtered_df.pivot_table(
                    values='Dwell Time',
                    index='Appointment Type',
                    columns='Compliance',
                    aggfunc='mean',
                    fill_value=np.nan
                ).reset_index()

                dwell_average_pivot['Grand Average'] = dwell_average_pivot.select_dtypes(include=[np.number]).mean(axis=1)

                # Calculate Grand Average row
                grand_avg_row = dwell_average_pivot.select_dtypes(include=[np.number]).mean().to_frame().T
                grand_avg_row['Appointment Type'] = 'Grand Average'
                dwell_average_pivot = pd.concat([dwell_average_pivot, grand_avg_row], ignore_index=True)

                # Make sure the rendering part is inside the expander
                st.subheader('Average Dwell Time by Appointment Type')
                st.table(dwell_average_pivot)

            # Create a grouped bar chart to visualize dwell time by compliance (Late and On Time)
            if 'Late' in dwell_average_pivot.columns and 'On Time' in dwell_average_pivot.columns:
                fig = go.Figure()
                
                # Add bars for Late
                fig.add_trace(go.Bar(
                    x=dwell_average_pivot['Appointment Type'],
                    y=dwell_average_pivot['Late'],
                    name='Late',
                    marker=dict(color='rgba(255, 0, 0, 0.7)'),  # Red color with transparency
                    text=[f'{val:.1f}%' for val in dwell_average_pivot['Late']],  # Add percentages as text
                    textposition='auto',
                    textfont=dict(color='white')  # Set text color to white
                ))
                
                # Add bars for On Time
                fig.add_trace(go.Bar(
                    x=dwell_average_pivot['Appointment Type'],
                    y=dwell_average_pivot['On Time'],
                    name='On Time',
                    marker=dict(color='rgba(0, 128, 0, 0.7)'),  # Green color with transparency
                    text=[f'{val:.1f}%' for val in dwell_average_pivot['On Time']],  # Add percentages as text
                    textposition='auto',
                    textfont=dict(color='white')  # Set text color to white
                ))

                # Update layout for better readability
                fig.update_layout(
                    title='Average Dwell Time by Appointment Type and Compliance',
                    xaxis_title='Appointment Type',
                    yaxis_title='Average Dwell Time',
                    barmode='group',
                    xaxis_tickangle=-45,
                    legend_title='Compliance',
                    height=500,
                    width=800
                )

                # Display the chart in Streamlit
                st.plotly_chart(fig, use_container_width=True, key="monthly_grouped_bar")

with tabs[4]:
    st.header("Year-to-Date (YTD) Dashboard")
    
    # Filtered DataFrame for YTD (using the entire merged_df)
    ytd_df = merged_df  # No need to filter, as we use the full dataset for YTD

    # Create two columns for layout
    col1, col2 = st.columns([1, 1])

    # Pivot: On Time Compliance by Week (left column)
    with col1:
        with st.expander('YTD On Time Compliance'):
            compliance_pivot = ytd_df.pivot_table(
                values='Shipment ID', 
                columns='Compliance',
                aggfunc='count',
                fill_value=0
            ).reset_index(drop=True)
            compliance_pivot['Grand Total'] = compliance_pivot.select_dtypes(include=[np.number]).sum(axis=1)
            compliance_pivot['On Time %'] = round((compliance_pivot.get('On Time', 0) / compliance_pivot['Grand Total']) * 100, 2)
            compliance_pivot.style.format({'On Time %': lambda x: '{:.2f}%'.format(x).rstrip('0').rstrip('.')})
            st.subheader('YTD On Time Compliance')
            st.table(compliance_pivot)

        # Add Line Chart for Compliance Trend (using the last day of each month for data points)
        trend_data = ytd_df.groupby(['Scheduled Date', 'Compliance']).size().unstack(fill_value=0).reset_index()
        trend_data['Scheduled Date'] = pd.to_datetime(trend_data['Scheduled Date'])
        trend_data['Scheduled Date'] = pd.to_datetime(trend_data['Scheduled Date'])
        

        # Create line chart
        fig = go.Figure()

        # Add 'On Time' line to the chart
        if 'On Time' in trend_data.columns:
            fig.add_trace(go.Scatter(
                x=trend_data['Scheduled Date'], 
                y=trend_data['On Time'], 
                mode='lines+markers',
                name='On Time',
                line=dict(color='green')
            ))

        # Add 'Late' line to the chart
        if 'Late' in trend_data.columns:
            fig.add_trace(go.Scatter(
                x=trend_data['Scheduled Date'], 
                y=trend_data['Late'], 
                mode='lines+markers',
                name='Late',
                line=dict(color='red')
            ))

        fig.update_layout(
            title='Compliance Trend Over the Year',
            xaxis_title='Scheduled Date',
            yaxis_title='Number of Shipments',
            xaxis=dict(type='category'),
            template='plotly_white'
        )

        st.plotly_chart(fig, use_container_width=True, key="ytd_line_chart")

    # Pivot: On Time Compliance by Carrier (right column)
    with col1:
        with st.expander('On Time Compliance by Carrier'):
            carrier_pivot = ytd_df.pivot_table(
                values='Shipment ID',
                index='Carrier',
                columns='Compliance',
                aggfunc='count',
                fill_value=0
            ).reset_index()
            
            carrier_pivot['Grand Total'] = carrier_pivot.select_dtypes(include=[np.number]).sum(axis=1)
            carrier_pivot['On Time %'] = round((carrier_pivot.get('On Time', 0) / carrier_pivot['Grand Total']) * 100, 2)
            carrier_pivot = carrier_pivot.sort_values(by='On Time %', ascending=False)
            
            st.subheader('On Time Compliance by Carrier')
            st.table(carrier_pivot)

        # Heat map for On Time Compliance by Carrier
        heatmap_data = carrier_pivot.set_index('Carrier')[['On Time %']]
        fig = go.Figure(data=go.Heatmap(
            z=heatmap_data['On Time %'].values.reshape(-1, 1),
            x=['On Time %'],
            y=heatmap_data.index,
            colorscale='RdYlGn',
            colorbar=dict(title="On Time %"),
            text=heatmap_data['On Time %'].values.reshape(-1, 1),
            texttemplate="%{text:.2f}%",
            showscale=True
        ))
        fig.update_layout(
            title='On Time Compliance Percentage by Carrier',
            xaxis_title='',
            yaxis_title='Carrier',
            yaxis_autorange='reversed',
            height=len(heatmap_data) * 40 + 100
        )
        st.plotly_chart(fig, use_container_width=True, key="ytd_heatmap")

    # Weekly Count by Dwell Time (right column)
    with col2:
        with st.expander('Weekly Count by Dwell Time'):
            if 'Dwell Time' in ytd_df.columns:
                ytd_df['Dwell Time Category'] = pd.cut(
                    ytd_df['Dwell Time'],
                    bins=[0, 2, 3, 4, 5, np.inf],
                    labels=['less than 2 hours', '2 to 3 hours', '3 to 4 hours', '4 to 5 hours', '5 or more hours']
                )
            else:
                st.error("'Dwell Time' column is missing from the dataset.")

            dwell_count_pivot = ytd_df.pivot_table(
                values='Shipment ID',
                index='Dwell Time Category',
                columns='Compliance',
                aggfunc='count',
                fill_value=0
            ).reset_index()
            dwell_count_pivot['Grand Total'] = dwell_count_pivot.select_dtypes(include=[np.number]).sum(axis=1)
            dwell_count_pivot['Late % of Total'] = round((dwell_count_pivot.get('Late', 0) / dwell_count_pivot['Grand Total']) * 100, 2)
            dwell_count_pivot['On Time % of Total'] = round((dwell_count_pivot.get('On Time', 0) / dwell_count_pivot['Grand Total']) * 100, 2)
            
            st.subheader('Weekly Count by Dwell Time')
            st.table(dwell_count_pivot)

        categories = dwell_count_pivot['Dwell Time Category']
        late_percentages = dwell_count_pivot['Late % of Total']
        on_time_percentages = dwell_count_pivot['On Time % of Total']
        
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=categories,
            y=on_time_percentages,
            name='On Time',
            marker_color='green',
            text=on_time_percentages,
            textposition='inside'
        ))
        fig.add_trace(go.Bar(
            x=categories,
            y=late_percentages,
            name='Late',
            marker_color='red',
            text=late_percentages,
            textposition='inside'
        ))
        fig.update_layout(
            barmode='stack',
            title='100% Stacked Bar Chart: Late vs On Time by Dwell Time Category',
            xaxis_title='Dwell Time Category',
            yaxis_title='% of Total Shipments',
            legend_title='Compliance',
            xaxis_tickangle=-45
        )
        st.plotly_chart(fig, use_container_width=True, key="ytd_100%_stacked")

    # Average Dwell Time by Appointment Type (right column)
    with col2:
        with st.expander('Average Dwell Time by Appointment Type'):
            dwell_average_pivot = ytd_df.pivot_table(
                values='Dwell Time',
                index='Appointment Type',
                columns='Compliance',
                aggfunc='mean',
                fill_value=np.nan
            ).reset_index()

            dwell_average_pivot['Grand Average'] = dwell_average_pivot.select_dtypes(include=[np.number]).mean(axis=1)

            grand_avg_row = dwell_average_pivot.select_dtypes(include=[np.number]).mean().to_frame().T
            grand_avg_row['Appointment Type'] = 'Grand Average'
            dwell_average_pivot = pd.concat([dwell_average_pivot, grand_avg_row], ignore_index=True)

            st.subheader('Average Dwell Time by Appointment Type')
            st.table(dwell_average_pivot)

        if 'Late' in dwell_average_pivot.columns and 'On Time' in dwell_average_pivot.columns:
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=dwell_average_pivot['Appointment Type'],
                y=dwell_average_pivot['Late'],
                name='Late',
                marker=dict(color='rgba(255, 0, 0, 0.7)'),
                text=[f'{val:.1f}%' for val in dwell_average_pivot['Late']],
                textposition='auto',
                textfont=dict(color='white')
            ))
            fig.add_trace(go.Bar(
                x=dwell_average_pivot['Appointment Type'],
                y=dwell_average_pivot['On Time'],
                name='On Time',
                marker=dict(color='rgba(0, 128, 0, 0.7)'),
                text=[f'{val:.1f}%' for val in dwell_average_pivot['On Time']],
                textposition='auto',
                textfont=dict(color='white')
            ))
            fig.update_layout(
                title='Average Dwell Time by Appointment Type and Compliance',
                xaxis_title='Appointment Type',
                yaxis_title='Average Dwell Time',
                barmode='group',
                xaxis_tickangle=-45,
                legend_title='Compliance',
                height=500,
                width=800
            )
            st.plotly_chart(fig, use_container_width=True, key="ytd_grouped_bar")