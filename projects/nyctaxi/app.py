import streamlit as st
import pandas as pd


def load_data():
    df = pd.read_csv('./projects/nyctaxi/out/trip_distance.csv')
    df = df.drop('hour', axis=1)
    return df


data = load_data()
max_trip_distance = data['trip_distance'].max()
max_hour = data['trip_distance'].idxmax()
min_trip_distance = data['trip_distance'].min()
min_hour = data['trip_distance'].idxmin()

with st.container():
    st.info('The average distance driven by yellow and green taxis per hour')
    col1, col2 = st.columns([1, 1])
    col1.metric(label="Max average trip distance", value=round(max_trip_distance))
    col1.metric(label="Max hour", value=max_hour)
    col2.metric(label="Min average trip distance", value=round(min_trip_distance))
    col2.metric(label="Min hour", value=min_hour)
    st.bar_chart(data)
