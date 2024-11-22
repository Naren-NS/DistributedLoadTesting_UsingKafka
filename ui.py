import streamlit as st
import requests
import json

def send_start_test(test_config):
    # Assuming your Flask API is running on localhost and port 5000
    response = requests.post("http://localhost:5000/start_test", json=test_config)
    try:
        st.success(print(response.content))
        # result = response.json()
        return response.content
    except json.decoder.JSONDecodeError:
        return None

def send_stop_test():
    response = requests.post("http://localhost:5000/stop_test")
    try:
        result = response.json()
        return result
    except json.decoder.JSONDecodeError:
        return None

def get_metrics():
    response = requests.get("http://localhost:5000/metrics")
    try:
        metrics = response.json()
        return metrics
    except json.decoder.JSONDecodeError:
        return None

st.title('Orchestrator Control Panel')

if st.button('Start Test'):
    test_config = {}  # Add your test configuration parameters here
    result = send_start_test(test_config)
    if result is not None:
        st.write(result)
    else:
        st.write("Error: Failed to parse response as JSON")

if st.button('Stop Test'):
    result = send_stop_test()
    if result is not None:
        st.write(result)
    else:
        st.write("Error: Failed to parse response as JSON")

if st.button('Show Metrics'):
    metrics = get_metrics()
    if metrics is not None:
        st.json(metrics)
    else:
        st.write("Error: Failed to parse response as JSON")