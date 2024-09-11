import streamlit as st
import json
from gremlin_python.driver import client, serializer
import time
# from ..images import Autoscale

# with open('config.json') as config_file:
#     config = json.load(config_file)

# ENDPOINT = config["ENDPOINT"]
# PRIMARY_KEY = config["PRIMARY_KEY"]
# DATABASE = config["DATABASE"]
# COLLECTION = config["COLLECTION"]

ENDPOINT = st.secrets["ENDPOINT"]
PRIMARY_KEY = st.secrets["PRIMARY_KEY"]
DATABASE = st.secrets["DATABASE"]
COLLECTION = st.secrets["COLLECTION"]

st.title("Querying")

on = st.toggle("See Details regarding cost")
cost_for_storage = 0.25

if on:
    st.write("The cost of all database operations is normalized by Azure Cosmos DB and is expressed by Request Units (or RUs, for short). Request charge is the request units consumed by all your database operations.")
    st.write("Cost to  Store data =",cost_for_storage," $ per GB per month")
    st.write("Autoscale provisioned")
    st.image("./images/Autoscale.png", caption="Costing details")
    st.write("ServerLess")
    st.image("./images/ServerLess.png", caption="Costing details")
    st.write("Standard (manual) provisioned")
    st.image("./images/Standard.png", caption="Costing details")

gremlin_client = client.Client(
            'wss://' + ENDPOINT + ':443/', 'g',
            username="/dbs/" + DATABASE + "/colls/" + COLLECTION,
            password=PRIMARY_KEY,
            message_serializer=serializer.GraphSONSerializersV2d0()
        )
st.write('Connected to gremlin cosmos database successfully!')

query = st.text_area(label = "Enter Your query here")

def execute_query(client, query):
    start_time = time.time()  # Record the start time
    callback = client.submitAsync(query)

    if callback.result() is not None:
        result = callback.result().all().result()
        end_time = time.time()  # Record the end time
        execution_time = end_time - start_time  # Calculate the execution time
        return result, execution_time
    else:
        raise Exception("No result from the query")

if query :
    result, execution_time = execute_query(gremlin_client, query)
    st.write(f"Execution time: {execution_time:.3f} seconds")

    callback_time = gremlin_client.submitAsync(query+".executionProfile()")
    st.write("Total Resource Usage : ",callback_time.result().one()[0]["totalResourceUsage"])

    json_string = json.dumps(result)
    st.write(result)

    st.download_button(
        label="Download JSON",
        file_name="data.json",
        mime="application/json",
        data=json_string,
    )