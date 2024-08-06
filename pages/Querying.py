import streamlit as st
import json
from gremlin_python.driver import client, serializer

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

gremlin_client = client.Client(
            'wss://' + ENDPOINT + ':443/', 'g',
            username="/dbs/" + DATABASE + "/colls/" + COLLECTION,
            password=PRIMARY_KEY,
            message_serializer=serializer.GraphSONSerializersV2d0()
        )
st.write('Connected to gremlin cosmos database successfully!')

query = st.text_area(label = "Enter Your query here")
    
if query :
    callback = gremlin_client.submitAsync(query)
    res = callback.result()
    l = []

    try :
        for i in res :
            l += i
    except StopIteration :
        pass
    
    json_string = json.dumps(l)

    st.json(json_string, expanded=True)

    st.download_button(
        label="Download JSON",
        file_name="data.json",
        mime="application/json",
        data=json_string,
    )
