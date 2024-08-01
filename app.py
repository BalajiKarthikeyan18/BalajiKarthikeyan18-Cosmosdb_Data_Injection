import streamlit as st
import pandas as pd
from components.processing import load,gremlin_connect,cleanup_graph


with st.sidebar:
    files = st.sidebar.file_uploader("Upload all the files:", type=["csv"], accept_multiple_files=True)

    # expander = st.expander("Clean the database?")
    
    # expander.caption('_:red[This will erase the whole database!]_')
    # clean=expander.button("Cleanup")
    # if clean:
    #     expander.write("first step") 
    #     expander.caption(cleanup_graph())

    


st.title("CosmosDB Data Injection: Northwind Dataset")
st.image("schema.jpeg", caption="Schema for reference")
   
if len(files)!=11:
    st.error("Incomplete uploaded files.")
else:
    VERTICES,EDGES=load(files)
    st.sidebar.write("Number of vertices to be inserted",len(VERTICES))
    st.sidebar.write("Number of EDGES to be inserted",len(EDGES))
    push=st.button("Push data into the database.")
    if push:
        gremlin_connect(VERTICES,EDGES)


    