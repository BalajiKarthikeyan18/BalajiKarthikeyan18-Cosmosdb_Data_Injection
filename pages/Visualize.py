import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from gremlin_python.driver.client import Client
from gremlin_python.driver import serializer
import networkx as nx
import json

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

# Function to connect to Azure Cosmos DB Gremlin API
def connect_to_gremlin():
    client = Client(
        'wss://' + ENDPOINT + ':443/', 'g',
        username=f'/dbs/{DATABASE}/colls/{COLLECTION}',
        password=PRIMARY_KEY,
        message_serializer=serializer.GraphSONSerializersV2d0()
    )
    return client

# Function to execute a Gremlin query
def execute_query(client, query):
    callback = client.submitAsync(query)
    if callback.result() is not None:
        return callback.result().all().result()
    else:
        raise Exception("No result from the query")

# Streamlit app setup
st.title('Azure Cosmos DB Gremlin Graph Visualization')

try:
    # Connect to the Gremlin server
    client = connect_to_gremlin()

    # Fetch all vertices
    vertices_query = "g.V().valueMap(true)"
    vertices = execute_query(client, vertices_query)

    # Fetch all edges
    edges_query = "g.E()"
    edges = execute_query(client, edges_query)

    st.write(f"Number of vertices: {len(vertices)}")
    st.write(f"Number of edges: {len(edges)}")
    # st.write("vertices", vertices)
    # st.write("edges", edges)

    # Close the client connection
    client.close()

    # Create a NetworkX graph
    G = nx.Graph()
    idss = ["600eb32a-31b3-452f-958e-ea04d5694f34","f787c22d-0891-42aa-9d07-585a757488da","aa6e09ao-900f4a08-a7f5-f09236f3d66e","aa6e09aO-900f-4a08-a7f5-f09236f3d66e","86aa184f-2aed-4e42-b103-cea32d8600e2","c1158337-30e9-4aae-b3f8-b930cecc7961"]
# ID•. aa6e09ao-900f4a08-a7f5-f09236f3d66e

    # Add vertices
    for vertex in vertices:
        vertex_id = vertex.get('id', [None])
        if vertex_id not in idss:
            properties = {k: v[0] for k, v in vertex.items() if k != 'id' and k != 'label'}
            G.add_node(vertex_id, **properties)

    # Add edges
    for edge in edges:
        source_id = edge.get('inV', [None])
        target_id = edge.get('outV', [None])
        if source_id not in idss and target_id not in idss:
            G.add_edge(source_id, target_id)

    st.write("Graph formed")

    # Extract node positions
    pos = nx.spring_layout(G)

    # Prepare data for Plotly
    edge_x = []
    edge_y = []
    for edge in G.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_x += [x0, x1, None]
        edge_y += [y0, y1, None]

    edge_trace = go.Scatter(
        x=edge_x,
        y=edge_y,
        line=dict(width=1, color='white'),
        hoverinfo='none',
        mode='lines'
    )

    node_x = []
    node_y = []
    node_text = []
    for node in G.nodes(data=True):
        x, y = pos[node[0]]
        node_x.append(x)
        node_y.append(y)
        node_text.append(f"ID: {node[0]}")

    node_trace = go.Scatter(
        x=node_x,
        y=node_y,
        # text=node_text,
        mode='markers+text',
        marker=dict(size=10, color='blue'),
        textposition='top center',
        hoverinfo='text'
    )

    # Create figure
    fig = go.Figure()
    fig.add_trace(edge_trace)
    fig.add_trace(node_trace)

    fig.update_layout(
        showlegend=False,
        hovermode='closest',
        margin=dict(b=0, l=0, r=0, t=0),
        xaxis=dict(showgrid=False, zeroline=False),
        yaxis=dict(showgrid=False, zeroline=False)
    )

    # Display the figure in Streamlit
    st.plotly_chart(fig)

except Exception as e:
    st.error(f"An error occurred: {e}")
