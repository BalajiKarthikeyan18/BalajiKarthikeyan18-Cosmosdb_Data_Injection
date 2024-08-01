import streamlit as st
import networkx as nx
import matplotlib.pyplot as plt
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

# Initialize Gremlin client (update with your Cosmos DB details)
gremlin_client = client.Client(
    'wss://' + ENDPOINT + ':443/', 'g',
    username="/dbs/" + DATABASE + "/colls/" + COLLECTION,
    password=PRIMARY_KEY,
    message_serializer=serializer.GraphSONSerializersV2d0()
    )
# Function to retrieve the whole graph
def get_whole_graph():
    query = "g.V().as('v').outE().as('e').inV().as('v2').select('v','e','v2')"
    callback = gremlin_client.submitAsync(query)
    result = callback.result()
    
    nodes = {}
    edges = []
    
    for item in result:
        v = item['v']
        v2 = item['v2']
        e = item['e']
        
        nodes[v.id] = v
        nodes[v2.id] = v2
        edges.append((v.id, v2.id, e.label))
    
    return nodes, edges

# Streamlit UI
st.title('Graph Visualization')

# Retrieve and visualize the graph
if st.button('Retrieve and Visualize Graph'):
    nodes, edges = get_whole_graph()
    
    # Initialize a NetworkX graph
    G = nx.Graph()
    
    # Add nodes
    for node_id, node_data in nodes.items():
        G.add_node(node_id, label=node_data.label, **node_data.properties)
    
    # Add edges
    for edge in edges:
        G.add_edge(edge[0], edge[1], label=edge[2])
    
    # Draw the graph
    plt.figure(figsize=(12, 8))
    pos = nx.spring_layout(G)
    labels = nx.get_node_attributes(G, 'name')
    nx.draw(G, pos, labels=labels, with_labels=True, node_color='skyblue', node_size=3000, font_size=15, font_color='black', font_weight='bold', edge_color='gray')
    
    # Show the plot
    st.pyplot(plt)

# Clean up the client
gremlin_client.close()
