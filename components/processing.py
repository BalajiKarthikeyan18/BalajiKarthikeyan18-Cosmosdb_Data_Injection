import pandas as pd
import streamlit as st
import nest_asyncio
from gremlin_python.driver import client, serializer
import json

with open('config.json') as config_file:
    config = json.load(config_file)

# ENDPOINT = st.secrets["ENDPOINT"]
# PRIMARY_KEY = st.secrets["PRIMARY_KEY"]
# DATABASE = st.secrets["DATABASE"]
# COLLECTION = st.secrets["COLLECTION"]

ENDPOINT = config["ENDPOINT"]
PRIMARY_KEY = config["PRIMARY_KEY"]
DATABASE = config["DATABASE"]
COLLECTION = config["COLLECTION"]

def load(files):
    VERTICES,EDGES=[],[]
    counter=0
    if files:
        for file in files:
            counter+=1
            st.write("File:",file.name)
            df = pd.read_csv(file)
            
            if file.name=="categories.csv":
                df_categories=df
                for column in df_categories.columns:
                    df_categories[column] = df_categories[column].fillna('').apply(lambda x: str(x).replace("'", "\\\'") if isinstance(x, str) else x)
                st.write(df_categories.head())
                for index,data in df_categories.iterrows():
                    VERTICES.append(f"g.addV('categories').property('categoryid',{data['categoryid']}).property('categoryname','{data['categoryname']}').property('description','{data['description']}').property('type','categories')")

            elif file.name=="customers.csv":
                df_customers=df
                for column in df_customers.columns:
                    df_customers[column] = df_customers[column].fillna('').apply(lambda x: str(x).replace("'", "\\\'") if isinstance(x, str) else x)
                st.write(df_customers.head())
                for index,data in df_customers.iterrows():
                    VERTICES.append(f"g.addV('customers').property('customerid','{data['customerid']}').property('companyname','{data['companyname']}').property('contactname','{data['contactname']}').property('contacttitle','{data['contacttitle']}').property('address','{data['address']}').property('city','{data['city']}').property('region','{data['region']}').property('postalcode','{data['postalcode']}').property('country','{data['country']}').property('phone','{data['phone']}').property('fax','{data['fax']}').property('type','{data['customerid']}')")

            elif file.name=="employee_territories.csv":
                df_employee_territories=df
                st.write(df_employee_territories.head())
                for index,data in df_employee_territories.iterrows():
                    VERTICES.append(f"g.addV('employee_territories').property('employeeid',{data['employeeid']}).property('territoryid',{data['territoryid']}).property('type','{data['employeeid']}')")
                
            elif file.name=="employees.csv":
                df_employees=df
                for column in df_employees.columns:
                    if df_employees[column].dtype == 'object':
                        df_employees[column] = df_employees[column].fillna('').apply(lambda x: str(x).replace("'", "\\\'"))
                    else:
                        df_employees[column] = df_employees[column].fillna(0)  # Fill null values for non-string columns with 0
                st.write(df_employees.head())
                for index,data in df_employees.iterrows():
                    VERTICES.append(f"g.addV('employees').property('employeeid',{data['employeeid']}).property('lastname','{data['lastname']}').property('firstname','{data['firstname']}').property('title','{data['title']}').property('titleofcourtesy','{data['titleofcourtesy']}').property('birthdate','{data['birthdate']}').property('hiredate','{data['hiredate']}').property('address','{data['address']}').property('city','{data['city']}').property('region','{data['region']}').property('postalcode','{data['postalcode']}').property('country','{data['country']}').property('homephone','{data['homephone']}').property('extension',{data['extension']}).property('notes','{data['notes']}').property('reportsto',{data['reportsto']}).property('photopath','{data['photopath']}').property('type','{data['employeeid']}')")

            elif file.name=="orders_details.csv":
                df_orders_details=df
                st.write(df_orders_details.head())
                for index,data in df_orders_details.iterrows():
                    VERTICES.append(f"g.addV('orders_details').property('orderid',{int(data['orderid'])}).property('productid',{data['productid']}).property('unitprice',{data['unitprice']}).property('quantity',{data['quantity']}).property('discount',{data['discount']}).property('type','{data['orderid']}')")

            elif file.name=="orders.csv":
                df_orders=df
                for column in df_orders.columns:
                    df_orders[column] = df_orders[column].fillna('').apply(lambda x: str(x).replace("'", "\\\'") if isinstance(x, str) else x)
                st.write(df_orders.head())
                for index,data in df_orders.iterrows():
                    VERTICES.append(f"g.addV('orders').property('orderid',{data['orderid']}).property('customerid','{data['customerid']}').property('employeeid',{data['employeeid']}).property('orderdate','{data['orderdate']}').property('requireddate','{data['requireddate']}').property('shippeddate','{data['shippeddate']}').property('shipvia',{data['shipvia']}).property('freight',{data['freight']}).property('shipname','{data['shipname']}').property('shipaddress','{data['shipaddress']}').property('shipcity','{data['shipcity']}').property('shipregion','{data['shipregion']}').property('shippostalcode','{data['shippostalcode']}').property('shipcountry','{data['shipcountry']}').property('type','{data['orderid']}')")

            elif file.name=="products.csv":
                df_products=df
                for column in df_products.columns:
                    df_products[column] = df_products[column].fillna('').apply(lambda x: str(x).replace("'", "\\\'") if isinstance(x, str) else x)
                st.write(df_products.head())
                for index,data in df_products.iterrows():
                    VERTICES.append(f"g.addV('products').property('productid',{data['productid']}).property('productname','{data['productname']}').property('supplierid',{data['supplierid']}).property('categoryid',{data['categoryid']}).property('quantityperunit','{data['quantityperunit']}').property('unitprice',{data['unitprice']}).property('unitsinstock',{data['unitsinstock']}).property('unitsonorder',{data['unitsonorder']}).property('reorderlevel',{data['reorderlevel']}).property('discontinued',{data['discontinued']}).property('type','{data['supplierid']}')")

            elif file.name=="regions.csv":
                df_regions=df
                st.write(df_regions.head())
                for index,data in df_regions.iterrows():
                    VERTICES.append(f"g.addV('regions').property('regionid',{data['regionid']}).property('regiondescription','{data['regiondescription']}').property('type','{data['regionid']}')")

            elif file.name=="shippers.csv":
                df_shippers=df
                st.write(df_shippers.head())
                for index,data in df_shippers.iterrows():
                    VERTICES.append(f"g.addV('shippers').property('shipperid',{data['shipperid']}).property('companyname','{data['companyname']}').property('phone','{data['phone']}').property('type','shippers')")

            elif file.name=="suppliers.csv":
                df_suppliers=df
                for column in df_suppliers.columns:
                    df_suppliers[column] = df_suppliers[column].fillna('').apply(lambda x: str(x).replace("'", "\\\'") if isinstance(x, str) else x)
                st.write(df_suppliers.head())
                for index,data in df_suppliers.iterrows():
                    VERTICES.append(f"g.addV('suppliers').property('supplierid',{data['supplierid']}).property('companyname','{data['companyname']}').property('contactname','{data['contactname']}').property('contacttitle','{data['contacttitle']}').property('address','{data['address']}').property('city','{data['city']}').property('region','{data['region']}').property('postalcode','{data['postalcode']}').property('country','{data['country']}').property('phone','{data['phone']}').property('fax','{data['fax']}').property('homepage','{data['homepage']}').property('type','{data['supplierid']}')")

            elif file.name=="territories.csv":
                df_territories=df
                st.write(df_territories.head())
                for index,data in df_territories.iterrows():
                    VERTICES.append(f"g.addV('territories').property('territoryid',{data['territoryid']}).property('territorydescription','{data['territorydescription']}').property('regionid',{data['regionid']}).property('type','{data['regionid']}')")

            else:
                st.error("Unknown Dataset")
        
        if counter==11:
            #category and supplier
            for index,data in df_products.iterrows():
                EDGES.append(f"g.V().hasLabel('products').has('productid',{data['productid']}).has('categoryid',{data['categoryid']}).addE('category').to(g.V().hasLabel('categories').has('categoryid',{data['categoryid']}))")
                EDGES.append(f"g.V().hasLabel('products').has('productid',{data['productid']}).has('supplierid',{data['supplierid']}).addE('supplier').to(g.V().hasLabel('suppliers').has('supplierid',{data['supplierid']}))")
            
            #customer and employee and shipper
            for index,data in df_orders.iterrows():
                EDGES.append(f"g.V().hasLabel('orders').has('orderid',{data['orderid']}).has('customerid','{data['customerid']}').addE('customer').to(g.V().hasLabel('customers').has('customerid','{data['customerid']}'))")
                EDGES.append(f"g.V().hasLabel('orders').has('orderid',{data['orderid']}).has('employeeid',{data['employeeid']}).addE('employee').to(g.V().hasLabel('employees').has('employeeid',{data['employeeid']}))")
                EDGES.append(f"g.V().hasLabel('orders').has('orderid',{data['orderid']}).has('shipvia',{data['shipvia']}).addE('shipper').to(g.V().hasLabel('shippers').has('shipperid',{data['shipvia']}))")

            #employee_info and territory 
            for index,data in df_employee_territories.iterrows():
                EDGES.append(f"g.V().hasLabel('employee_territories').has('employeeid',{data['employeeid']}).has('territoryid', {data['territoryid']}).addE('employee_info').to(g.V().hasLabel('employees').has('employeeid',{data['employeeid']}))")
                EDGES.append(f"g.V().hasLabel('employee_territories').has('employeeid',{data['employeeid']}).has('territoryid',{data['territoryid']}).addE('territory').to(g.V().hasLabel('territories').has('territoryid',{data['territoryid']}))")

            #order and product
            for index,data in df_orders_details.iterrows():
                EDGES.append(f"g.V().hasLabel('orders_details').has('orderid',{data['orderid']}).has('productid',{data['productid']}).addE('order').to(g.V().hasLabel('orders').has('orderid',{data['orderid']}))")
                EDGES.append(f"g.V().hasLabel('orders_details').has('orderid',{data['orderid']}).has('productid',{data['productid']}).addE('product').to(g.V().hasLabel('products').has('productid',{data['productid']}))")
                
            #region
            for index,data in df_territories.iterrows():
                EDGES.append(f"g.V().hasLabel('territories').has('territoryid',{data['territoryid']}).has('regionid',{data['regionid']}).addE('region').to(g.V().hasLabel('regions').has('regionid',{data['regionid']}))")

    return VERTICES,EDGES

def gremlin_connect(VERTICES,EDGES):

    # CONFIG STUFF - YOU NEED TO EDIT THIS
    # Make sure to create your Cosmos DB Gremlin API endpoint at https://portal.azure.com
    # Create the Database and Collection in the portal
    # This script will populate the data that we use in our demo


    # ENDPOINT = 'YOUR_ENDPOINT.gremlin.cosmosdb.azure.com'
    # PRIMARY_KEY = 'YOUR_PRIMARY_KEY'
    # DATABASE = 'YOUR_DATABASE_NAME'
    # COLLECTION = 'YOUR_COLLECTION_NAME'

    
    def insert_vertices(gremlin_client):
        for vertex in VERTICES:
            callback = gremlin_client.submitAsync(vertex)
            if callback.result() is None:            
                st.write("Something went wrong with this query: {0}".format(vertex))

    def insert_edges(gremlin_client):
        for edge in EDGES:
            callback = gremlin_client.submitAsync(edge)
            if callback.result() is None:            
                st.write("Something went wrong with this query:\n{0}".format(edge))

    def handler():
        # Initialize client
        st.write('Initializing client...')
        # GraphSON V2 is called out here, as V3 is not supported yet
        gremlin_client = client.Client(
            'wss://' + ENDPOINT + ':443/', 'g',
            username="/dbs/" + DATABASE + "/colls/" + COLLECTION,
            password=PRIMARY_KEY,
            message_serializer=serializer.GraphSONSerializersV2d0()
        )
        st.write('Client initialized!')

        # Insert vertices (nodes)
        insert_vertices(gremlin_client)

        st.write("Vertices inserted.")
        # Insert edges (relationships)
        insert_edges(gremlin_client)

        st.write("Edges inserted.")

        st.write('Data injected successfully.')
        
    handler()

def cleanup_graph():
    st.write("first step") 

    gremlin_client = client.Client(
            'wss://' + ENDPOINT + ':443/', 'g',
            username="/dbs/" + DATABASE + "/colls/" + COLLECTION,
            password=PRIMARY_KEY,
            message_serializer=serializer.GraphSONSerializersV2d0()

        )
    st.write("started cleanup")   
    callback = gremlin_client.submitAsync("g.V().drop()")
    st.write("second step")
    st.write(callback.result())

    if callback.result() is not None:
        return "Cleaned up the graph!"