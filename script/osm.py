from pyrosm import OSM, get_data, data
from pyrosm.data import sources
from shapely import geos
import pandas as pd
import psycopg2
import geopandas as gpd
from sqlalchemy import create_engine
import osmnx as ox


def get_network_edges(state_name):
    '''
    Texas and Florida have too large files to be loaded by get_network function in the local computer, use ox.graph_from_place instead
    '''
    fp = get_data(state_name)
    print('filepath to test data:', fp)
    osm = OSM(fp)
    edges = osm.get_network(nodes=False, network_type='driving')
    print(f"Get {state_name} network data successfully!")

    return edges
# The function is used to connect postresql


def db_connect(host, database, user, password, port):
    conn_string = 'postgresql://{0}:{1}@{2}:{3}/{4}'.format(
        user, password, host, port, database)
    pg_conn = psycopg2.connect(conn_string)
    cur = pg_conn.cursor()
    engine = create_engine(conn_string)
    sql_conn = engine.connect()

    return pg_conn, cur, sql_conn


def upload_data_to_postgres(host, database, user, password, port, schema, states):
    pg_conn, cur, sql_conn = db_connect(host, database, user, password, port)
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    try:
        # for table_name in list(states.values()):
        for table_name in list(states.values()):
            edges = get_network_edges(table_name)
            edges['geometry'] = edges.geometry.to_wkt()

            # Upload DataFrame to the table
            table_name = table_name.lower()
            edges.to_sql(table_name, sql_conn, schema,
                         if_exists='replace', index=False)
            full_table_name = f"{schema}.{table_name}"
            print(
                f"DataFrame uploaded to the table {full_table_name} successfully!")

    except Exception as e:
        print(f"Error creating schema and table or uploading DataFrame: {e}")

    pg_conn.commit()
    pg_conn.close()


def main():
    # Define schema and table names
    schema = 'geospatial'
    states = {
        'AK': 'Alaska',
        'AL': 'Alabama',
        'AR': 'Arkansas',
        'AZ': 'Arizona',
        'CA1': 'Northern California',
        'CA2': 'Southern California',
        'CO': 'Colorado',
        'CT': 'Connecticut',
        'DC': 'District of Columbia',
        'DE': 'Delaware',
        'FL': 'Florida',
        'GA': 'Georgia',
        'HI': 'Hawaii',
        'IA': 'Iowa',
        'ID': 'Idaho',
        'IL': 'Illinois',
        'IN': 'Indiana',
        'KS': 'Kansas',
        'KY': 'Kentucky',
        'LA': 'Louisiana',
        'MA': 'Massachusetts',
        'MD': 'Maryland',
        'ME': 'Maine',
        'MI': 'Michigan',
        'MN': 'Minnesota',
        'MO': 'Missouri',
        'MS': 'Mississippi',
        'MT': 'Montana',
        'NC': 'North Carolina',
        'ND': 'North Dakota',
        'NE': 'Nebraska',
        'NH': 'New Hampshire',
        'NJ': 'New Jersey',
        'NM': 'New Mexico',
        'NV': 'Nevada',
        'NY': 'New York',
        'OH': 'Ohio',
        'OK': 'Oklahoma',
        'OR': 'Oregon',
        'PA': 'Pennsylvania',
        'RI': 'Rhode Island',
        'SC': 'South Carolina',
        'SD': 'South Dakota',
        'TN': 'Tennessee',
        'TX': 'Texas',
        'UT': 'Utah',
        'VA': 'Virginia',
        'VT': 'Vermont',
        'WA': 'Washington',
        'WI': 'Wisconsin',
        'WV': 'West Virginia',
        'WY': 'Wyoming'
    }

    # PostgreSQL connection details
    host = 'localhost'
    database = 'national_volume_osm'
    user = 'postgres'
    password = 'wjh00813'
    port = 5432

#     #Load state bounding box to partition network data (some of the states like Florida and Texas have very large files)
#     states_shp = gpd.read_file('../osm/states_bbox/cb_2018_us_state_500k.shp')

    # upload dataframe to postgresql
    upload_data_to_postgres(host, database, user,
                            password, port, schema, states)


if __name__ == "__main__":
    main()
