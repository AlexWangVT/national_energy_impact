from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import psycopg2

spark = SparkSession.builder \
        .appName("Python Spark SQL basic example") \
        .config('spark.jars', 'file:///D:/postgresql_jar/postgresql-42.6.0.jar') \
        .getOrCreate()

def db_connect(host, database, user, password, port, schema, file_name):
    conn_string = 'postgresql://{0}:{1}@{2}:{3}/{4}'.format(user, password, host, port, database)
    pg_conn = psycopg2.connect(conn_string)
    cur = pg_conn.cursor()  
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    table_name = file_name.split('/')[4].split('.')[0].split(' ')[0]
    cur.execute(f"DROP TABLE IF EXISTS {schema}.{table_name}")
    pg_conn.commit()
    pg_conn.close()


def process_station_data(drop_cols, file_name):
    station_df = spark.read.csv(file_name, sep='|', header=True)
    station_df = station_df.drop(*drop_cols)
    station = station_df.withColumnRenamed('Travel_Lane', 'Year_Record1')\
                       .withColumnRenamed('Travel_Dir', 'Travel_Lane')\
                       .withColumnRenamed('Station_Id', 'Travel_Dir')\
                       .withColumnRenamed('State_Code', 'Station_Id')\
                       .withColumnRenamed('Year_Record', 'State_Code')\
                       .withColumnRenamed('Year_Record1', 'Year_Record')
    
    return station 


def load_data_to_postgres(df, schema, file_name, host, database, user, password, port):
    table_name = file_name.split('/')[4].split('.')[0].split(' ')[0]
    dbtable = f"{schema}.{table_name}"
    url = "jdbc:postgresql://{0}:{1}/{2}".format(host, port, database)
    df.write.format("jdbc")\
        .option('driver', 'org.postgresql.Driver') \
        .option("url", url).option('dbtable', dbtable) \
        .option("user", user).option("password", password).save()

def main():

    host = 'localhost'
    database = 'national_volume_osm'
    user = 'postgres'
    password = 'wjh00813'
    port = 5432
    drop_cols = ('Sample_Type_Volume', 'Num_Lanes_Volume', 'Method_Volume', 'Sample_Type_Class',\
                'Num_Lanes_Class', 'Method_Class', 'Algorithm_Volume', 'Sample_Type_Truck', \
                'Num_Lanes_Truck', 'Method_Truck', 'Data_Retrieval', 'Primary_Purpose', \
                'LRS_Id', 'LRS_Point', 'SHRP_Id', 'Is_Sample', 'Sample_Id', 'Con_Route_Signing', \
                    'Con_Signed_Route')
    
    states_dict = {
            'AK': 'Alaska',
            'AL': 'Alabama',
            'AR': 'Arkansas',
            'AZ': 'Arizona',
            'CA': 'Northern California',
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
    states = list(states_dict.keys())
    years = [range(2011, 2023)[-1]]
    months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
    schema_sta = 'station'
    
    sta_file_names = ["../HPMS_Data/{0}/{0}_station_data/{1}_{0} (TMAS).STA".format(year, state)\
                  for state in states for year in years]

    
    for file_name in sta_file_names:
        db_connect(host, database, user, password, port, schema_sta, file_name)
        station = process_station_data(drop_cols, file_name)
        load_data_to_postgres(station, schema_sta, file_name, host, database, user, password, port)


if __name__ == "__main__":
    main()