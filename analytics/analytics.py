from time import sleep, mktime
from os import environ
import json
import pandas as pd
from sqlalchemy import create_engine, inspect, Column, MetaData, String, Table, Integer, Float
from sqlalchemy.exc import OperationalError
from datetime import timedelta, datetime
from geopy import distance

print('Waiting for the data generator...')
sleep(20)
print('ETL Starting...')


#Functions for converting timestamp to date and time
def convert_timestamp_to_date(timestamp):
    return datetime.fromtimestamp(int(timestamp)).strftime("%Y%m%d")

def convert_timestamp_to_time(timestamp):
    return datetime.fromtimestamp(int(timestamp)).strftime("%H")

#Function for calculating distance
def calculate_dist(a,b):
    return distance.distance(a, b).km

#Connection strings of pql db(source) and mysql db(destination)
psql_cs = environ["POSTGRESQL_CS"]
mysql_cs = environ["MYSQL_CS"]

#Create devices_results table in mysql db
while True:
    try:
        mysql_engine = create_engine(mysql_cs, pool_pre_ping=True, pool_size=5)
        if not inspect(mysql_engine).has_table("devices_results"): 
            metadata_mysql = MetaData()
            devices_results = Table(
                'devices_results', metadata_mysql,
                Column('device_id', String(36)),
                Column('date', String(8)),
                Column('time', String(2)),
                Column('max_temperature', Integer),
                Column('point_nbr', Integer),
                Column('total_distance', Float)
            )
            metadata_mysql.create_all(mysql_engine)
            mysql_engine.dispose()
        break
    except OperationalError:
        sleep(0.1)

#wait
wiat_time = (60-datetime.now().minute)*60
print(f"{wiat_time} seconds wait...")
sleep(wiat_time)



#ETL PROCESS
while True:
    #Check last state of inserted data into mysql db and generate missing period of time
    check_query = "select max(str_to_date(concat(date,time),'%%Y%%m%%d%%H')) as last_insert_date from devices_results"
    df_checked = pd.read_sql_query(check_query, mysql_cs)
    last_insert = str(list(df_checked['last_insert_date'])[0])
    if last_insert == 'None':
        min_time_stamp = 0
    else:
        min_time_stamp = mktime(datetime.strptime(last_insert, "%Y-%m-%d %H:%M:%S").timetuple())+3600

    max_date_time = datetime.now().strftime("%Y%m%d%H") + '0000'
    max_time_stamp = datetime.strptime(max_date_time, "%Y%m%d%H%M%S").timestamp()

    #Extract missing data from psl db 
    fetch_query = f"SELECT * FROM devices where cast(time as int) >= {min_time_stamp} and cast(time as int) < {max_time_stamp}"
    df_fetched = pd.read_sql_query(fetch_query, psql_cs)

    #Transform extracted data into intended format 
    if len(df_fetched.index) != 0:
        try:
            df_fetched['location'] = df_fetched['location'].apply(lambda x:(json.loads(x)['latitude'],json.loads(x)['longitude']))
            df_fetched['date'] = df_fetched['time'].apply(convert_timestamp_to_date)
            df_fetched['time'] = df_fetched['time'].apply(convert_timestamp_to_time)
            df_fetched = df_fetched.sort_values(by=['device_id','date','time'])
            df_fetched['dest_location'] = df_fetched.groupby(['device_id','date','time'])['location'].shift()
            df_fetched['dest_location'] = df_fetched['dest_location'].fillna(df_fetched['location'])
            df_fetched['distance'] = df_fetched.apply(lambda x: calculate_dist(x["location"], x["dest_location"]), axis = 1)
            df_fetched.drop(['location','dest_location'], inplace=True, axis=1)
            df_fetched = df_fetched.groupby(['device_id','date','time'], as_index=False).\
                        agg(max_temperature=('temperature','max'), point_nbr=('temperature','size'),total_distance=('distance','sum'))
            #Load processed Data into mysql db
            df_fetched.to_sql(name='devices_results', con=mysql_cs, if_exists='append', index=False)
        except:
            pass
    sleep(3600)
    min_date = convert_timestamp_to_date(min_time_stamp) + convert_timestamp_to_time(min_time_stamp)
    max_date = convert_timestamp_to_date(max_time_stamp) + convert_timestamp_to_time(max_time_stamp)
    print(f'ETL is started for datetime between {min_date} and {max_date}')