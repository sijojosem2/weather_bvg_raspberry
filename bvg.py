import requests
import json, sys
from pandas import json_normalize
import sqlalchemy
import random
import time


input_data = {
    "api": "https://v5.bvg.transport.rest/stops/900007105/departures?results=10",
    "api_key": "74343b194f3e153814c9a84b5849894a",
    "db": {

        "host": "192.168.132.123",
        "database": "postgres",
        "user": "pi",
        "password": "SpamWalletRiverCircus",
        "port": "5432"
    },

    "table": "bvg_time_table",
    "location_table": "location_table2"
}



def connect(db_deets):
    try:
        print("\nConnecting to DB object...")
        engine = sqlalchemy.create_engine(
            "postgresql://{}:{}@{}:{}/{}".format(db_deets['user'], db_deets['password'], db_deets['host'],
                                                 db_deets['port'], db_deets['database']))
        print("\nEstablished connection to DB object : ", engine)
    except (Exception, sqlalchemy.exc.DataError) as error:
        print("\n Error while connecting to DB object :", error)
        sys.exit(1)
    return engine



def get_data(url):
    try:
        r = requests.get(url, timeout=5)
        print("\nURL Request Succesfull")
    except (
            Exception,
            requests.exceptions.Timeout,
            requests.exceptions.HTTPError,
    ) as error:
        print("\nURL Request Failure :", error)
        sys.exit(1)

    return r.json()



def create_df(json):

    return json_normalize(json)[['delay','direction', 'plannedWhen','line.product','line.name','stop.name']]



def pg_insert(connection, df, table_name):
    print("\nCommencing Load to {} ".format(table_name))

    try:

        df.to_sql(table_name, con=connection.execution_options(autocommit=True), index=False, if_exists='append')
        print("\n{} Load status : SUCCESS".format(table_name))
    except (Exception, sqlalchemy.exc.DataError) as error:
        print("\nDB Load status : FAILURE".format(table_name), error)
        connection.close()

    return



def main():


    time.sleep(random.uniform(0.11,59.99))
    pg_insert( connect(input_data['db']).connect(), create_df(get_data(input_data['api'])),  input_data['table'] )





if __name__ == "__main__":
    main()

