import requests
import pandas as pd
import json, sys
from pandas import json_normalize
import sqlalchemy

input_data = {
    "api": "https://api.openweathermap.org/data/2.5/forecast?q={}&units=metric&lang=english&appid={}",
    "api_key": "123",
    "db": {

        "host": "localhost",
        "database": "postgres",
        "user": "pg",
        "password": "pg",
        "port": "5432"
    },

    "weather_table": "weather_table_11",
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


def pg_insert(connection, df, table_name):
    print("\nCommencing Load to {} ".format(table_name))

    try:

        df.to_sql(table_name, con=connection.execution_options(autocommit=True), index=False, if_exists='append')
        print("\n{} Load status : SUCCESS".format(table_name))
    except (Exception, sqlalchemy.exc.DataError) as error:
        print("\nDB Load status : FAILURE".format(table_name), error)
    connection.close()

    return


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
    return pd.concat(
        [

            pd.merge(
                (json_normalize(json, ["list"], meta=[["city", "id"]]).fillna("")).astype(str),
                (json_normalize(json["list"]).fillna("")).astype(str),
            ),
            (
                json_normalize(
                    json["list"], "weather", record_prefix="weather_"
                )
            ),
        ],
        axis=1,
    ).rename(columns=lambda x: x.replace('.', '_')).assign(source_record_created=pd.Timestamp.now()).fillna("")


def main():
    if len(sys.argv) != 2:
        exit("Please execute like : {} CITY_NAME".format(sys.argv[0]))

    pg_insert(
        connect(input_data['db']).connect(),
        create_df(get_data(input_data['api'].format(sys.argv[1], input_data['api_key']))), input_data['weather_table']
    )


if __name__ == "__main__":
    main()
