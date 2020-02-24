from airflow.models import Variable
import pandas as pd
import sqlalchemy as db
import configparser
import logging


# variables
SOURCE_MYSQL_HOST = Variable.get('SOURCE_MYSQL_HOST')
SOURCE_MYSQL_PORT = Variable.get('SOURCE_MYSQL_PORT')
SOURCE_MYSQL_USER = Variable.get('SOURCE_MYSQL_USER')
SOURCE_MYSQL_PASSWORD = Variable.get('SOURCE_MYSQL_PASSWORD')
SOURCE_MYSQL_ROOT_PASSWORD = Variable.get('SOURCE_MYSQL_ROOT_PASSWORD')
SOURCE_MYSQL_DATABASE = Variable.get('SOURCE_MYSQL_DATABASE')

DW_MYSQL_HOST = Variable.get('DW_MYSQL_HOST')
DW_MYSQL_PORT = Variable.get('DW_MYSQL_PORT')
DW_MYSQL_USER = Variable.get('DW_MYSQL_USER')
DW_MYSQL_PASSWORD = Variable.get('DW_MYSQL_PASSWORD')
DW_MYSQL_ROOT_PASSWORD = Variable.get('DW_MYSQL_ROOT_PASSWORD')
DW_MYSQL_DATABASE = Variable.get('DW_MYSQL_DATABASE')

# Database connection URI
db_conn_url = "mysql+pymysql://{}:{}@{}:{}/{}".format(SOURCE_MYSQL_USER,
                                                      SOURCE_MYSQL_PASSWORD,
                                                      SOURCE_MYSQL_HOST,
                                                      SOURCE_MYSQL_PORT,
                                                      SOURCE_MYSQL_DATABASE)
db_engine = db.create_engine(db_conn_url)

# Data warehouse connection URI
dw_conn_url = "mysql+pymysql://{}:{}@{}:{}/{}".format(DW_MYSQL_USER,
                                                      DW_MYSQL_PASSWORD,
                                                      DW_MYSQL_HOST,
                                                      DW_MYSQL_PORT,
                                                      DW_MYSQL_DATABASE)
dw_engine = db.create_engine(dw_conn_url)


def get_dimMovie_last_id(db_engine):
    """Function to get last film_id from dimemsion table `dimMovie`"""

    query = "SELECT max(film_id) AS last_film_id FROM dimMovie"
    tdf = pd.read_sql(query, db_engine)
    return tdf.iloc[0]['last_film_id']


def extract_table_film(last_id, db_engine):
    """Function to extract table `film`"""

    if last_id == None:
        last_id = -1

    query = "SELECT * FROM film WHERE film_id > {} LIMIT 100000".format(
        last_id)
    return pd.read_sql(query, db_engine)


def lookup_table_language(film_df, db_engine):
    """Function to lookup table `language`"""

    unique_ids = list(film_df.language_id.unique()) + \
        list(film_df.original_language_id.unique())
    unique_ids = list(filter(None, unique_ids))

    query = "SELECT * FROM language WHERE language_id IN ({})".format(
        ','.join(map(str, unique_ids)))
    return pd.read_sql(query, db_engine)


def join_film_language(film_df, language_df):
    """Transformation: join table `film` and `language`"""

    film_df = pd.merge(film_df, language_df, left_on='language_id',
                       right_on='language_id', how='left')
    film_df = film_df[['film_id', 'title', 'description', 'release_year',
                       'name', 'original_language_id', 'rental_duration',
                       'length', 'rating', 'special_features']]
    film_df = film_df.rename(
        {'name': 'language', 'original_language_id': 'original_language'}, axis=1)
    return film_df


def validate(source_df, destination_df):
    """Function to validate transformation result"""

    source_row_count = source_df.shape[0]
    destination_row_count = destination_df.shape[0]

    if(source_row_count == destination_row_count):
        return destination_df
    else:
        raise ValueError(
            'Transformation result is not valid: row count is not equal')


def load_dim_movie(destination_df):
    """Load to data warehouse"""

    destination_df.to_sql('dimMovie', dw_engine,
                          if_exists='append', index=False)


def run_job(**kwargs):

    execution_date = kwargs["execution_date"].date()
    logging.info("Execution datetime={}".format(execution_date))

    ############################################
    # EXTRACT
    ############################################

    # Get last film_id from dimMovie data warehouse
    last_id = get_dimMovie_last_id(dw_engine)
    logging.info('last id={}'.format(last_id))

    # Extract the film table into a pandas DataFrame
    film_df = extract_table_film(last_id, db_engine)

    # If no records fetched, then exit
    if film_df.shape[0] == 0:
        logging.info('No new record in source table')
    else:
        # Extract lookup table `language`
        language_df = lookup_table_language(film_df, db_engine)

        ############################################
        # TRANSFORM
        ############################################

        # Join table `film` with `language`
        dim_movie_df = join_film_language(film_df, language_df)

        # Validate result
        dim_movie_df = validate(film_df, dim_movie_df)
        logging.info(dim_movie_df)

        ############################################
        # LOAD
        ############################################

        # Load dimension table `dimMovie`
        load_dim_movie(dim_movie_df)
