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


def get_dimStore_last_id(db_engine):
    """Function to get last store_id from dimemsion table `dimStore`"""

    query = "SELECT max(store_id) AS last_id FROM dimStore"
    tdf = pd.read_sql(query, db_engine)
    return tdf.iloc[0]['last_id']


def extract_table_store(last_id, db_engine):
    """Function to extract table `store`"""

    if last_id == None:
        last_id = -1

    query = "SELECT * FROM store WHERE store_id > {} LIMIT 100000".format(
        last_id)
    logging.info("query={}".format(query))
    return pd.read_sql(query, db_engine)


def lookup_table_address(store_df, db_engine):
    """Function to lookup table `address`"""

    unique_ids = list(store_df.address_id.unique())
    unique_ids = list(filter(None, unique_ids))

    query = "SELECT * FROM address WHERE address_id IN ({})".format(
        ','.join(map(str, unique_ids)))
    return pd.read_sql(query, db_engine)


def lookup_table_city(address_df, db_engine):
    """Function to lookup table `city`"""

    unique_ids = list(address_df.city_id.unique())
    unique_ids = list(filter(None, unique_ids))

    query = "SELECT * FROM city WHERE city_id IN ({})".format(
        ','.join(map(str, unique_ids)))
    return pd.read_sql(query, db_engine)


def lookup_table_country(address_df, db_engine):
    """Function to lookup table `country`"""

    unique_ids = list(address_df.country_id.unique())
    unique_ids = list(filter(None, unique_ids))

    query = "SELECT * FROM country WHERE country_id IN ({})".format(
        ','.join(map(str, unique_ids)))
    return pd.read_sql(query, db_engine)


def lookup_table_staff(store_df, db_engine):
    """Function to lookup table `staff`"""

    unique_ids = list(store_df.manager_staff_id.unique())
    unique_ids = list(filter(None, unique_ids))

    query = "SELECT * FROM staff WHERE staff_id IN ({})".format(
        ','.join(map(str, unique_ids)))
    return pd.read_sql(query, db_engine)


def join_store_address(store_df, address_df):
    """Transformation: join table `store` and `address`"""

    store_df = pd.merge(store_df, address_df, left_on='address_id',
                        right_on='address_id', how='left')
    store_df = store_df[['store_id', 'manager_staff_id', 'last_update_x',
                         'address', 'address2', 'district', 'city_id', 'postal_code']]
    return store_df


def join_store_city(store_df, city_df):
    """Transformation: join table `store` and `city`"""

    store_df = pd.merge(store_df, city_df, left_on='city_id',
                        right_on='city_id', how='left')
    store_df = store_df[['store_id', 'manager_staff_id', 'last_update',
                         'address', 'address2', 'district', 'city', 'country_id', 'postal_code']]
    store_df = store_df.rename({'last_update_x': 'last_update'}, axis=1)
    return store_df


def join_store_country(store_df, country_df):
    """Transformation: join table `store` and `country`"""

    store_df = pd.merge(store_df, country_df, left_on='country_id',
                        right_on='country_id', how='left')
    store_df = store_df[['store_id', 'manager_staff_id', 'last_update_x',
                         'address', 'address2', 'district', 'city', 'country', 'postal_code']]
    store_df = store_df.rename({'last_update_x': 'last_update'}, axis=1)
    return store_df


def join_store_manager_staff(store_df, manager_staff_df):
    """Transformation: join table `store` and `manager_staff`"""

    store_df = pd.merge(store_df, manager_staff_df, left_on='manager_staff_id',
                        right_on='staff_id', how='left')
    store_df = store_df[['store_id_x', 'address', 'address2', 'district', 'city', 'country', 'postal_code',
                         'first_name', 'last_name']]
    store_df = store_df.rename({'store_id_x': 'store_id',
                                'first_name': 'manager_first_name', 
                                'last_name': 'manager_last_name'}, axis=1)
    return store_df


def validate(source_df, destination_df):
    """Function to validate transformation result"""

    source_row_count = source_df.shape[0]
    destination_row_count = destination_df.shape[0]

    if(source_row_count == destination_row_count):
        return destination_df
    else:
        raise ValueError(
            'Transformation result is not valid: row count is not equal')


def load_dim_store(destination_df):
    """Load to data warehouse"""

    destination_df.to_sql('dimStore', dw_engine,
                          if_exists='append', index=False)


def run_job(**kwargs):

    execution_date = kwargs["execution_date"].date()
    logging.info("Execution datetime={}".format(execution_date))

    ############################################
    # EXTRACT
    ############################################

    # Get last store_id from dimStore data warehouse
    last_id = get_dimStore_last_id(dw_engine)
    logging.info('last id={}'.format(last_id))

    # Extract the store table into a pandas DataFrame
    store_df = extract_table_store(last_id, db_engine)

    # If no records fetched, then exit
    if store_df.shape[0] == 0:
        logging.info('No new record in source table')
        exit()

    # Extract lookup table `address`
    address_df = lookup_table_address(store_df, db_engine)

    # Extract lookup table `city`
    city_df = lookup_table_city(address_df, db_engine)

    # Extract lookup table `country`
    country_df = lookup_table_country(city_df, db_engine)

    # Extract lookup table `staff`
    staff_df = lookup_table_staff(store_df, db_engine)

    ############################################
    # TRANSFORM
    ############################################

    # Join table `store` with `address`
    dim_store_df = join_store_address(store_df, address_df)

    # Join table `store` with `city`
    dim_store_df = join_store_city(dim_store_df, city_df)

    # Join table `store` with `country`
    dim_store_df = join_store_country(dim_store_df, country_df)

    # Join table `store` with `manager_staff`
    dim_store_df = join_store_manager_staff(dim_store_df, staff_df)

    # Add start_date column
    dim_store_df['start_date'] = '1970-01-01'

    # Validate result
    dim_store_df = validate(store_df, dim_store_df)
    logging.info('dim_store_df=\n{}'.format(dim_store_df.dtypes))

    ############################################
    # LOAD
    ############################################

    # Load dimension table `dimStore`
    load_dim_store(dim_store_df)
