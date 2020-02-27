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


def get_dimCustomer_last_id(db_engine):
    """Function to get last customer_key from dimemsion table `dimCustomer`"""

    query = "SELECT max(customer_key) AS last_id FROM dimCustomer"
    tdf = pd.read_sql(query, db_engine)
    return tdf.iloc[0]['last_id']


def extract_table_customer(last_id, db_engine):
    """Function to extract table `customer`"""

    if last_id == None:
        last_id = -1

    query = "SELECT * FROM customer WHERE customer_id > {} LIMIT 100000".format(
        last_id)
    logging.info(query)
    return pd.read_sql(query, db_engine)


def lookup_table_address(customer_df, db_engine):
    """Function to lookup table `address`"""

    unique_ids = list(customer_df.address_id.unique())
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


def join_customer_address(customer_df, address_df):
    """Transformation: join table `customer` and `address`"""

    customer_df = pd.merge(customer_df, address_df, left_on='address_id',
                           right_on='address_id', how='left')
    customer_df = customer_df[['customer_id', 'first_name', 'last_name',
                               'email', 'address', 'address2', 'district', 'city_id',
                               'postal_code', 'phone', 'active', 'create_date']]
    return customer_df


def join_customer_city(customer_df, city_df):
    """Transformation: join table `customer` and `city`"""

    customer_df = pd.merge(customer_df, city_df, left_on='city_id',
                           right_on='city_id', how='left')
    customer_df = customer_df[['customer_id', 'first_name', 'last_name',
                               'email', 'address', 'address2', 'district', 'city', 'country_id',
                               'postal_code', 'active', 'create_date']]
    return customer_df


def join_customer_country(customer_df, country_df):
    """Transformation: join table `customer` and `country`"""

    customer_df = pd.merge(customer_df, country_df, left_on='country_id',
                           right_on='country_id', how='left')
    customer_df = customer_df[['customer_id', 'first_name', 'last_name',
                               'email', 'address', 'address2', 'district', 'city', 'country',
                               'postal_code', 'active', 'create_date']]
    return customer_df


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

    destination_df = destination_df.rename({'customer_id': 'customer_key'}, axis=1)
    destination_df.to_sql('dimCustomer', dw_engine,
                          if_exists='append', index=False)


def run_job(**kwargs):

    execution_date = kwargs["execution_date"].date()
    logging.info("Execution datetime={}".format(execution_date))

    ############################################
    # EXTRACT
    ############################################

    # Get last customer_key from dimCustomer data warehouse
    last_id = get_dimCustomer_last_id(dw_engine)
    logging.info('last_id={}'.format(last_id))

    # Extract the store table into a pandas DataFrame
    customer_df = extract_table_customer(last_id, db_engine)

    # If no records fetched, then exit
    if customer_df.shape[0] == 0:
        logging.info('No new record in source table')
    else:
        # Extract lookup table `address`
        address_df = lookup_table_address(customer_df, db_engine)

        # Extract lookup table `city`
        city_df = lookup_table_city(address_df, db_engine)

        # Extract lookup table `country`
        country_df = lookup_table_country(city_df, db_engine)

        ############################################
        # TRANSFORM
        ############################################

        # Join table `customer` with `address`
        dim_customer_df = join_customer_address(customer_df, address_df)

        # Join table `customer` with `city`
        dim_customer_df = join_customer_city(dim_customer_df, city_df)

        # Join table `customer` with `country`
        dim_customer_df = join_customer_country(dim_customer_df, country_df)

        # Validate result
        dim_customer_df = validate(customer_df, dim_customer_df)
        logging.info('dim_customer_df=\n{}'.format(dim_customer_df.dtypes))

        ############################################
        # LOAD
        ############################################

        # Load dimension table `dimCustomer`
        load_dim_store(dim_customer_df)
