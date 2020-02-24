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


def get_factSales_last_id(db_engine):
    """Function to get last sales_key from fact table `factSales`"""

    query = "SELECT max(sales_key) AS last_id FROM factSales"
    tdf = pd.read_sql(query, db_engine)
    return tdf.iloc[0]['last_id']


def extract_table_payment(last_id, execution_date, db_engine):
    """Function to extract table `payment`"""

    if last_id == None:
        last_id = -1

    query = "SELECT * FROM payment WHERE payment_id > {} AND DATE(payment_date) <= '{}' LIMIT 100000".format(
        last_id, execution_date)
    return pd.read_sql(query, db_engine)


def lookup_dim_customer(payment_df, db_engine):
    """Function to lookup table `dimCustomer`"""

    unique_ids = list(payment_df.customer_id.unique())
    unique_ids = list(filter(None, unique_ids))

    query = "SELECT * FROM dimCustomer WHERE customer_id IN ({})".format(
        ','.join(map(str, unique_ids)))
    return pd.read_sql(query, db_engine)


def lookup_table_rental(payment_df, db_engine):
    """Function to lookup table `rental`"""

    payment_df = payment_df.dropna(how='any', subset=['rental_id'])
    unique_ids = list(payment_df.rental_id.unique())

    query = "SELECT * FROM rental WHERE rental_id IN ({})".format(
        ','.join(map(str, unique_ids)))
    return pd.read_sql(query, db_engine)


def lookup_table_inventory(rental_df, db_engine):
    """Function to lookup table `inventory`"""

    rental_df = rental_df.dropna(how='any', subset=['inventory_id'])
    unique_ids = list(rental_df.inventory_id.unique())

    query = "SELECT * FROM inventory WHERE inventory_id IN ({})".format(
        ','.join(map(str, unique_ids)))
    return pd.read_sql(query, db_engine)


def lookup_dim_movie(inventory_df, db_engine):
    """Function to lookup table `dimMovie`"""

    inventory_df = inventory_df.dropna(how='any', subset=['film_id'])
    unique_ids = list(inventory_df.film_id.unique())

    query = "SELECT * FROM dimMovie WHERE film_id IN ({})".format(
        ','.join(map(str, unique_ids)))
    return pd.read_sql(query, db_engine)


def lookup_dim_store(inventory_df, db_engine):
    """Function to lookup table `dimStore`"""

    inventory_df = inventory_df.dropna(how='any', subset=['store_id'])
    unique_ids = list(inventory_df.store_id.unique())

    query = "SELECT * FROM dimStore WHERE store_id IN ({})".format(
        ','.join(map(str, unique_ids)))
    return pd.read_sql(query, db_engine)


def join_payment_dimCustomer(payment_df, dimCustomer_df):
    """Transformation: join table `payment` and `dim_dimCustomer`"""

    payment_df = pd.merge(payment_df, dimCustomer_df, left_on='customer_id',
                          right_on='customer_id', how='left')
    payment_df = payment_df[['payment_id', 'customer_key', 'customer_id',
                             'rental_id', 'amount', 'payment_date',
                             'start_date', 'end_date']]
    payment_df = payment_df.rename({'start_date': 'customer_start_date', 'end_date': 'customer_end_date'}, axis=1)
    # make sure we only join with customer record that is active at the time of transaction_date
    payment_df = payment_df[(pd.to_datetime(payment_df.customer_start_date) <= payment_df.payment_date)
                            & ((pd.to_datetime(payment_df.customer_end_date) >= payment_df.payment_date) | (payment_df.customer_end_date.isnull()))]
    return payment_df


def join_payment_rental(payment_df, rental_df):
    """Transformation: join table `payment` and `rental`"""

    payment_df = pd.merge(payment_df, rental_df, left_on='rental_id',
                          right_on='rental_id', how='left')
    payment_df = payment_df[['payment_id', 'customer_key',
                             'inventory_id', 'amount', 'payment_date',]]
    return payment_df


def join_payment_inventory(payment_df, inventory_df):
    """Transformation: join table `payment` and `inventory`"""

    payment_df = pd.merge(payment_df, inventory_df, left_on='inventory_id',
                          right_on='inventory_id', how='left')
    payment_df = payment_df[['payment_id', 'customer_key',
                             'film_id', 'store_id', 'amount', 'payment_date',]]
    return payment_df


def join_payment_dimMovie(payment_df, dimMovie_df):
    """Transformation: join table `payment` and `dimMovie`"""

    payment_df = pd.merge(payment_df, dimMovie_df, left_on='film_id',
                          right_on='film_id', how='left')
    payment_df = payment_df[['payment_id', 'customer_key',
                             'movie_key', 'store_id', 'amount', 'payment_date',]]
    return payment_df


def join_payment_dimStore(payment_df, dimStore_df):
    """Transformation: join table `payment` and `dimStore`"""
    
    payment_df = pd.merge(payment_df, dimStore_df, left_on='store_id',
                          right_on='store_id', how='left')
    payment_df = payment_df[['payment_id', 'customer_key',
                             'movie_key', 'store_key', 'amount', 'payment_date', 'start_date', 'end_date']]
    payment_df = payment_df.rename({'start_date': 'store_start_date', 'end_date': 'store_end_date'}, axis=1)
    # make sure we only join with store record that is active at the time of transaction_date
    payment_df = payment_df[(pd.to_datetime(payment_df.store_start_date) <= payment_df.payment_date)
                            & ((pd.to_datetime(payment_df.store_end_date) >= payment_df.payment_date) | (payment_df.store_end_date.isnull()))
                            | (payment_df.store_key.isnull())]

    return payment_df


def add_date_key(payment_df):
    """Add date_key smart key"""

    payment_df['date_key'] = payment_df.payment_date.dt.strftime('%Y%m%d')
    return payment_df


def rename_remove_columns(payment_df):
    """Rename and remove columns"""

    payment_df = payment_df.rename({'payment_id': 'sales_key', 'amount': 'sales_amount'}, axis=1)
    payment_df = payment_df[['sales_key', 'date_key', 'customer_key', 'movie_key', 'store_key', 'sales_amount']]
    return payment_df


def validate(source_df, destination_df):
    """Function to validate transformation result"""

    # make sure row count is equal between source and destination
    source_row_count = source_df.shape[0]
    destination_row_count = destination_df.shape[0]

    if(source_row_count != destination_row_count):
        raise ValueError(
            'Transformation result is not valid: row count is not equal (source={}; destination={})'.format(
                source_row_count, destination_row_count))

    # make sure there is no null value in all dimenstion key
    if destination_df['customer_key'].hasnans:
        raise ValueError(
            'Transformation result is not valid: column customer_key has NaN value')

    return destination_df


def load_dim_payment(destination_df):
    """Load to data warehouse"""

    destination_df.to_sql('factSales', dw_engine,
                          if_exists='append', index=False)


def run_job(**kwargs):

    execution_date = kwargs["execution_date"].date()
    logging.info("Execution datetime={}".format(execution_date))

    ############################################
    # EXTRACT
    ############################################

    # Get last payment_id from factSales data warehouse
    last_id = get_factSales_last_id(dw_engine)
    logging.info('last id={}'.format(last_id))

    # Extract the payment table into a pandas DataFrame
    payment_df = extract_table_payment(last_id, execution_date, db_engine)

    # If no records fetched, then exit
    if payment_df.shape[0] == 0:
        logging.info('No new record in source table')
    else:
        # Extract lookup table `dimCustomer`
        dimCustomer_df = lookup_dim_customer(payment_df, dw_engine)

        # Extract lookup table `rental`
        rental_df = lookup_table_rental(payment_df, db_engine)

        # Extract lookup table `inventory`
        inventory_df = lookup_table_inventory(rental_df, db_engine)

        # Extract lookup table `dimMovie`
        dimMovie_df = lookup_dim_movie(inventory_df, dw_engine)

        # Extract lookup table `dimStore`
        dimStore_df = lookup_dim_store(inventory_df, dw_engine)

        ############################################
        # TRANSFORM
        ############################################

        # Join table `payment` with `dimCustomer`
        dim_payment_df = join_payment_dimCustomer(payment_df, dimCustomer_df)
        logging.info('dim_payment_df=\n{}'.format(dim_payment_df))

        # Join table `payment` with `rental`
        dim_payment_df = join_payment_rental(dim_payment_df, rental_df)
        logging.info('dim_payment_df=\n{}'.format(dim_payment_df))

        # Join table `payment` with `inventory`
        dim_payment_df = join_payment_inventory(dim_payment_df, inventory_df)
        logging.info('dim_payment_df=\n{}'.format(dim_payment_df))

        # Join table `payment` with `dimMovie`
        dim_payment_df = join_payment_dimMovie(dim_payment_df, dimMovie_df)
        logging.info('dim_payment_df=\n{}'.format(dim_payment_df))

        # Join table `payment` with `dimStore`
        dim_payment_df = join_payment_dimStore(dim_payment_df, dimStore_df)
        logging.info('dim_payment_df=\n{}'.format(dim_payment_df))

        # Add date_key smart key
        dim_payment_df = add_date_key(dim_payment_df)
        logging.info('dim_payment_df=\n{}'.format(dim_payment_df))

        # Rename and remove columns
        dim_payment_df = rename_remove_columns(dim_payment_df)
        logging.info('dim_payment_df=\n{}'.format(dim_payment_df))

        # Validate result
        dim_payment_df = validate(payment_df, dim_payment_df)
        logging.info('dim_payment_df=\n{}'.format(dim_payment_df.dtypes))

        # ############################################
        # # LOAD
        # ############################################

        # Load dimension table `factSales`
        load_dim_payment(dim_payment_df)
