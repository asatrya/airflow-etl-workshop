CREATE TABLE dimDate
(
    date_key integer NOT NULL,
    `date` date NOT NULL,
    `year` smallint NOT NULL,
    `quarter` tinyint NOT NULL,
    `month` tinyint NOT NULL,
    `day` tinyint NOT NULL,
    `week` tinyint NOT NULL,
    is_weekend boolean,
    is_holiday boolean,
    PRIMARY KEY(date_key)
);

CREATE TABLE dimCustomer
(
    customer_key int unsigned NOT NULL,
    first_name varchar(45) NOT NULL,
    last_name varchar(45) NOT NULL,
    email varchar(50),
    address varchar(50) NOT NULL,
    address2 varchar(50),
    district varchar(20) NOT NULL,
    city varchar(50) NOT NULL,
    country varchar(50) NOT NULL,
    postal_code varchar(10),
    phone varchar(20),
    active tinyint(1) NOT NULL,
    create_date datetime NOT NULL,
    PRIMARY KEY(customer_key)
);

CREATE TABLE dimMovie
(
    movie_key int unsigned NOT NULL,
    title varchar(255) NOT NULL,
    description text,
    release_year year(4),
    `language` varchar(20) NOT NULL,
    original_language varchar(20),
    rental_duration tinyint(3) unsigned NOT NULL,
    `length` smallint(5) unsigned NOT NULL,
    rating varchar(5) NOT NULL,
    special_features varchar(60) NOT NULL,
    PRIMARY KEY (movie_key)
);

CREATE TABLE dimStore
(
    store_key int unsigned NOT NULL,
    address varchar(50) NOT NULL,
    address2 varchar(50),
    district varchar(20) NOT NULL,
    city varchar(50) NOT NULL,
    country varchar(50) NOT NULL,
    postal_code varchar(10),
    manager_first_name varchar(45) NOT NULL,
    manager_last_name varchar(45) NOT NULL,
    PRIMARY KEY (store_key)
);

CREATE TABLE factSales
(
    sales_key INT unsigned NOT NULL,
    date_key INT NOT NULL,
    customer_key INT unsigned NOT NULL,
    movie_key INT unsigned,
    store_key INT unsigned,
    sales_amount decimal(5,2) NOT NULL,
    FOREIGN KEY fk_date (date_key) REFERENCES dimDate(date_key),
    FOREIGN KEY fk_customer (customer_key) REFERENCES dimCustomer(customer_key),
    FOREIGN KEY fk_movie (movie_key) REFERENCES dimMovie(movie_key),
    FOREIGN KEY fk_store (store_key) REFERENCES dimStore(store_key),
    PRIMARY KEY (sales_key)
);