DROP SCHEMA IF EXISTS v6 CASCADE;
CREATE SCHEMA IF NOT EXISTS v6;
SET search_path to v6;

CREATE TABLE IF NOT EXISTS shop (
    shop_id BIGINT,
    fetched_time FLOAT8,
    shop_location VARCHAR,
    shopee_verified BOOLEAN,
    PRIMARY KEY (shop_id, fetched_time)
);
CREATE TABLE IF NOT EXISTS product (
    product_id BIGINT,
    fetched_time FLOAT8,
    product_name TEXT,
    product_image TEXT,
    product_link TEXT,
    category_id INTEGER,
    updated_at FLOAT8,
    shop_id BIGINT,
    PRIMARY KEY (product_id, fetched_time),
    FOREIGN KEY (shop_id, fetched_time) REFERENCES shop(shop_id, fetched_time) ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS product_price (
    product_id BIGINT,
    fetched_time FLOAT8,
    product_price NUMERIC,
    product_discount NUMERIC,
    currency VARCHAR,
    PRIMARY KEY (product_id, fetched_time),
    FOREIGN KEY (product_id, fetched_time) REFERENCES product(product_id, fetched_time) ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS product_rating (
    product_id BIGINT,
    fetched_time FLOAT8,
    rating_star NUMERIC,
    rating_count_1 NUMERIC,
    rating_count_2 NUMERIC,
    rating_count_3 NUMERIC,
    rating_count_4 NUMERIC,
    rating_count_5 NUMERIC,
    PRIMARY KEY(product_id, fetched_time),
    FOREIGN KEY (product_id, fetched_time) REFERENCES product(product_id, fetched_time) ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS product_feedback (
    product_id BIGINT,
    fetched_time FLOAT8,
    feedback_count INTEGER,
    PRIMARY KEY (product_id, fetched_time),
    FOREIGN KEY (product_id, fetched_time) REFERENCES product(product_id, fetched_time) ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS product_quantity (
    product_id BIGINT,
    fetched_time FLOAT8,
    sold INTEGER,
    stock INTEGER,
    PRIMARY KEY (product_id, fetched_time),
    FOREIGN KEY (product_id, fetched_time) REFERENCES product(product_id, fetched_time) ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS product_time (
    product_id BIGINT,
    fetched_time FLOAT8,
    day SMALLINT,
    month SMALLINT,
    year SMALLINT,
    datetime TIMESTAMPTZ,
    PRIMARY KEY (product_id, fetched_time),
    FOREIGN KEY (product_id, fetched_time) REFERENCES product(product_id, fetched_time) ON UPDATE CASCADE ON DELETE CASCADE
)