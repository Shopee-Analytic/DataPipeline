DROP SCHEMA IF EXISTS t01 CASCADE;
CREATE SCHEMA IF NOT EXISTS t01;
SET search_path to t01;

CREATE TABLE IF NOT EXISTS shop (
    shop_id BIGINT,
    fetched_time FLOAT8,
    shop_location VARCHAR,
    shopee_verified BOOLEAN,
    is_official_shop BOOLEAN,
    PRIMARY KEY (shop_id, fetched_time)
);
CREATE TABLE IF NOT EXISTS product (
    product_id BIGINT,
    fetched_time FLOAT8,
    product_name TEXT,
    product_image TEXT,
    product_link TEXT,
    updated_at FLOAT8,
    shop_id BIGINT,
    PRIMARY KEY (product_id, fetched_time),
    FOREIGN KEY (shop_id, fetched_time) REFERENCES shop(shop_id, fetched_time) ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS product_brand (
    product_id BIGINT,
    fetched_time FLOAT8,
    product_brand TEXT,
    category_id NUMERIC,
    label_ids INTEGER ARRAY,
    PRIMARY KEY (product_id, fetched_time),
    FOREIGN KEY (product_id, fetched_time) REFERENCES product(product_id, fetched_time) ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS product_price (
    product_id BIGINT,
    fetched_time FLOAT8,
    product_price NUMERIC,
    product_discount NUMERIC,
    currency VARCHAR,
    is_freeship BOOLEAN,
    is_on_flash_sale BOOLEAN,
    PRIMARY KEY (product_id, fetched_time),
    FOREIGN KEY (product_id, fetched_time) REFERENCES product(product_id, fetched_time) ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS product_rating (
    product_id BIGINT,
    fetched_time FLOAT8,
    rating_star NUMERIC,
    rating_count INTEGER ARRAY,
    rating_with_context INTEGER,
    rating_with_image INTEGER,
    PRIMARY KEY(product_id, fetched_time),
    FOREIGN KEY (product_id, fetched_time) REFERENCES product(product_id, fetched_time) ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS product_feedback (
    product_id BIGINT,
    fetched_time FLOAT8,
    feedback_count INTEGER,
    liked_count INTEGER,
    view_count INTEGER,
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