CREATE DATABASE ECOMMERCE_DB;
USE DATABASE ECOMMERCE_DB;
CREATE SCHEMA STAGE_EXTERNAL;

--suzdavame external stage
CREATE OR REPLACE STAGE ecommerce_stage
URL = 's3://fakecompanydata'
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);

CREATE OR REPLACE TABLE raw_orders(
    order_id INT,
    customer_id STRING,
    customer_name STRING,
    order_date DATE,
    product STRING,
    quantity INT,
    price DECIMAL(10,2),
    discount DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    payment_method STRING,
    shipping_address STRING,
    status STRING
);

CREATE OR REPLACE TABLE raw_staging (
    order_id STRING,
    customer_id STRING,
    customer_name STRING,
    order_date STRING,
    product STRING,
    quantity INT,
    price DECIMAL(10,2),
    discount DECIMAL(4,2),
    total_amount DECIMAL(10,2),
    payment_method STRING,
    shipping_address STRING,
    status STRING
);

COPY INTO raw_staging
FROM @ecommerce_stage/ecommerce_orders.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
ON_ERROR = 'CONTINUE';

CREATE OR REPLACE TABLE td_invalid_date_format AS 
SELECT * FROM raw_staging
WHERE TRY_CAST(order_date AS DATE) IS NULL;

INSERT INTO raw_orders
SELECT 
    order_id,
    customer_id,
    customer_name,
    TO_DATE(order_date, 'YYYY-MM-DD') AS order_date,
    product,
    quantity,
    price,
    discount,
    total_amount,
    payment_method,
    shipping_address,
    status
FROM raw_staging
WHERE TRY_CAST(order_date AS DATE) IS NOT NULL;

--smenqme datata na pravilna
UPDATE td_invalid_date_format
SET order_date = '2024-02-29'
WHERE order_date = '2024-02-30';

--vkarvame veche pravilnata data i rekordite i v raw_orders
INSERT INTO raw_orders
SELECT * FROM td_invalid_date_format
WHERE order_date IS NOT NULL;

--suzdavame tablicata za rekordite za reviewta po sushtata struktura kato raw_orders
CREATE OR REPLACE TABLE td_for_review AS 
SELECT * 
FROM raw_orders 
WHERE 1 = 0;

--vkarvame rekordite v tablicata
INSERT INTO td_for_review
SELECT * 
FROM raw_orders
WHERE status = 'Delivered' AND (shipping_address IS NULL OR TRIM(shipping_address) = '');

--suzdavame tablicata sus sushtata struktura kato raw_orders
CREATE OR REPLACE TABLE td_suspicious_records AS
SELECT *
FROM raw_orders
WHERE 1 =0;

--vkarvame rekordite v tablicata
INSERT INTO td_suspicious_records
SELECT *
FROM raw_orders
WHERE customer_id IS NULL OR TRIM(customer_id) = '';

--updeitvame raw_orders ako nqma payment method na unknown
UPDATE raw_orders
SET payment_method = 'Unknown'
WHERE payment_method IS NULL;

--suzdavane na tablica za nevalidna cena i kolichestvo
CREATE OR REPLACE TABLE td_invalid_price_quantity AS
SELECT *
FROM raw_orders
WHERE 1 = 0;

--vkarvame dannite v tablicata
INSERT INTO td_invalid_price_quantity
SELECT * 
FROM raw_orders
WHERE price <= 0 OR quantity <= 0;

--Updeitvame discount 
UPDATE raw_orders
SET discount = 
    CASE   
        WHEN discount < 0 THEN 0
        WHEN discount > 0.5 THEN 0.5
        ELSE discount
    END;


--Updeitvame cenata na vseki zapis ot raw_orders za da sme sigurni che sa pravilni izchisleniq
UPDATE raw_orders
SET total_amount = (quantity * price) * (1 - discount);

--updeitvame statusa na dostaveni poruchki bez adres na pending
UPDATE raw_orders
SET status = 'Pending'
WHERE shipping_address IS NULL AND status = 'Delivered';


--suzdavane na td_clean_records 
CREATE OR REPLACE TABLE td_clean_records AS
SELECT DISTINCT * FROM raw_orders
WHERE order_id NOT IN (SELECT order_id FROM td_for_review)
AND order_id NOT IN (SELECT order_id FROM td_invalid_price_quantity)
AND order_id NOT IN (SELECT order_id FROM td_suspicious_records);




