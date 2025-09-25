-- ##########################################################
-- ####### (1) Create a Database & Schema
-- ##########################################################

-- SET ROLE AND WAREHOUSE:
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

-- CREATE A NEW DATABASE:
CREATE OR REPLACE DATABASE CUSTOMER_DATA;
USE DATABASE CUSTOMER_DATA;

-- CREATE TWO NEW SCHEMAS:
CREATE OR REPLACE SCHEMA RAW_DATA;
CREATE OR REPLACE SCHEMA FLATTEN_DATA;

-- ##########################################################
-- ####### (2) Create Raw Table in the RAW_DATA schema
-- ##########################################################

-- Create Raw Table in the RAW_DATA schema:
USE SCHEMA CUSTOMER_DATA.RAW_DATA;

CREATE OR REPLACE TABLE CUSTOMER_RAW
    (JSON_DATA VARIANT);

SELECT * FROM CUSTOMER_RAW;

-- ############################################################
-- ####### (3) Create flattened table in the FLATTEN_DATA schema
-- ############################################################

-- Create flattened table in the FLATTEN_DATA schema:
USE SCHEMA CUSTOMER_DATA.FLATTEN_DATA;

CREATE OR REPLACE TABLE CUSTOMER_FLATTEN (
    CUSTOMERID INT,
    NAME STRING,
    EMAIL STRING,
    REGION STRING,
    COUNTRY STRING,
    PRODUCTNAME STRING,
    PRODUCTBRAND STRING,
    CATEGORY STRING,
    QUANTITY INT,
    PRICEPERUNIT FLOAT,
    TOTALSALES FLOAT,
    PURCHASEMODE STRING,
    MODEOFPAYMENT STRING,
    PURCHASEDATE DATE);

SELECT * FROM CUSTOMER_FLATTEN;

-- ##########################################################
-- ####### (4) Upload the JSON File to an Internal Stage
-- ##########################################################

-- Create an Internal Stage in the RAW_DATA schema
USE SCHEMA CUSTOMER_DATA.RAW_DATA;

CREATE OR REPLACE STAGE CUSTOMER_STAGE;

-- List the file in the internal stage before uploading the file
LIST @CUSTOMER_STAGE;

---------------------------------------------------------------------------------------
------- UPLOAD MANUALLY THE CUSTOMER JSON DATA INTO THE INTERNAL CUSTOMER_STAGE -------
---------------------------------------------------------------------------------------

-- List the file in the internal stage after uploading the file
LIST @CUSTOMER_STAGE;

-- #################################################################################
-- ####### (5) Load Data from the Stage into the Raw Table using COPY INTO command
-- #################################################################################

-- create the JSON file format:
USE SCHEMA CUSTOMER_DATA.RAW_DATA;

CREATE OR REPLACE FILE FORMAT JSON_FORMAT
TYPE = 'JSON';

-- load uploaded JSON file from the internal CUSTOMER_STAGE into table CUSTOMER_RAW:
SELECT * FROM CUSTOMER_RAW;

COPY INTO CUSTOMER_RAW 
FROM @CUSTOMER_STAGE
FILE_FORMAT = (FORMAT_NAME = JSON_FORMAT);

SELECT * FROM CUSTOMER_RAW;

-- #############################################################################
-- ####### (6) Perform JSON flattening and insert data into the flatten Table
-- #############################################################################

INSERT INTO CUSTOMER_DATA.FLATTEN_DATA.CUSTOMER_FLATTEN
SELECT
    CUSTOMERS.VALUE:customerid::INT AS CUSTOMERID,
    CUSTOMERS.VALUE:name::STRING AS NAME,
    CUSTOMERS.VALUE:email::STRING AS EMAIL,
    CUSTOMERS.VALUE:region::STRING AS REGION,
    CUSTOMERS.VALUE:country::STRING AS COUNTRY,
    CUSTOMERS.VALUE:productname::STRING AS PRODUCTNAME,
    CUSTOMERS.VALUE:productbrand::STRING AS PRODUCTBRAND,
    CUSTOMERS.VALUE:category::STRING AS CATEGORY,
    CUSTOMERS.VALUE:quantity::INT AS QUANTITY,
    CUSTOMERS.VALUE:priceperunit::FLOAT AS PRICEPERUNIT,
    CUSTOMERS.VALUE:totalsales::FLOAT AS TOTALSALES,
    CUSTOMERS.VALUE:purchasemode::STRING AS PURCHASEMODE,
    CUSTOMERS.VALUE:modeofpayment::STRING AS MODEOFPAYMENT,    
    CUSTOMERS.VALUE:purchasedate::DATE AS PURCHASEDATE
FROM CUSTOMER_DATA.RAW_DATA.CUSTOMER_RAW,
    LATERAL FLATTEN (INPUT => JSON_DATA) AS CUSTOMERS;

SELECT * FROM CUSTOMER_DATA.FLATTEN_DATA.CUSTOMER_FLATTEN;

-- #############################################################################
-- ####### (7) Data Analysis on the flattened data
-- #############################################################################

USE SCHEMA CUSTOMER_DATA.FLATTEN_DATA;

-----------------------------------------------------------------
-- 1. Calculate the total sales for each region.
-----------------------------------------------------------------
CREATE OR REPLACE VIEW VIEW_1 AS
SELECT 
    REGION,
    SUM(TOTALSALES) AS REGION_TOTAL_SALES
FROM CUSTOMER_FLATTEN
GROUP BY REGION;

SELECT * FROM VIEW_1;

-----------------------------------------------------------------
-- 2. Identify the region with the highest total sales.
-----------------------------------------------------------------
CREATE OR REPLACE VIEW VIEW_2 AS
SELECT 
    REGION,
    REGION_TOTAL_SALES AS HIGHEST_TOTAL_SALES
FROM VIEW_1
WHERE HIGHEST_TOTAL_SALES = (SELECT MAX(REGION_TOTAL_SALES) FROM VIEW_1);

SELECT * FROM VIEW_2;

-----------------------------------------------------------------
-- 3. Determine the total quantity sold for each product brand.
-----------------------------------------------------------------
CREATE OR REPLACE VIEW VIEW_3 AS
SELECT 
    PRODUCTBRAND,
    SUM(QUANTITY) AS TOTAL_QUANTITY
FROM CUSTOMER_FLATTEN
GROUP BY PRODUCTBRAND;

SELECT * FROM VIEW_3;

-----------------------------------------------------------------
-- 4. Find the product with the least quantity sold.
-----------------------------------------------------------------
CREATE OR REPLACE VIEW VIEW_4 AS
SELECT 
    PRODUCTBRAND,
    TOTAL_QUANTITY AS LEAST_TOTAL_QUANTITY
FROM VIEW_3
WHERE TOTAL_QUANTITY = (SELECT MIN(TOTAL_QUANTITY) FROM VIEW_3);

SELECT * FROM VIEW_4;

-----------------------------------------------------------------
-- 5. Identify the customer who made the highest purchase.
-----------------------------------------------------------------
CREATE OR REPLACE VIEW VIEW_5 AS
SELECT 
    CUSTOMERID,
    NAME,
    PRODUCTBRAND,
    TOTALSALES
FROM CUSTOMER_FLATTEN
ORDER BY TOTALSALES DESC
LIMIT 1;

SELECT * FROM VIEW_5;

-----------------------------------------------------------------
-- 6. Locate the product name and brand with the lowest unit price.
-----------------------------------------------------------------
CREATE OR REPLACE VIEW VIEW_6 AS
SELECT 
    PRODUCTNAME,
    PRODUCTBRAND,
    PRICEPERUNIT
FROM CUSTOMER_FLATTEN
WHERE PRICEPERUNIT = (SELECT MIN(PRICEPERUNIT) FROM CUSTOMER_FLATTEN);

SELECT * FROM VIEW_6;

-----------------------------------------------------------------
-- 7. List the top 5 best-selling products.
-----------------------------------------------------------------
CREATE OR REPLACE VIEW VIEW_7 AS
SELECT 
    PRODUCTNAME,
    PRODUCTBRAND,
    SUM(TOTALSALES) AS PRODUCT_SALE
FROM CUSTOMER_FLATTEN
GROUP BY PRODUCTNAME, PRODUCTBRAND
ORDER BY PRODUCT_SALE DESC
LIMIT 5;

SELECT * FROM VIEW_7;

-----------------------------------------------------------------
-- 8. Identify the 5 countries with the lowest sales.
-----------------------------------------------------------------
CREATE OR REPLACE VIEW VIEW_8 AS
SELECT 
    COUNTRY,
    SUM(TOTALSALES) AS COUNTRY_SALE
FROM CUSTOMER_FLATTEN
GROUP BY COUNTRY
ORDER BY COUNTRY_SALE ASC
LIMIT 5;

SELECT * FROM VIEW_8;
