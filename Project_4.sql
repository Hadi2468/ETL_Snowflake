-- ##########################################################
-- ##### (1) Create a Database & Schema
-- ##########################################################

-- SET ROLE AND WAREHOUSE:
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

-- CREATE DATABASE AND SCHEMA:
CREATE DATABASE TIMETRAVEL_DB;
CREATE SCHEMA TIMETRAVEL_DATA;

-- ###################################################################
-- ##### (2) Create EMPLOYEE Table in the TIMETRAVEL_DATA schema
-- ###################################################################

USE SCHEMA TIMETRAVEL_DB.TIMETRAVEL_DATA;

-- CREATE TABLE:
CREATE OR REPLACE TABLE TIMETRAVEL_DB.TIMETRAVEL_DATA.EMPLOYEE
    (EMPLOYEE_ID STRING,
    FIRST_NAME STRING,
    LAST_NAME STRING,
    DEPARTMENT STRING,
    SALARY FLOAT,
    HIRE_DATE DATE);

SELECT * FROM TIMETRAVEL_DB.TIMETRAVEL_DATA.EMPLOYEE;

-- ###################################################################
-- ##### (3) Populate the Table
-- ###################################################################

-- INSERT POULATED DATA
INSERT INTO TIMETRAVEL_DB.TIMETRAVEL_DATA.EMPLOYEE VALUES
    ('E1', 'John', 'Doe', 'Finance', 75000.50, '2020-01-15'),
    ('E2', 'Jane', 'Smith', 'HR', 68000.00, '2018-03-20'),
    ('E3', 'Alice', 'Johnson', 'IT', 92000.75, '2019-07-10'),
    ('E4', 'Bob', 'Williams', 'Sales', 58000.25, '2021-06-01'),
    ('E5', 'Charlie', 'Brown', 'Marketing', 72000.00, '2022-04-22'),
    ('E6', 'Emily', 'Davis', 'IT', 89000.10, '2017-11-12'),
    ('E7', 'Frank', 'Miller', 'Finance', 83000.30, '2016-09-05'),
    ('E8', 'Grace', 'Taylor', 'Sales', 61000.45, '2023-02-11'),
    ('E9', 'Hannah', 'Moore', 'HR', 67000.80, '2020-05-18'),
    ('E10', 'Jack', 'White', 'Marketing', 70000.90, '2019-12-25');

-- ###################################################################
-- ##### (4) View the Current Data in the EMPLOYEE table
-- ###################################################################

SELECT * FROM TIMETRAVEL_DB.TIMETRAVEL_DATA.EMPLOYEE;

-- ###################################################################
-- ##### (5) Simulate Data Deletion
-- ###################################################################

-- DELETE TWO RECORDS FROM THE TABLE:
DELETE FROM TIMETRAVEL_DB.TIMETRAVEL_DATA.EMPLOYEE
WHERE EMPLOYEE_ID IN ('E2', 'E7');

-- ###################################################################
-- ##### (6) Verify the Deletion
-- ###################################################################

-- MAKE SURE THAT TWO RECORDS ARE DELETED:
SELECT * FROM TIMETRAVEL_DB.TIMETRAVEL_DATA.EMPLOYEE;

-- ###################################################################
-- ##### (7) Fetch the QUERY_ID of the DELETE statement
-- ###################################################################

-- FETCH THE QUERY_ID FROM THE SNOWFLAKE MONITORING --> QUERY_HISTORY TABLE:
QUERY_ID: '01bf46a8-0000-34f9-005b-d88b0007c076';

-- ###################################################################
-- ##### (8) RECOVERING DELETED DATA USING TIME TRAVEL
-- ###################################################################

-- RECOVERING DELETED DATA USING TIME TRAVELL BEFORE FEATURE, BEFORE DELETION:

SELECT * FROM TIMETRAVEL_DB.TIMETRAVEL_DATA.EMPLOYEE BEFORE (STATEMENT => '01bf46a8-0000-34f9-005b-d88b0007c076');

-- ###################################################################
-- ##### (9) RECOVER THE DELETED RECORDS
-- ###################################################################

-- INSERT THE DELETED RECORDS BACK INTO THE TABLE:
INSERT INTO TIMETRAVEL_DB.TIMETRAVEL_DATA.EMPLOYEE 
SELECT S.* 
FROM TIMETRAVEL_DB.TIMETRAVEL_DATA.EMPLOYEE BEFORE (STATEMENT => '01bf46a8-0000-34f9-005b-d88b0007c076') S
LEFT JOIN TIMETRAVEL_DB.TIMETRAVEL_DATA.EMPLOYEE T
USING(EMPLOYEE_ID)
WHERE T.EMPLOYEE_ID IS NULL;

-- ###################################################################
-- ##### (10) Verify the Recovery
-- ###################################################################

SELECT * FROM TIMETRAVEL_DB.TIMETRAVEL_DATA.EMPLOYEE;
