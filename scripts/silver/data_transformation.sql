--checking for duplicates in the primary key column (cst_id) and ensuring there are no null values in that column. The expectation is that there should be no results returned, indicating that there are no duplicates or null values in the cst_id column.

SELECT *
FROM(

SELECT *,
ROW_NUMBER () OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
) a
WHERE flag_last = 1;


--checking for white spaces in the cst_id column. The expectation is that there should be no results returned, indicating that there are no white spaces in the cst_id column.

SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

SELECT cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);

SELECT cst_material_status
FROM bronze.crm_cust_info
WHERE cst_material_status != TRIM(cst_material_status);


--Data Standardization & Consistency

SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;

INSERT INTO silver.crm_cust_info(
    cst_id, cst_key,
    cst_firstname,
    cst_lastname,
    cst_material_status,
    cst_gndr,
    cst_create_date)
SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
     WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
     ELSE 'n/a'
END cst_material_status,
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
     WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
     ELSE 'n/a'
END cst_gndr,
cst_create_date
FROM(
SELECT *,
ROW_NUMBER () OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
) a
WHERE flag_last = 1;




