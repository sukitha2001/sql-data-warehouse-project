--checking for duplicates in the primary key column (cst_id) and ensuring there are no null values in that column. The expectation is that there should be no results returned, indicating that there are no duplicates or null values in the cst_id column.

SELECT *
FROM(

SELECT *,
ROW_NUMBER () OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM silver.crm_cust_info
) a
WHERE flag_last = 1;


--checking for white spaces in the cst_id column. The expectation is that there should be no results returned, indicating that there are no white spaces in the cst_id column.

SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

SELECT cst_gndr
FROM silver.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);

SELECT cst_material_status
FROM silver.crm_cust_info
WHERE cst_material_status != TRIM(cst_material_status);