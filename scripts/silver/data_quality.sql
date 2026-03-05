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


-- checking for duplicates in the primary key column (prd_id) and ensuring there are no null values in that column. The expectation is that there should be no results returned, indicating that there are no duplicates or null values in the prd_id column.

SELECT
prd_id,
COUNT (*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL


SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);


SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;


SELECT DISTINCT prd_line
FROM silver.crm_prd_info;


SELECT * FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;


SELECT 
    prd_id, 
    prd_key, 
    prd_nm, 
    prd_start_dt, 
    prd_end_dt,
    (LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) - INTERVAL '1 day' AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');


SELECT * FROM silver.crm_prd_info;


SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity, 
sls_price
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info);


SELECT NULLIF (sls_order_dt, 0) AS sls_order_dt FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LENGTH(sls_order_dt::TEXT) != 8;


-- checking for invalid date formats in the sls_order_dt, sls_ship_dt, and sls_due_dt columns. The expectation is that there should be no results returned, indicating that all date values are in the correct format and can be successfully converted to date data types.

SELECT * FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt OR sls_ship_dt > sls_due_dt;


-- » Check Data Consistency: Between Sales, Quantity, and Price 
-- » sales = Quantity * Price
-- » Values must not be NULL, zero, or negative.

select distinct
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales <= 0
or sls_quantity <= 0
or sls_price <= 0
or sls_sales IS NULL
or sls_quantity IS NULL
or sls_price IS NULL
ORDER BY sls_sales, sls_quantity, sls_price;


