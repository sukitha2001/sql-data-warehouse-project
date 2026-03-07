/*
Stored Procedure: Load Silver Layer (Bronze -> Silver)
Script Purpose:
This stored procedure performs the ELT process to populate the 'silver' schema.
It performs the following actions:
- Truncates the silver tables before loading data.
- Applies data cleansing, formatting, and transformation logic.
- Inserts the cleaned data from 'bronze' tables into 'silver' tables.
Parameters:
None.
This stored procedure does not accept any parameters or return any values.
Usage Example:
CALL silver.load_silver();
*/

CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    v_row_count INT;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    table_start_time TIMESTAMP;
    table_end_time TIMESTAMP;
    etl_duration INTERVAL;
BEGIN
    -- Start overall timer
    start_time := clock_timestamp();
    
    RAISE NOTICE 'Starting Silver Layer load at: %', start_time;
    RAISE NOTICE 'Starting transformations and data loads...';

    -- ==========================================
    -- DATA LOADING & DURATION TRACKING
    -- ==========================================

    -- Table 1: crm_cust_info
    RAISE NOTICE 'Loading silver.crm_cust_info...';
    table_start_time := clock_timestamp();
    TRUNCATE TABLE silver.crm_cust_info;
    
    INSERT INTO silver.crm_cust_info(
        cst_id, cst_key, cst_firstname, cst_lastname, 
        cst_material_status, cst_gndr, cst_create_date
    )
    SELECT 
        cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname) AS cst_lastname,
        CASE WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
             WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
             ELSE 'n/a'
        END AS cst_material_status,
        CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
             WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
             ELSE 'n/a'
        END AS cst_gndr,
        cst_create_date
    FROM (
        SELECT *,
        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
        FROM bronze.crm_cust_info
    ) a
    WHERE flag_last = 1;

    SELECT COUNT(*) INTO v_row_count FROM silver.crm_cust_info;
    table_end_time := clock_timestamp();
    RAISE NOTICE 'Loaded % rows into silver.crm_cust_info in %', v_row_count, (table_end_time - table_start_time);


    -- Table 2: crm_prd_info
    RAISE NOTICE 'Loading silver.crm_prd_info...';
    table_start_time := clock_timestamp();
    TRUNCATE TABLE silver.crm_prd_info;

    INSERT INTO silver.crm_prd_info(
        prd_id, cat_id, prd_key, prd_nm, prd_cost, 
        prd_line, prd_start_dt, prd_end_dt
    )
    SELECT
        prd_id,
        REPLACE(SUBSTR(prd_key, 1, 5), '-', '_') AS cat_id,
        SUBSTR(prd_key, 7) AS prd_key,
        prd_nm,
        COALESCE(prd_cost, 0) AS prd_cost,
        CASE UPPER(TRIM(prd_line))
             WHEN 'M' THEN 'Mountain'
             WHEN 'R' THEN 'Road'
             WHEN 'S' THEN 'Other Sales'
             WHEN 'T' THEN 'Touring'
             ELSE 'n/a'
        END AS prd_line,
        CAST(prd_start_dt AS DATE) AS prd_start_dt,
        CAST((LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) - INTERVAL '1 day' AS DATE) AS prd_end_dt
    FROM bronze.crm_prd_info;

    SELECT COUNT(*) INTO v_row_count FROM silver.crm_prd_info;
    table_end_time := clock_timestamp();
    RAISE NOTICE 'Loaded % rows into silver.crm_prd_info in %', v_row_count, (table_end_time - table_start_time);


    -- Table 3: crm_sales_details
    RAISE NOTICE 'Loading silver.crm_sales_details...';
    table_start_time := clock_timestamp();
    TRUNCATE TABLE silver.crm_sales_details;

    INSERT INTO silver.crm_sales_details(
        sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, 
        sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price
    )
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
             ELSE TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD')
        END AS sls_order_dt,
        CASE WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
             ELSE TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD')
        END AS sls_ship_dt,
        CASE WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
             ELSE TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD')
        END AS sls_due_dt,
        CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
             THEN sls_quantity * ABS(sls_price)
             ELSE sls_sales
        END AS sls_sales,
        sls_quantity,
        CASE WHEN sls_price IS NULL OR sls_price <= 0 
             THEN ABS(sls_sales / NULLIF(sls_quantity, 0))
             ELSE sls_price
        END AS sls_price
    FROM bronze.crm_sales_details;

    SELECT COUNT(*) INTO v_row_count FROM silver.crm_sales_details;
    table_end_time := clock_timestamp();
    RAISE NOTICE 'Loaded % rows into silver.crm_sales_details in %', v_row_count, (table_end_time - table_start_time);


    -- Table 4: erp_1oc_a101
    RAISE NOTICE 'Loading silver.erp_1oc_a101...';
    table_start_time := clock_timestamp();
    TRUNCATE TABLE silver.erp_1oc_a101;

    INSERT INTO silver.erp_1oc_a101(cid, cntry)
    SELECT
        REPLACE(cid, '-', '') AS cid,
        CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
             WHEN TRIM(cntry) = 'US' OR TRIM(cntry) = 'USA' THEN 'United States'
             WHEN TRIM(cntry) = '' OR TRIM(cntry) IS NULL THEN 'n/a'
             ELSE TRIM(cntry) 
        END AS cntry
    FROM bronze.erp_1oc_a101;

    SELECT COUNT(*) INTO v_row_count FROM silver.erp_1oc_a101;
    table_end_time := clock_timestamp();
    RAISE NOTICE 'Loaded % rows into silver.erp_1oc_a101 in %', v_row_count, (table_end_time - table_start_time);


    -- Table 5: erp_cust_az12
    RAISE NOTICE 'Loading silver.erp_cust_az12...';
    table_start_time := clock_timestamp();
    TRUNCATE TABLE silver.erp_cust_az12;

    INSERT INTO silver.erp_cust_az12(cid, bdate, gen)
    SELECT
        CASE 
            WHEN cid LIKE 'NAS%' THEN SUBSTR(cid, 4)
            ELSE cid
        END AS cid,
        CASE 
            WHEN bdate > CURRENT_DATE THEN NULL
            ELSE bdate
        END AS bdate,
        CASE 
            WHEN UPPER(TRIM(gen)) = 'F' OR UPPER(TRIM(gen)) = 'FEMALE' THEN 'Female'
            WHEN UPPER(TRIM(gen)) = 'M' OR UPPER(TRIM(gen)) = 'MALE' THEN 'Male'
            ELSE 'n/a'
        END AS gen
    FROM bronze.erp_cust_az12;   

    SELECT COUNT(*) INTO v_row_count FROM silver.erp_cust_az12;
    table_end_time := clock_timestamp();
    RAISE NOTICE 'Loaded % rows into silver.erp_cust_az12 in %', v_row_count, (table_end_time - table_start_time);


    -- Table 6: erp_px_cat_g1v2
    RAISE NOTICE 'Loading silver.erp_px_cat_g1v2...';
    table_start_time := clock_timestamp();
    TRUNCATE TABLE silver.erp_px_cat_g1v2;

    INSERT INTO silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)
    SELECT
        id,
        cat,
        subcat,
        maintenance
    FROM bronze.erp_px_cat_g1v2;

    SELECT COUNT(*) INTO v_row_count FROM silver.erp_px_cat_g1v2;
    table_end_time := clock_timestamp();
    RAISE NOTICE 'Loaded % rows into silver.erp_px_cat_g1v2 in %', v_row_count, (table_end_time - table_start_time);

    -- ==========================================
    -- SUCCESSFUL COMPLETION
    -- ==========================================
    end_time := clock_timestamp();
    etl_duration := end_time - start_time;

    RAISE NOTICE 'Silver layer load complete!';
    RAISE NOTICE 'Total ETL Duration: %', etl_duration;

EXCEPTION
    WHEN OTHERS THEN
        end_time := clock_timestamp();
        etl_duration := end_time - start_time;

        RAISE NOTICE '==========================================';
        RAISE NOTICE 'ERROR OCCURRED DURING LOADING SILVER LAYER';
        RAISE NOTICE 'Error Message: %', SQLERRM;
        RAISE NOTICE 'Error Number (SQLSTATE): %', SQLSTATE;
        RAISE NOTICE 'Process failed after running for: %', etl_duration;
        RAISE NOTICE '==========================================';
END;
$$;

CALL silver.load_silver();