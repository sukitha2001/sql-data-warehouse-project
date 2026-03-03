-- ==========================================
-- Procedure Script: Load Bronze Layer
-- ==========================================

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
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
    
    RAISE NOTICE 'Starting Bronze Layer load at: %', start_time;
    RAISE NOTICE 'Starting bulk loads...';

    -- ==========================================
    -- DATA LOADING & DURATION TRACKING
    -- ==========================================

    -- Table 1: crm_cust_info
    RAISE NOTICE 'Loading bronze.crm_cust_info...';
    table_start_time := clock_timestamp();
    TRUNCATE TABLE bronze.crm_cust_info;
    COPY bronze.crm_cust_info FROM '/Users/sukitharathnayake/CodeRepo/sql-data-warehouse-project/datasets/source_crm/cust_info.csv' WITH (FORMAT CSV, HEADER);
    SELECT COUNT(*) INTO v_row_count FROM bronze.crm_cust_info;
    table_end_time := clock_timestamp();
    RAISE NOTICE 'Loaded % rows into bronze.crm_cust_info in %', v_row_count, (table_end_time - table_start_time);

    -- Table 2: crm_prd_info
    RAISE NOTICE 'Loading bronze.crm_prd_info...';
    table_start_time := clock_timestamp();
    TRUNCATE TABLE bronze.crm_prd_info;
    COPY bronze.crm_prd_info FROM '/Users/sukitharathnayake/CodeRepo/sql-data-warehouse-project/datasets/source_crm/prd_info.csv' WITH (FORMAT CSV, HEADER);
    SELECT COUNT(*) INTO v_row_count FROM bronze.crm_prd_info;
    table_end_time := clock_timestamp();
    RAISE NOTICE 'Loaded % rows into bronze.crm_prd_info in %', v_row_count, (table_end_time - table_start_time);

    -- Table 3: crm_sales_details
    RAISE NOTICE 'Loading bronze.crm_sales_details...';
    table_start_time := clock_timestamp();
    TRUNCATE TABLE bronze.crm_sales_details;
    COPY bronze.crm_sales_details FROM '/Users/sukitharathnayake/CodeRepo/sql-data-warehouse-project/datasets/source_crm/sales_details.csv' WITH (FORMAT CSV, HEADER);
    SELECT COUNT(*) INTO v_row_count FROM bronze.crm_sales_details;
    table_end_time := clock_timestamp();
    RAISE NOTICE 'Loaded % rows into bronze.crm_sales_details in %', v_row_count, (table_end_time - table_start_time);

    -- Table 4: erp_1oc_a101
    RAISE NOTICE 'Loading bronze.erp_1oc_a101...';
    table_start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_1oc_a101;
    COPY bronze.erp_1oc_a101 FROM '/Users/sukitharathnayake/CodeRepo/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv' WITH (FORMAT CSV, HEADER);
    SELECT COUNT(*) INTO v_row_count FROM bronze.erp_1oc_a101;
    table_end_time := clock_timestamp();
    RAISE NOTICE 'Loaded % rows into bronze.erp_1oc_a101 in %', v_row_count, (table_end_time - table_start_time);

    -- Table 5: erp_cust_az12
    RAISE NOTICE 'Loading bronze.erp_cust_az12...';
    table_start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_cust_az12;
    COPY bronze.erp_cust_az12 FROM '/Users/sukitharathnayake/CodeRepo/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv' WITH (FORMAT CSV, HEADER);
    SELECT COUNT(*) INTO v_row_count FROM bronze.erp_cust_az12;
    table_end_time := clock_timestamp();
    RAISE NOTICE 'Loaded % rows into bronze.erp_cust_az12 in %', v_row_count, (table_end_time - table_start_time);

    -- Table 6: erp_px_cat_g1v2
    RAISE NOTICE 'Loading bronze.erp_px_cat_g1v2...';
    table_start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    COPY bronze.erp_px_cat_g1v2 FROM '/Users/sukitharathnayake/CodeRepo/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv' WITH (FORMAT CSV, HEADER);  
    SELECT COUNT(*) INTO v_row_count FROM bronze.erp_px_cat_g1v2;
    table_end_time := clock_timestamp();
    RAISE NOTICE 'Loaded % rows into bronze.erp_px_cat_g1v2 in %', v_row_count, (table_end_time - table_start_time);

    -- ==========================================
    -- SUCCESSFUL COMPLETION
    -- ==========================================
    end_time := clock_timestamp();
    etl_duration := end_time - start_time;

    RAISE NOTICE 'Bronze layer load complete!';
    RAISE NOTICE 'Total ETL Duration: %', etl_duration;

EXCEPTION
    WHEN OTHERS THEN
        end_time := clock_timestamp();
        etl_duration := end_time - start_time;

        RAISE NOTICE '==========================================';
        RAISE NOTICE 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
        RAISE NOTICE 'Error Message: %', SQLERRM;
        RAISE NOTICE 'Error Number (SQLSTATE): %', SQLSTATE;
        RAISE NOTICE 'Process failed after running for: %', etl_duration;
        RAISE NOTICE '==========================================';
END;
$$;

CALL bronze.load_bronze();