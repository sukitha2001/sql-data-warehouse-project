CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    v_row_count INT;
BEGIN
    RAISE NOTICE 'Starting load of Bronze layer...';

    --Recreate Tables
    DROP TABLE IF EXISTS bronze.crm_cust_info;
    CREATE TABLE bronze.crm_cust_info (
        cst_id INT,
        est_key VARCHAR(50),
        cst_firstname VARCHAR(50),
        cst_lastname VARCHAR(50),
        cst_material_status VARCHAR(50), 
        cst_gndr VARCHAR(50),
        est_create_date DATE
    );

    DROP TABLE IF EXISTS bronze.crm_prd_info;
    CREATE TABLE bronze.crm_prd_info (
        prd_id INT,
        pra_key VARCHAR(50),
        prd_nm VARCHAR(50),
        prd_cost INT,
        prd_line VARCHAR(50),
        prd_start_dt TIMESTAMP,
        prd_end_dt TIMESTAMP
    );

    DROP TABLE IF EXISTS bronze.crm_sales_details;
    CREATE TABLE bronze.crm_sales_details (
        sls_ord_num VARCHAR(50),
        s1s_prd_key VARCHAR(50),
        sls_cust_id INT,
        sls_order_dt INT,
        sls_ship_dt INT,
        s1s_due_dt INT,
        sls_sales INT, 
        sls_quantity INT, 
        sls_price INT
    );

    DROP TABLE IF EXISTS bronze.erp_1oc_a101;
    CREATE TABLE bronze.erp_1oc_a101 (
        cid VARCHAR(50),
        cntry VARCHAR(50)
    );

    DROP TABLE IF EXISTS bronze.erp_cust_az12;
    CREATE TABLE bronze.erp_cust_az12 ( 
        cid VARCHAR(50),
        bdate DATE, 
        gen VARCHAR(50)
    );

    DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
    CREATE TABLE bronze.erp_px_cat_g1v2 (
        id VARCHAR(50),
        cat VARCHAR(50),
        subcat VARCHAR(50),
        maintenance VARCHAR(50)
    );

    RAISE NOTICE 'Tables recreated successfully. Starting bulk loads...';

    -- 2. Load Data

    TRUNCATE TABLE bronze.crm_cust_info;
    COPY bronze.crm_cust_info
    FROM '/Users/sukitharathnayake/CodeRepo/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
    WITH (FORMAT CSV, HEADER);
    
   
    SELECT COUNT(*) INTO v_row_count FROM bronze.crm_cust_info;
    RAISE NOTICE 'Loaded % rows into bronze.crm_cust_info', v_row_count;


    TRUNCATE TABLE bronze.crm_prd_info;
    COPY bronze.crm_prd_info
    FROM '/Users/sukitharathnayake/CodeRepo/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
    WITH (FORMAT CSV, HEADER);

    SELECT COUNT(*) INTO v_row_count FROM bronze.crm_prd_info;
    RAISE NOTICE 'Loaded % rows into bronze.crm_prd_info', v_row_count;


    TRUNCATE TABLE bronze.crm_sales_details;
    COPY bronze.crm_sales_details
    FROM '/Users/sukitharathnayake/CodeRepo/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
    WITH (FORMAT CSV, HEADER);

    SELECT COUNT(*) INTO v_row_count FROM bronze.crm_sales_details;
    RAISE NOTICE 'Loaded % rows into bronze.crm_sales_details', v_row_count;


    TRUNCATE TABLE bronze.erp_1oc_a101;
    COPY bronze.erp_1oc_a101
    FROM '/Users/sukitharathnayake/CodeRepo/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
    WITH (FORMAT CSV, HEADER);

    SELECT COUNT(*) INTO v_row_count FROM bronze.erp_1oc_a101;
    RAISE NOTICE 'Loaded % rows into bronze.erp_1oc_a101', v_row_count;


    TRUNCATE TABLE bronze.erp_cust_az12;
    COPY bronze.erp_cust_az12
    FROM '/Users/sukitharathnayake/CodeRepo/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
    WITH (FORMAT CSV, HEADER);

    SELECT COUNT(*) INTO v_row_count FROM bronze.erp_cust_az12;
    RAISE NOTICE 'Loaded % rows into bronze.erp_cust_az12', v_row_count;


    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    COPY bronze.erp_px_cat_g1v2
    FROM '/Users/sukitharathnayake/CodeRepo/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
    WITH (FORMAT CSV, HEADER);  

    SELECT COUNT(*) INTO v_row_count FROM bronze.erp_px_cat_g1v2;
    RAISE NOTICE 'Loaded % rows into bronze.erp_px_cat_g1v2', v_row_count;

    RAISE NOTICE 'Bronze layer load complete!';

END;
$$;