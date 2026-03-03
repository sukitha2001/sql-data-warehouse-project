CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
BEGIN

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

    TRUNCATE TABLE bronze.crm_cust_info;
    COPY bronze.crm_cust_info
    FROM '/Users/sukitharathnayake/CodeRepo/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
    WITH (
        FORMAT CSV,
        HEADER
    );

    SELECT * FROM bronze.crm_cust_info;

    SELECT COUNT(*) FROM bronze.crm_cust_info;


    TRUNCATE TABLE bronze.crm_prd_info;
    COPY bronze.crm_prd_info
    FROM '/Users/sukitharathnayake/CodeRepo/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
    WITH (
        FORMAT CSV,
        HEADER
    );

    SELECT * FROM bronze.crm_prd_info;

    SELECT COUNT(*) FROM bronze.crm_prd_info;


    TRUNCATE TABLE bronze.crm_sales_details;
    COPY bronze.crm_sales_details
    FROM '/Users/sukitharathnayake/CodeRepo/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
    WITH (
        FORMAT CSV,
        HEADER
    );

    SELECT * FROM bronze.crm_sales_details;

    SELECT COUNT(*) FROM bronze.crm_sales_details;

    TRUNCATE TABLE bronze.erp_1oc_a101;
    COPY bronze.erp_1oc_a101
    FROM '/Users/sukitharathnayake/CodeRepo/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
    WITH (
        FORMAT CSV,
        HEADER
    );

    SELECT * FROM bronze.erp_1oc_a101;

    SELECT COUNT(*) FROM bronze.erp_1oc_a101;


    TRUNCATE TABLE bronze.erp_cust_az12;
    COPY bronze.erp_cust_az12
    FROM '/Users/sukitharathnayake/CodeRepo/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
    WITH (
        FORMAT CSV,
        HEADER
    );

    SELECT * FROM bronze.erp_cust_az12;

    SELECT COUNT(*) FROM bronze.erp_cust_az12;

    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    COPY bronze.erp_px_cat_g1v2
    FROM '/Users/sukitharathnayake/CodeRepo/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
    WITH (
        FORMAT CSV,
        HEADER
    );  

    SELECT * FROM bronze.erp_px_cat_g1v2;
    SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2;

END;
$$;