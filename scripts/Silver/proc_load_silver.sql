/*
================================================================================
Stored Procedure:  
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema.
  Action Performed:
    - Truncated Silver Tables.
    - Inserts transformed and cleaned data from Bronze into Silver tables

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values

Usage Example:
    EXEC silver.load_silver; 
================================================================================
*/
-- check for nulls & duplicates in PK
-- check for unwanted space
-- data standardization & consistency
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time Datetime, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT'=======================================';
		PRINT'Loading Silver layer';
		PRINT'=======================================';

		PRINT'---------------------------------------';
		PRINT'Loading CRM Tables';
		PRINT'---------------------------------------';

		--Loading silver.crm_cust_info
		SET @start_time = GETDATE()
		PRINT ('>> Truncating Table: silver.crm_cust_info')
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Data Into: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
			cst_id, 
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)
		select
			cst_id, 
			cst_key,
			TRIM(cst_firstname) as cst_firstname,
			TRIM(cst_lastname) as cst_lastname,
			CASE WHEN upper(trim(cst_marital_status)) = 'M' THEN 'Married'
				 WHEN upper(trim(cst_marital_status)) = 'S' THEN 'Single'
				 ELSE 'N/A' 
			END cst_marital_status,
			CASE WHEN upper(trim(cst_gndr)) = 'F' THEN 'Female'
				 WHEN upper(trim(cst_gndr)) = 'M' THEN 'Male'
				 ELSE 'N/A' 
			END cst_gndr,
			cst_create_date

		FROM (select 
				*,
				ROW_NUMBER() over(partition by cst_id order by cst_create_date desc) flag_last
				FROM bronze.crm_cust_info 
				where cst_id is not null)t
		WHERE flag_last = 1;
		SET @end_time = GETDATE()
		PRINT'>> Load Duration: '+cast(DATEDIFF(SECOND, @start_time,@end_time) AS nvarchar) + ' seconds';
		PRINT'--------------------------------------------------------------'

		-- Loading silver.crm_prd_info
		SET @start_time = GETDATE()
		PRINT ('>> Truncating Table: silver.crm_prd_info')
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data Into: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_name,
			prd_cost,
			prd_line, 
			prd_start_dt,
			prd_end_dt

		) 
		select 
			prd_id, 
			replace(substring(prd_key, 1,5),'-','_') as cat_id,
			substring(prd_key, 7, len(prd_key)) as prd_key,
			prd_name, 
			ISNULL(prd_cost,0) as prd_cost, 
			CASE WHEN upper(TRIM(prd_line)) = 'M' THEN 'Mountain'
				WHEN upper(TRIM(prd_line)) = 'R' THEN 'Road'
				WHEN upper(TRIM(prd_line)) = 'S' THEN 'Other Sales'
				WHEN upper(TRIM(prd_line)) = 'T' THEN 'Touring'
				else 'N?A'
			END as prd_line, 
			CAST(prd_start_dt as date) prd_start_dt, 
			CAST(LEAD(prd_start_dt) over(partition by prd_key order by prd_start_dt)-1 as date)  prd_end_dt
		from bronze.crm_prd_info;

		-- Loading silver.crm_sales_details
		SET @start_time = GETDATE();
		PRINT ('>> Truncating Table: silver.crm_sales_details');
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data Into: silver.crm_sales_details';
		Insert into silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt, 
			sls_due_dt, 
			sls_sales,
			sls_quantity,
			sls_price
		)

		select 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			case when sls_order_dt = 0 OR len(sls_order_dt) !=8 then null
				 else cast(cast(sls_order_dt as varchar) as date)
			end sls_order_dt,
			case when sls_ship_dt = 0 OR len(sls_ship_dt) !=8 then null
				 else cast(cast(sls_ship_dt as varchar) as date)
			end sls_ship_dt, 
			case when sls_due_dt = 0 OR len(sls_due_dt) !=8 then null
				 else cast(cast(sls_due_dt as varchar) as date)
			end sls_due_dt, 
			Case when sls_sales is null or sls_sales <-0 or sls_sales != sls_quantity * abs(sls_price)
				 then sls_quantity * abs(sls_price)
				 else sls_sales
			end sls_sales,
			sls_quantity,
			case when sls_price is null or sls_price <= 0
				 then sls_sales/nullif(sls_quantity,0)
				 else sls_price
			end sls_price
		from bronze.crm_sales_details
		SET @end_time = GETDATE()
		PRINT'>> Load Duration: '+cast(DATEDIFF(SECOND, @start_time,@end_time) AS nvarchar) + ' seconds';
		PRINT'--------------------------------------------------------------'

		-- Loading silver.erp_cust_az12
		SET @start_time = GETDATE();
		PRINT ('>> Truncating Table: silver.erp_cust_az12')
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting Data Into: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12 (cid,bdate,gen)
		SELECT
			CASE WHEN cid LIKE 'NAS%' THEN substring(cid,4,len(cid))
				 ELSE cid
			END cid,
			CASE WHEN bdate > GETDATE() THEN NULL
				 ELSE bdate
			END bdate,
			CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
				 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				 ELSE 'N/A'
			END gen
		from bronze.erp_cust_az12
		SET @end_time = GETDATE()
		PRINT'>> Load Duration: '+cast(DATEDIFF(SECOND, @start_time,@end_time) AS nvarchar) + ' seconds';
		PRINT'--------------------------------------------------------------'

		-- Loading silver.erp_loc_a101
		SET @start_time = GETDATE();
		PRINT ('>> Truncating Table: silver.erp_loc_a101')
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data Into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101 (cid,cntry)
		SELECT 
			replace(cid,'-','') cid, 
			CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
				 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a' 
				 ELSE TRIM(cntry)
			END cntry
		from bronze.erp_loc_a101;
		SET @end_time = GETDATE()
		PRINT'>> Load Duration: '+cast(DATEDIFF(SECOND, @start_time,@end_time) AS nvarchar) + ' seconds';
		PRINT'--------------------------------------------------------------'

		-- Loading silver.erp_px_cat_g1v2
		SET @start_time = GETDATE();
		PRINT ('>> Truncating Table: silver.erp_px_cat_g1v2')
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2
		(id, cat, subcat, maintenance)
		select 
			id,
			cat,
			subcat,
			maintenance
		from bronze.erp_px_cat_g1v2
		SET @end_time = GETDATE()
		PRINT'>> Load Duration: '+cast(DATEDIFF(SECOND, @start_time,@end_time) AS nvarchar) + ' seconds';
		PRINT'--------------------------------------------------------------'
	END TRY
	BEGIN CATCH
		PRINT '=======================================================';
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
		PRINT 'Error Message ' + ERROR_MESSAGE();
		PRINT 'Error Message ' + CAST(ERROR_MESSAGE() AS NVARCHAR);
		PRINT 'Error Message ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '=======================================================';
	END CATCH
END
