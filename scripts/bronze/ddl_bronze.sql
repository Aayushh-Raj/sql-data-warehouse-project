/*
======================================================================================================
DDL Script: Create Bronze Tables
======================================================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables if they already exists.
    Run this script to re-define the DDL structure of 'bronze' Tables
======================================================================================================
*/

IF OBJECT_ID ('bronze.crm_cust_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;
GO
  
Create Table bronze.crm_cust_info (
	cst_id INT,
	cst_key nvarchar(50),
	cst_firstname nvarchar(50),
	cst_lastname nvarchar(50),
	cst_marital_status nvarchar(50),
	cst_gndr nvarchar(50),
	cst_create_date DATE
);
GO
  
IF OBJECT_ID ('bronze.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;
GO
  
Create Table bronze.crm_prd_info (
	prd_id INT,
	prd_key nvarchar(50),
	prd_name nvarchar(50),
	prd_cost INT,
	prd_line nvarchar(50),
	prd_start_dt DATETIME,
	prd_end_dt DATETIME
);
GO
  
IF OBJECT_ID ('bronze.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;
GO
  
Create Table bronze.crm_sales_details (
	sls_ord_num nvarchar(50),
	sls_prd_key nvarchar(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);
GO
  
IF OBJECT_ID ('bronze.erp_loc_a101', 'U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101;
GO
  
Create Table bronze.erp_loc_a101 (
	cid nvarchar(50),
	cntry nvarchar(50)
	
);
GO

IF OBJECT_ID ('bronze.erp_cust_az12', 'U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12;
GO
  
Create Table bronze.erp_cust_az12 (
	cid nvarchar(50),
	bdate DATE,
	gen nvarchar(50)
	
);
GO

IF OBJECT_ID ('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2;
GO
  
Create Table bronze.erp_px_cat_g1v2 (
	id nvarchar(50),
	cat nvarchar(50),
	subcat nvarchar(50),
	maintenance nvarchar(50)
);
GO


