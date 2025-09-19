--- This scripts check if we already have database datawarehouse or not, if not, then it will create it, and if yes then it will drop it to create one.
---- This scripts then create the schema under the datawarehouse table : schema is like a folder where you will manage everything. 

-- check first if database exists or not 
if exists (select 1 from sys.databases where name = 'Datawarehouse')
begin
  alter database datawarehouse set single_user with rollback immediate;
  drop database datawarehouse;
end;
GO
---Use master to get to main database creation in sql server

USE master;	
--- Create the database Datawarehouse

Create database Datawarehouse; 

Use datawarehouse;

Create schema bronze;
GO
Create schema silver;
GO
Create schema gold;
GO
-- Creating the tables for loading data 

if object_id ('bronze.crm_cust_info', 'U') IS NOT NULL 
	DROP TABLE bronze.crm_cust_info;
Create table bronze.crm_cust_info (
	cst_id int,
	cst_key nvarchar(50),
	cst_firstname nvarchar(50),
	cst_lastname nvarchar(50),
	cst_material_status nvarchar(50),
	cst_gndr nvarchar(50),
	cst_create_date date
);
if object_id ('bronze.crm_prd_info', 'U') IS NOT NULL 
	DROP TABLE bronze.crm_prd_info;
create table bronze.crm_prd_info (
	prd_id int,
	prd_key nvarchar(50),
	prd_nm nvarchar(50),
	prd_cost int,
	prd_line nvarchar(50),
	prd_start_dt datetime,
	prd_end_dt datetime
);
if object_id ('bronze.crm_sales_details', 'U') IS NOT NULL 
	DROP TABLE bronze.crm_sales_details;
create table bronze.crm_sales_details (
	sls_ord_num nvarchar(50),
	sls_prd_key nvarchar(50),
	sls_cust_id int,
	sls_order_dt int,
	sls_ship_dt int,
	sls_due_dt int,
	sls_sales int,
	sls_quantity int,
	sls_price int

);
if object_id ('bronze.erp_loc_a101', 'U') IS NOT NULL 
	DROP TABLE bronze.erp_loc_a101;
create table bronze.erp_loc_a101 (
	cid nvarchar (50),
	cntry nvarchar (50)
);
if object_id ('bronze.erp_cust_az12', 'U') IS NOT NULL 
	DROP TABLE bronze.erp_cust_az12;
create table bronze.erp_cust_az12 (
	cid nvarchar (50),
	bdate date,
	gen nvarchar (50)
);
if object_id ('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL 
	DROP TABLE bronze.erp_px_cat_g1v2;
create table bronze.erp_px_cat_g1v2 (
	id nvarchar (50),
	cat nvarchar (50),
	subcat nvarchar (50),
	maintenance nvarchar(50)
);
--- first bulk load of data into Store Prodecure , this SP name is bronze.load_brone, you can exe it by exec bronze.load_brone

Create or alter procedure bronze.load_bronze as
Begin
	declare @start_time datetime , @end_time datetime;
	Begin try
		print '===========================================';
		print 'loading bronze layer';
		print '===========================================';

		print '-------------------------------------------';
		print 'loading CRM Table';
		print '-------------------------------------------';
		set @start_time = getdate();
		print '>> Truncating table : bronze.crm_cust_info';
		truncate table bronze.crm_cust_info;
		print '>> Inserting datea into : bronze.crm_cust_info';
		bulk insert bronze.crm_cust_info
		from 'C:\Users\Deepika\Desktop\cust_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>> load duration : ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds '

		truncate table bronze.crm_prd_info;

		bulk insert bronze.crm_prd_info
		from 'C:\Users\Deepika\Desktop\prd_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);

		truncate table bronze.crm_sales_details;

		bulk insert bronze.crm_sales_details
		from 'C:\Users\Deepika\Desktop\sales_details.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		print '-------------------------------------------';
		print 'loading ERP Table';
		print '-------------------------------------------';
		--- load erp table

		truncate table bronze.erp_cust_az12;

		bulk insert bronze.erp_cust_az12
		from 'C:\Users\Deepika\Desktop\CUST_AZ12.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);

		truncate table bronze.erp_loc_a101;

		bulk insert bronze.erp_loc_a101
		from 'C:\Users\Deepika\Desktop\LOC_A101.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);

		truncate table bronze.erp_px_cat_g1v2;

		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\Deepika\Desktop\PX_CAT_G1V2.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
	END try
	begin catch 
		print '================================================='
		print 'Error Occured During Loading bronze layer'
		print 'Error Message' + Error_message();
		print 'Error Message' + cast(Error_number() as nvarchar);
		print 'Error Message' + cast(Error_state() as nvarchar);
		print '================================================='
	end catch 
END
