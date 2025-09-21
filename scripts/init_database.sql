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
----------------------------------------------------------------Silver Layer Starts-----------------------------------------			
------ silver date table creation scripts with Metedata column 
if object_id ('silver.crm_cust_info', 'U') IS NOT NULL 
	DROP TABLE silver.crm_cust_info;
Create table silver.crm_cust_info (
	cst_id int,
	cst_key nvarchar(50),
	cst_firstname nvarchar(50),
	cst_lastname nvarchar(50),
	cst_material_status nvarchar(50),
	cst_gndr nvarchar(50),
	cst_create_date date,
	dwh_create_date datetime2 default getdate()	
);
if object_id ('silver.crm_prd_info', 'U') IS NOT NULL 
	DROP TABLE silver.crm_prd_info;
create table silver.crm_prd_info (
	prd_id int,
	prd_key nvarchar(50),
	prd_nm nvarchar(50),
	prd_cost int,
	prd_line nvarchar(50),
	prd_start_dt datetime,
	prd_end_dt datetime,
	dwh_create_date datetime2 default getdate()	
);
if object_id ('silver.crm_sales_details', 'U') IS NOT NULL 
	DROP TABLE silver.crm_sales_details;
create table silver.crm_sales_details (
	sls_ord_num nvarchar(50),
	sls_prd_key nvarchar(50),
	sls_cust_id int,
	sls_order_dt int,
	sls_ship_dt int,
	sls_due_dt int,
	sls_sales int,
	sls_quantity int,
	sls_price int,
	dwh_create_date datetime2 default getdate()	

);
if object_id ('silver.erp_loc_a101', 'U') IS NOT NULL 
	DROP TABLE silver.erp_loc_a101;
create table silver.erp_loc_a101 (
	cid nvarchar (50),
	cntry nvarchar (50),
	dwh_create_date datetime2 default getdate()	
);
if object_id ('silver.erp_cust_az12', 'U') IS NOT NULL 
	DROP TABLE silver.erp_cust_az12;
create table silver.erp_cust_az12 (
	cid nvarchar (50),
	bdate date,
	gen nvarchar (50),
	dwh_create_date datetime2 default getdate()	
);
if object_id ('silver.erp_px_cat_g1v2', 'U') IS NOT NULL 
	DROP TABLE silver.erp_px_cat_g1v2;
create table silver.erp_px_cat_g1v2 (
	id nvarchar (50),
	cat nvarchar (50),
	subcat nvarchar (50),
	maintenance nvarchar(50),
	dwh_create_date datetime2 default getdate()	
);


------ Silver load script 1 - Data Cleaning, duplicates and loading to silver layer Script.
insert into silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_material_status,
	cst_gndr,
	cst_create_date
)
select

	cst_id,
	cst_key,
	trim(cst_firstname) as cst_firstname,
	trim(cst_lastname) as cst_lastname,
	case when upper(trim(cst_material_status)) = 'M' then 'Married'
		 when upper(trim(cst_material_status)) = 'S' then 'Single'
		 else 'n/a'
	End cst_material_status,
	case when upper(trim(cst_gndr)) = 'F' then 'Female'
		 when upper(trim(cst_gndr)) = 'M' then 'Male'
		 else 'n/a'
	End cst_gndr,
	cst_create_date

from (
	select 
	*,
	row_number() over (partition by cst_id order by cst_create_date DESC) as flag_last
	from bronze.crm_cust_info
	--where cst_id = 29466
	) t where flag_last = 1

------ Silver load script 2 - Data Cleaning, duplicates and loading to silver layer Script.

insert into silver.crm_prd_info (
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)
Select 
	prd_id,
	replace(SUBSTRING(prd_key,1, 5),'-','_') as cat_id,
	SUBSTRING(prd_key,7, len(prd_key)) as prd_key,
	--prd_key,
	prd_nm,
	isnull(prd_cost,0) as prd_cost,
	--prd_line,
	case upper(trim(prd_line))
	       when 'M' then 'Mountain'
		   when 'R' then 'Road'
		   when 'S' then 'Other Sales'
		   when 'T' then 'Touring'
	--case when upper(trim(prd_line)) = 'M' then 'Mountain'
		 --when upper(trim(prd_line)) = 'R' then 'Road'
		 --when upper(trim(prd_line)) = 'S' then 'Other Sales'
		 --when upper(trim(prd_line)) = 'T' then 'Touring'
		 Else 'n/a'
	End as prd_line,
	cast(prd_start_dt as date) as prd_start_dt,
	cast(lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)-1 as date) as prd_end_dt
	--prd_start_dt,
	--prd_end_dt
from bronze.crm_prd_info
--where SUBSTRING(prd_key,7, len(prd_key)) in (select sls_prd_key from bronze.crm_sales_details)
--where replace(SUBSTRING(prd_key,1, 5),'-','_') not in	
--(select distinct id from bronze.erp_px_cat_g1v2)
--select sls_prd_key from bronze.crm_sales_details


------ Silver load script 3 - Data Cleaning, duplicates and loading to silver layer Script.
insert into silver.crm_sales_details (
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
Select 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
         else cast(cast(sls_order_dt as varchar) as date)
    end as sls_order_dt,
    case when  sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
         else cast(cast(sls_ship_dt as varchar) as date)
    end as  sls_ship_dt,
    case when  sls_due_dt = 0 or len(sls_due_dt) != 8 then null
         else cast(cast(sls_due_dt as varchar) as date)
    end as  sls_due_dt,
    case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
	     then sls_quantity * abs(sls_price)
        else sls_sales
    end as sls_sales,
    sls_quantity,
    case when sls_price is null or sls_price <= 0 
	    then sls_sales / nullif(sls_quantity,0)
        else sls_price
    end as sls_price
from bronze.crm_sales_details
--where sls_ord_num != trim(sls_ord_num)
--where sls_cust_id not in (select cst_id from silver.crm_cust_info)
--select * from silver.crm_sales_details
