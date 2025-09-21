---- This is Store Procedure that will run to load the ETL data from bronze layer to the silver layer , so this is the load
--exec silver.load_silver
create or alter procedure silver.load_silver as
begin
------ Silver load script 1 - Data Cleaning, duplicates and loading to silver layer Script.
	print '>> truncating table : silver.crm_cust_info';
	truncate table silver.crm_cust_info;
	print '>> inserting data into : silver.crm_cust_info';
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
	print '>> truncating table : silver.crm_prd_info';
	truncate table silver.crm_prd_info;
	print '>> inserting data into : silver.crm_prd_info';
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
	print '>> truncating table : silver.crm_sales_details';
	truncate table silver.crm_sales_details;
	print '>> inserting data into : crm_sales_details';
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

	------ Silver load script 1 : ERP - Data Cleaning, duplicates and loading to silver layer Script.
	print '>> truncating table : silver.erp_cust_az12';
	truncate table silver.erp_cust_az12;
	print '>> inserting data into : erp_cust_az12';
	insert into silver.erp_cust_az12 (cid, bdate, gen)
	select 
	case when cid like 'NAS%' then substring(cid,4,len(cid))
		else cid
	end cid,
	case when bdate > getdate() then null 
		 else bdate
	end as bdate,
		case when upper(trim(gen)) in ('F', 'FEMALE') then 'FEMALE'	
		when upper(trim(gen)) in ('M', 'MALE') then 'MALE'	
		else 'n/a'
	end as gen
	from bronze.erp_cust_az12

	---select * from silver.erp_cust_az12


	------ Silver load script 2 : ERP - Data Cleaning, duplicates and loading to silver layer Script.
	print '>> truncating table : silver.erp_loc_a101';
	truncate table silver.erp_loc_a101;
	print '>> inserting data into : erp_loc_a101';
	insert into silver.erp_loc_a101 
	(cid,cntry)
	select 
	replace(cid, '-', '') cid,
	case when trim(cntry) = 'DE' then 'Germany'
		 when trim(cntry) in ('US','USA') then 'United States'
		 when trim(cntry) = '' or cntry is null then 'n/a'
		 else trim(cntry)
	end as cntry
	from bronze.erp_loc_a101 --where replace(cid, '-', '') not in 
	--select * from bronze.erp_loc_a101
	--(select cst_key from silver.crm_cust_info)
	-- data standarization consistency 
	--select distinct cntry as old_cntry,


	------ Silver load script 3 : ERP - Data Cleaning, duplicates and loading to silver layer Script.
	print '>> truncating table : erp_px_cat_g1v2';
	truncate table silver.erp_px_cat_g1v2;
	print '>> inserting data into : erp_px_cat_g1v2';
	insert into silver.erp_px_cat_g1v2
	(id, cat, subcat, maintenance)
	select 
		id,
		cat,
		subcat,
		maintenance
	from bronze.erp_px_cat_g1v2
END
