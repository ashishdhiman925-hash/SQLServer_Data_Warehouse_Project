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
