----------------------------------Gold View 1 : Build dimension customer : Dimension is information but not transaction or date info -----------------------------------------------------------------
--Select cst_id, count(*) from (
	
	Create view gold.dim_customers AS
	Select 
		row_number() over (order by cst_id) as customer_key,	
		ci.cst_id as customer_id,
		ci.cst_key as customer_number,
		ci.cst_firstname as first_name,
		ci.cst_lastname as last_name,
		la.cntry as country,
		ci.cst_material_status as marital_status,
		case when ci.cst_gndr != 'n/a' then ci.cst_gndr -- CRM is the master table
			else coalesce(ca.gen,'n/a')
		end as gender,
		ca.bdate as birthdate,
		ci.cst_create_date as create_date	
	from silver.crm_cust_info as ci
	left join silver.erp_cust_az12 ca
	on  ci.cst_key = ca.cid
	left join silver.erp_loc_a101 la
	on  ci.cst_key = la.cid	
--)t group by cst_id
--having count(*) > 1
--select * from gold.dim_customers


------------------------- Gold View 2 : Build Dimension Products ----------------------------------View 

--Select prd_key, count(*) from (
create view gold.dim_products as 
Select
	ROW_NUMBER () over (order by pn.prd_start_dt, pn.prd_key) as product_key,
	pn.prd_id as product_id,
	pn.prd_key as product_number,
	pn.prd_nm as product_name,
	pn.cat_id as category_id,
	pc.cat as category ,
	pc.subcat as subcategory,
	pc.maintenance,
	pn.prd_cost as cost,
	pn.prd_line as product_line,
	pn.prd_start_dt as start_date
From silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id = pc.id
where prd_end_dt is null -- filter out all historical data
--)t group by prd_key
--having count(*) > 1


----------------------------Gold View 3 : Fact Table - Joining Dimensions Sarrogate Keys ------------------------

Create View gold.fact_sales as 
select 
	sd.sls_ord_num as order_number,
	pr.product_key,
	cu.customer_key,
	sd.sls_order_dt as order_date,
	sd.sls_ship_dt as shipping_date,
	sd.sls_due_dt as due_date,
	sd.sls_sales as sales_amount,
	sd.sls_quantity as quantity,
	sd.sls_price price
From silver.crm_sales_details sd
left join gold.dim_products pr
on sd.sls_prd_key = pr.product_number
left join gold.dim_customers cu
on sd.sls_cust_id = cu.customer_id
