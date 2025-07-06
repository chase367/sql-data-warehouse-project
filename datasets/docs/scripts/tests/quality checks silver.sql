/*
========================================================================
Quality Checks
========================================================================

Script Purpose:
    This script performs various quality checks for data consistency, accuracy,
    and standardization across the 'silver' schemas. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
========================================================================
*/

-- ==================================================
-- Checking 'silver.crm_cust_info'
-- ==================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results

=========================================
-- SILVER . CRM_CUST_INFO
=========================================

MAIN QUERY
SELECT
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    trim (cst_lastname) as cst_lastname,
    
    case when upper(trim(cst_material_status)) = 'S' then 'Single'
    when upper(trim(cst_material_status)) = 'M' then 'Married'
    else 'N/A'
    end  cst_material_status,
  
  case when upper(trim(cst_gndr)) = 'F' then 'Female'
    when upper(trim(cst_gndr)) = 'M' then 'Male'
    else 'N/A'
    end cst_gndr,
    cst_create_date
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY cst_id 
            ORDER BY cst_create_date DESC
        ) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) t
WHERE flag_last = 1;

-- check for unwanted spaces
-- expectation: no results
select cst_firstname
from silver.crm_cust_info
where cst_firstname != trim(cst_firstname)

-- rename column
EXEC sp_rename 
  
 'bronze.crm_cust_info.cst_marital_status', 
 'cst_material_status', 
 'COLUMN';

-- data standardization and consistency
select distinct cst_gndr
from silver.crm_cust_info

-- check for duplicates
select
cst_id,
count (*)
from silver.crm_cust_info
group by cst_id
having count (*) > 1 or cst_id is null

=========================================
-- SILVER.CRM_PRD_INFO
========================================
MAIN QUERY
select
prd_id,
replace(substring(prd_key, 1, 5), '-', '_') as cat_id,
substring(prd_key, 7, len(prd_key)) as prd_key,
prd_nm,
isnull(prd_cost, 0) as prd_cost,
case upper(trim(prd_line))
when 'M' then 'Mountain'
 when 'R' then 'Road'
 when 'S' then 'Other Sales'
 when 'T' then 'Touring'
 else 'N/A'
 end as prd_line,
cast (prd_start_dt as date) as prd_start_dt,
cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as date) as prd_end_dt
from bronze.crm_prd_info

-- query for distinct

select distinct id from bronze.erp_px_cat_g1v2

select sls_prd_key from bronze.crm_sales_details

-- check primary key
select
prd_id,
count (*)
from silver.crm_prd_info
group by prd_id
having count (*) > 1 or prd_id is null

-- check for negative or nulls
select prd_cost 
from bronze.crm_prd_info
where prd_cost < 0 or prd_cost is null

-- check for invlaid date orders
select *
from bronze.crm_prd_info
where prd_end_dt < prd_start_dt

-- reconfiguring dates
select 
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as prd_end_dt_test
from bronze.crm_prd_info
where prd_key in ('AC-HE-HL-U509-R', 'AC-HE-HL-US09')

=========================================
--SILVER.CRM_SALES_DETAILS
=========================================

MAIN QUERY
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
   case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
    else cast (cast(sls_order_dt as varchar) as date)
    end as sls_order_dt,
  case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
    else cast (cast(sls_ship_dt as varchar) as date)
    end as sls_ship_dt,
   case when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
    else cast (cast(sls_due_dt as varchar) as date)
    end as sls_due_dt,
    case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * abs(sls_price)
then sls_quantity * abs(sls_price)
else sls_sales 
end as sls_sales,
    sls_quantity,
    case when sls_price is null or sls_price <= 0
then sls_sales / nullif(sls_quantity, 0)
else sls_price 
end as sls_price
FROM bronze.crm_sales_details

-- check for invalid dates
select 
nullif(sls_order_dt,0) sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <= 0
or len(sls_order_dt) != 8
or sls_order_dt > 20500101
or sls_order_dt < 19000101

-- check for invalid date orders
select *
from bronze.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt

-- check data consistenmcy between sales, Q and P
-- Sales = Q * P
-- Values must not be null, 0, or negative

select distinct
sls_sales as old_sls_sales,
sls_quantity,
sls_price as old_sls_price,
case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * abs(sls_price)
then sls_quantity * abs(sls_price)
else sls_sales 
end as sls_sales,

case when sls_price is null or sls_price <= 0
then sls_sales / nullif(sls_quantity, 0)
else sls_price 
end as sls_price
from bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0
order by sls_sales, sls_quantity, sls_price

=========================================
SILVER.ERP_CUST_AZ12
=========================================
MAIN QUERY
select
case when cid like 'NAS%' then substring(cid, 4, len(cid))
else cid
end cid,
case when bdate > getdate() then null
else bdate
end as bdate,
case when upper(trim(gen)) in ('F', 'FEMALE') THEN 'Female'
when upper(trim(gen)) in ('M', 'MALE') THEN 'Male'
ELSE 'N/A'
END AS gen
from bronze.erp_cust_az12

-- identify out of range dates
select distinct 
bdate
from bronze.erp_cust_az12
where bdate < '1924-01-01' or bdate > getdate()

-- data standardization and consistency
select distinct gen,
from bronze.erp_cust_az12

=========================================
SILVER.ERP_LOC_A101
=========================================
MAIN QUERY 
select 
replace(cid, '-', '') cid,
case when trim(cntry) = 'DE' then 'Germany'
when trim (cntry) in ('US', 'USA') then 'United States'
when trim('cntry') = '' or cntry is null then 'N/A'
else trim(cntry)
end as cntry
from bronze.erp_loc_a101;


-- data standard and consistency
select distinct cntry
from bronze.erp_loc_a101
order by cntry

=========================================
SILVER.ERP_PX_CAT_G1V2
=========================================
MAIN QUERY
select
id,
cat,
subcat,
maintenance
from bronze.erp_px_cat_g1v2

-- check for unwanted spaces
select * from bronze.erp_px_cat_g1v2
where cat != trim(cat)
or subcat != trim(subcat)
or maintenance != trim(maintenance)

-- data standardization and consistency
select distinct
maintenance 
from bronze.erp_px_cat_g1v2
