/*
===================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===================================================================

Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the BULK INSERT command to load data from CSV files to bronze tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;

===================================================================
*/

exec bronze.load_bronze

create or alter procedure bronze.load_bronze as 
begin
declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
set @batch_start_time = getdate();
begin try
print '=====================';
print 'Loading Bronze Layer';
print '=====================';

print '---------------------';
print 'Loading CRM Tables';
print '---------------------';

set @start_time = getdate();
print '>> Truncating Table: bronze.crm_cust_info';
	truncate table bronze.crm_cust_info;

	print '>> Inserting Data Into: bronze.crm_cust_info';
	BULK INSERT bronze.crm_cust_info
	FROM 'C:\Users\cj843\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_crm/cust_info.csv'
	WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	);
	set @end_time = getdate();
	print '>> Load Duration: ' +cast(datediff(second, @start_time, @end_time) as nvarchar) +' seconds';
	print '>>------------------';

	set @start_time = getdate();
	print '>> Truncating Table: bronze.crm_prd_info';
	truncate table bronze.crm_prd_info;

	print '>> Inserting Data Into: bronze.crm_prd_info';
	BULK INSERT bronze.crm_prd_info
	FROM 'C:\Users\cj843\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_crm/prd_info.csv'
	WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	);
	set @end_time = getdate();
	print '>> Load Duration: ' +cast(datediff(second, @start_time, @end_time) as nvarchar) +' seconds';
	print '>>----------';

		set @start_time = getdate();
	print '>> Truncating Table: bronze.crm_sales_details';
	truncate table bronze.crm_sales_details;

	print '>> Inserting Data Into: bronze.crm_sales_details';
	BULK INSERT bronze.crm_sales_details
	FROM 'C:\Users\cj843\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_crm/sales_details.csv'
	WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	);
	set @end_time = getdate();
	print '>> Load Duration: ' +cast(datediff(second, @start_time, @end_time) as nvarchar) +' seconds';
	print '>>----------';

print '---------------------';
print 'Loading ERP Tables';
print '---------------------';
	
			set @start_time = getdate();
	print '>> Truncating Table: bronze.erp_loc_a101';
	truncate table bronze.erp_loc_a101;

	print '>> Inserting Data Into: bronze.erp_loc_a101';
	BULK INSERT bronze.erp_loc_a101
	FROM 'C:\Users\cj843\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_erp/loc_a101.csv'
	WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	);
	set @end_time = getdate();
	print '>> Load Duration: ' +cast(datediff(second, @start_time, @end_time) as nvarchar) +' seconds';
	print '>>----------';

	print '>> Truncating Table: bronze.erp_cust_az12';
	truncate table bronze.erp_cust_az12;

				set @start_time = getdate();
	print '>> Inserting Data Into: bronze.erp_cust_az12';
	BULK INSERT bronze.erp_cust_az12
	FROM 'C:\Users\cj843\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_erp/cust_az12.csv'
	WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	);
	set @end_time = getdate();
	print '>> Load Duration: ' +cast(datediff(second, @start_time, @end_time) as nvarchar) +' seconds';
	print '>>----------';

			set @start_time = getdate();
	print '>> Truncating Table: bronze.erp_px_cat_g1v2';
	truncate table bronze.erp_px_cat_g1v2;

	print '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
	BULK INSERT bronze.erp_px_cat_g1v2
	FROM 'C:\Users\cj843\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_erp/px_cat_g1v2.csv'
	WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	);
	set @end_time = getdate();
	print '>> Load Duration: ' +cast(datediff(second, @start_time, @end_time) as nvarchar) +' seconds';
	print '>>----------';
	
	set @batch_end_time =GETDATE();
	print '============='
	print ' Loading Bronze Layer is Completed';
	print ' - Total Load Duration: ' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + ' seconds';
	print '============='
	end try
	begin catch
	print '===============';
	print 'ERROR OCCURED DURING LOADING BRONZE LAYER';
	print ' Error Message' + ERROR_MESSAGE();
	print ' Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
	print ' Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
	print '===============';

	end catch
end
