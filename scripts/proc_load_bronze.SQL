/*
=================================================
STORED PROCEDURE: LOAD BRONZE LAYER (Source--> Bronze)
==============================================
Script Purpose:
this stored procedure loads data into the 'bronze' schema from external CSV files.
It performs following actions:
truncates the bronze tables before loading the data.
Uses the 'BULK INSERT' command load  data from csv files to Bronze tables

Parameters:
None.
the stored procedure does not accept any parameters or return any values.
USAGE EXAMPLE:
EXEC bronze.load_bronze.
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @START_TIME DATETIME, @END_TIME DATETIME,
	@batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		PRINT '******************';
		PRINT 'LOADING BRONZE LAYER';
		PRINT'*******************';

		PRINT'-------------------';
		PRINT 'LOADING CRM TABLES';
		PRINT'-------------------';
		SET @batch_start_time=GETDATE();
		SET @START_TIME= GETDATE();
		PRINT'<< TRUNCATING TABLE:BRONZE.crm_cust_info';
			TRUNCATE table BRONZE.crm_cust_info;
			PRINT'<< INSERTING DATA INTO TABLE: BRONZE.crm_cust_info ';
			BULK INSERT BRONZE.crm_cust_info
			FROM 'C:\Users\DELL\Desktop\coding\SQL\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
			WITH (
					FIRSTROW=2,
					FIELDTERMINATOR = ',',
					TABLOCK
					);
				set @END_TIME = GETDATE();
					PRINT'>>load duration: '+ CAST(DATEDIFF(SECOND, @START_TIME,@END_TIME) AS NVARCHAR)+'Seconds';
					print'***************';

			SET @START_TIME = GETDATE();
		PRINT'<< TRUNCATING TABLE:BRONZE.crm_prd_info';
					TRUNCATE TABLE BRONZE.crm_prd_info
				PRINT'<< INSERTING DATA INTO TABLE: BRONZE.crm_prd_info ';
			BULK INSERT BRONZE.crm_prd_info
			FROM 'C:\Users\DELL\Desktop\coding\SQL\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
			WITH(
					FIRSTROW=2,
					FIELDTERMINATOR=',',
					TABLOCK
					);
			SET @END_TIME= GETDATE();
				PRINT'.. EXECUTION TIME: '+ CAST(DATEDIFF(SECOND, @START_TIME,@END_TIME) AS NVARCHAR)+'Seconds';
				print'***************';


			SET @START_TIME = GETDATE();
					PRINT'<< TRUNCATING TABLE:BRONZE.crm_sales_details';
			TRUNCATE TABLE BRONZE.crm_sales_details
		PRINT'<< INSERTING DATA INTO TABLE: BRONZE.crm_sales_details ';
			BULK INSERT BRONZE.crm_sales_details
			FROM 'C:\Users\DELL\Desktop\coding\SQL\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
			WITH(
					FIRSTROW=2,
					FIELDTERMINATOR=',',
					TABLOCK
					);
				SET @END_TIME = GETDATE();
					PRINT'.. EXECUTION TIME: '+ CAST(DATEDIFF(SECOND, @START_TIME,@END_TIME) AS NVARCHAR)+'Seconds';
					print'***************';

		PRINT'-------------------';
		PRINT 'LOADING ERP TABLES';
		PRINT'-------------------';

		SET @START_TIME = GETDATE();
		PRINT'<< TRUNCATING TABLE:BRONZE.erp_CUST_AZ12';
				TRUNCATE TABLE  BRONZE.erp_CUST_AZ12
			PRINT'<< INSERTING DATA INTO TABLE: BRONZE.erp_CUST_AZ12 ';
			BULK INSERT BRONZE.erp_CUST_AZ12
			FROM 'C:\Users\DELL\Desktop\coding\SQL\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
			WITH(
					FIRSTROW=2,
					FIELDTERMINATOR=',',
					TABLOCK
					);
				SET @END_TIME = GETDATE();
					PRINT'.. EXECUTION TIME: '+ CAST(DATEDIFF(SECOND, @start_time,@end_time) AS NVARCHAR)+'Seconds';
					print'***************';

				SET @START_TIME = GETDATE();
			PRINT'<< TRUNCATING TABLE:BRONZE.erp_LOC_A101';
					TRUNCATE TABLE  [BRONZE].[erp_LOC_A101]
					PRINT'<< INSERTING DATA INTO TABLE: BRONZE.erp_LOC_A101 ';
			BULK INSERT [BRONZE].[erp_LOC_A101]
			FROM 'C:\Users\DELL\Desktop\coding\SQL\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
			WITH(
					FIRSTROW=2,
					FIELDTERMINATOR=',',
					TABLOCK
					);
				SET @END_TIME = GETDATE();
					PRINT'.. EXECUTION TIME: '+ CAST(DATEDIFF(SECOND, @START_TIME,@END_TIME) AS NVARCHAR)+'Seconds';
					print'***************';


				SET @START_TIME = GETDATE();
		PRINT'<< TRUNCATING TABLE:BRONZE.erp_PX_CAT_G1V2';
				TRUNCATE TABLE [BRONZE].[erp_PX_CAT_G1V2]
			PRINT'<< INSERTING DATA INTO TABLE: BRONZE.erp_PX_CAT_G1V2 ';
			BULK INSERT [BRONZE].[erp_PX_CAT_G1V2]
			FROM 'C:\Users\DELL\Desktop\coding\SQL\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
			WITH(
					FIRSTROW=2,
					FIELDTERMINATOR=',',
					TABLOCK
					);
				SET @END_TIME = GETDATE();
					PRINT'.. EXECUTION TIME: '+ CAST(DATEDIFF(SECOND, @START_TIME,@END_TIME) AS NVARCHAR)+'Seconds';
					print'***************';
		SET @batch_end_time = GETDATE();
			print'=====================';
			PRINT'BATCH loading time: '+ CAST(DATEDIFF(SECOND, @batch_start_time,@batch_end_time) AS NVARCHAR)+'Seconds';
			print'=====================';
	END TRY
		BEGIN CATCH
			PRINT'=========================';
			PRINT'ERROR OCCURED DURING LOADING BRONZE LAYER';
			PRINT 'ERROR MESSAGE'+ ERROR_MESSAGE();
			PRINT'ERROR MESSAGE'+ CAST(ERROR_NUMBER() AS NVARCHAR(50));
			PRINT'ERROR MESSAGE'+ CAST(ERROR_STATE() AS NVARCHAR(50));
			PRINT'=========================';
		END CATCH
END
		
		
		
	
