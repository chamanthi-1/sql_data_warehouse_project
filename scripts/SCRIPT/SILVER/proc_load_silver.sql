/*
=================================================
STORED PROCEDURE: LOAD BRONZE LAYER (Bronze--> Silver)
==============================================
Script Purpose:
this stored procedure performs the ETL(Extract,Transfer,Load) process to 
populate the 'SILVER' schema TABLES from BRONZE SCHEMA.
ACTIONS PERFORMED:
truncates the SILVERtables before loading the data.
INSERTS TRANSFORMED AND CLEANED DATA from BRONZE to SILVER tables

Parameters:
None.
the stored procedure does not accept any parameters or return any values.
USAGE EXAMPLE:
EXEC silver.load_silver.
*/
CREATE OR ALTER PROCEDURE SILVER.LOAD_SILVER AS
	BEGIN
	DECLARE @START_TIME DATETIME, @END_TIME DATETIME,
		@batch_start_time DATETIME, @batch_end_time DATETIME;
		BEGIN TRY
		SET @batch_start_time=GETDATE();
		PRINT '******************';
			PRINT 'LOADING SILVER LAYER';
			PRINT'*******************';

			PRINT'-------------------';
			PRINT 'LOADING CRM TABLES';
			PRINT'-------------------';
			--LOADING CRM_CUST_INFO----
			SET @START_TIME= GETDATE();
	PRINT'<< TRUNCATING TABLE:SILVER.crm_cust_info';
	truncate TABLE SILVER.crm_cust_info;
	PRINT'<< INSERTING DATA INTO TABLE: SILVER.crm_cust_info ';
	INSERT INTO SILVER.crm_cust_info(
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date)

	SELECT
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname)AS cst_lastname,
	CASE
		WHEN UPPER(TRIM(cst_marital_status)) ='M' THEN 'MARRIED'
		WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'SINGLE'
			ELSE 'N/A'
	END cst_marital_status,
	CASE
		WHEN UPPER(TRIM(cst_gndr))= 'F' THEN 'FEMALE'
		WHEN UPPER(TRIM(cst_gndr))= 'M' THEN 'MALE'
		ELSE 'N/A'
	END cst_gndr,
	cst_create_date
	FROM(

	select
	*,
	ROW_NUMBER() OVER(PARTITION BY CST_ID ORDER BY CST_CREATE_DATE DESC) FLAG_LAST
	from BRONZE.crm_cust_info
	WHERE cst_id IS NOT NULL)T
	WHERE FLAG_LAST=1 
					set @END_TIME = GETDATE();
						PRINT'>>load duration: '+ CAST(DATEDIFF(SECOND, @START_TIME,@END_TIME) AS NVARCHAR)+'Seconds';
						print'***************';

				--LOADING CRM_PRD_INFO----
				SET @START_TIME= GETDATE();
	PRINT'<< TRUNCATING TABLE:SILVER.crm_prd_info';
	truncate TABLE SILVER.crm_prd_info;
	PRINT'<< INSERTING DATA INTO TABLE: SILVER.crm_prd_info ';
	INSERT INTO SILVER.crm_prd_info(
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
	)
	SELECT
	prd_id,
	REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
	substring(prd_key,7,LEN(prd_key)) as prd_key,
	prd_nm,
	COALESCE(prd_cost, '0')AS prd_cst,

	CASE
		WHEN prd_line='M' THEN 'MOUNTAIN'
		WHEN prd_line='R' THEN 'ROAD'
		WHEN prd_line='S' THEN 'OTHER SALES'
		WHEN prd_line='T' THEN 'TOURING'
		ELSE 'N/A'
		END AS prd_line,
	CAST(prd_start_dt AS DATE) AS prd_start_dt,
	cast(LEAD(prd_start_dt)OVER(PARTITION BY prd_key ORDER BY prd_Start_dt)-1 as date) prd_end_dt
	from BRONZE.crm_prd_info 
	set @END_TIME = GETDATE();
						PRINT'>>load duration: '+ CAST(DATEDIFF(SECOND, @START_TIME,@END_TIME) AS NVARCHAR)+'Seconds';
						print'***************';

						--LOADING CRM_SALES_DETAILS----
						SET @START_TIME= GETDATE();
	PRINT'<< TRUNCATING TABLE:SILVER.crm_sales_details';
	truncate TABLE SILVER.crm_sales_details;
	PRINT'<< INSERTING DATA INTO TABLE: SILVER.crm_sales_details ';
	INSERT INTO SILVER.crm_sales_details(
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

		CASE
			WHEN sls_order_dt =0 OR LEN(sls_order_dt)!=8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS NVARCHAR(50)) AS DATE)
			END AS sls_order_dt,
		CASE
			WHEN sls_ship_dt <=0 OR LEN(sls_ship_dt)!=8 THEN NULL
			else CAST(CAST(sls_ship_dt AS NVARCHAR) AS date)
			END sls_ship_dt,
		CASE
			WHEN sls_due_dt<=0 OR LEN(sls_due_dt)!=8 THEN NULL
			else CAST(CAST(sls_due_dt AS NVARCHAR) AS date)
			END sls_due_dt,
	CASE
		WHEN sls_sales <=0 or sls_sales IS NULL OR sls_sales!=sls_quantity*abs(sls_price)
			THEN abs((sls_quantity*sls_price))
			else abs(sls_sales)
		END sls_sales,
	sls_quantity,
	CASE
		WHEN sls_price<=0 or sls_price IS NULL 
		THEN sls_sales/NULLIF(sls_quantity,0)
		ELSE sls_price
	END sls_price
	from BRONZE.crm_sales_details
	set @END_TIME = GETDATE();
						PRINT'>>load duration: '+ CAST(DATEDIFF(SECOND, @START_TIME,@END_TIME) AS NVARCHAR)+'Seconds';
						print'***************';

						--LOADING erp_CUST_AZ12----
						SET @START_TIME= GETDATE();
	PRINT'<< TRUNCATING TABLE:SILVER.erp_CUST_AZ12';
	truncate TABLE SILVER.erp_CUST_AZ12;
	PRINT'<< INSERTING DATA INTO TABLE: SILVER.erp_CUST_AZ12 ';
	INSERT INTO SILVER.erp_CUST_AZ12(
	CID,
	BDATE,
	GEN
	)
	select
	CASE
		WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LEN(CID))
		ELSE CID
	END CID,
	CASE
		WHEN BDATE>GETDATE() THEN NULL
		ELSE BDATE
	END BDATE,

	CASE 
		WHEN UPPER(TRIM(GEN))IN ('M','MALE') THEN 'MALE'
		WHEN UPPER(TRIM(GEN))IN('F','FEMALE') THEN 'FEMALE'
		ELSE 'N/A'
		END AS GEN
	from BRONZE.erp_CUST_AZ12
	set @END_TIME = GETDATE();
						PRINT'>>load duration: '+ CAST(DATEDIFF(SECOND, @START_TIME,@END_TIME) AS NVARCHAR)+'Seconds';
						print'***************';

						--LOADING erp_LOC_A101----
						SET @START_TIME= GETDATE();
	PRINT'<< TRUNCATING TABLE:SILVER.erp_LOC_A101';
	truncate TABLE SILVER.erp_LOC_A101;
	PRINT'<< INSERTING DATA INTO TABLE: SILVER.erp_LOC_A101 ';
	INSERT INTO SILVER.erp_LOC_A101(
	CID,CNTRY)
	SELECT
	REPLACE(CID,'-','') AS CID,
	CASE
		WHEN TRIM(CNTRY) IN('US','USA') THEN 'UNITEDSTATES'
		WHEN TRIM(CNTRY)='DE' THEN 'GERMANY'
		WHEN TRIM(CNTRY)='' OR TRIM(CNTRY)=NULL THEN 'N/A'
		ELSE TRIM(CNTRY)
		END CNTRY
	FROM BRONZE.erp_LOC_A101
	set @END_TIME = GETDATE();
						PRINT'>>load duration: '+ CAST(DATEDIFF(SECOND, @START_TIME,@END_TIME) AS NVARCHAR)+'Seconds';
						print'***************';

						--LOADINGerp_PX_CAT_G1V2----
	SET @START_TIME= GETDATE();
	PRINT'<< TRUNCATING TABLE:SILVER.erp_PX_CAT_G1V2';
	truncate TABLE SILVER.erp_PX_CAT_G1V2;
	PRINT'<< INSERTING DATA INTO TABLE: SILVER.erp_PX_CAT_G1V2 ';
	INSERT INTO SILVER.erp_PX_CAT_G1V2(
	ID,
	CAT,
	SUBCAT,
	MAINTENANCE
	)
	SELECT
	ID,
	CAT,
	SUBCAT,
	MAINTENANCE
	FROM BRONZE.erp_PX_CAT_G1V2
	set @END_TIME = GETDATE();
						PRINT'>>load duration: '+ CAST(DATEDIFF(SECOND, @START_TIME,@END_TIME) AS NVARCHAR)+'Seconds';
						print'***************';
							SET @batch_end_time = GETDATE();
				print'=====================';
				PRINT'LOADING SILVER DATA IS COMPLETED'
				PRINT'BATCH loading time: '+ CAST(DATEDIFF(SECOND, @batch_start_time,@batch_end_time) AS NVARCHAR)+'Seconds';
				print'=====================';
	END TRY
		BEGIN CATCH
		PRINT'=========================';
				PRINT'ERROR OCCURED DURING LOADING SILVER LAYER';
				PRINT 'ERROR MESSAGE'+ ERROR_MESSAGE();
				PRINT'ERROR MESSAGE'+ CAST(ERROR_NUMBER() AS NVARCHAR(50));
				PRINT'ERROR MESSAGE'+ CAST(ERROR_STATE() AS NVARCHAR(50));
				PRINT'=========================';
			END CATCH
	END

	
