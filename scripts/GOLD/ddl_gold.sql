/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================


IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO
CREATE VIEW gold.dim_customers as
select
ROW_NUMBER() OVER(ORDER BY cst_id) customer_key,
ci.cst_id as customer_id,
ci.cst_key as customer_number ,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
CL.CNTRY as country,
ci.cst_marital_status as martial_status,
CASE 
	WHEN ci.cst_gndr!='n/a' then ci.cst_gndr
else COALESCE(ca.gen,'n/a')
END AS Gender,
ca.BDATE as birthdate,
ci.cst_create_date as create_date
from SILVER.crm_cust_info ci LEFT JOIN
SILVER.erp_CUST_AZ12 ca
on ca.CID=ci.cst_key
LEFT JOIN SILVER.erp_LOC_A101 CL
ON CL.CID=CI.cst_key;
GO
-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================

IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO
create view gold.dim_products as
SELECT
row_number() over(order by pr.prd_start_dt,pr.prd_key) as prd_key,
pr.prd_id as product_id,
pr.prd_key as product_number,
pr.prd_nm as product_name,
pr.cat_id as category_id,
px.CAT as category,
px.SUBCAT as sub_category,
px.MAINTENANCE,
pr.prd_cost as cost,
pr.prd_line as product_line,
pr.prd_start_dt as start_date
FROM SILVER.crm_prd_info pr
LEFT JOIN SILVER.erp_PX_CAT_G1V2 px
ON pr.cat_id= px.ID
WHERE PR.prd_end_dt IS NULL;
GO
-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO
create view GOLD.fact_sales AS
select
cs.sls_ord_num as order_number,
dp.prd_key as product_key,
DC.customer_key as customer_key,
cs.sls_order_dt as order_date,
cs.sls_ship_dt as ship_date,
cs.sls_due_dt as due_date,
cs.sls_sales as sales_amount,
cs.sls_quantity as quantity,
cs.sls_price as price
from silver.crm_sales_details cs
LEFT JOIN gold.dim_product dp
	on  cs.sls_prd_key= dp.product_number
left join gold.dim_customers dc
	ON CS.sls_cust_id= DC.customer_id
GO
