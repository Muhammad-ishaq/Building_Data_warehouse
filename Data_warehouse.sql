
use master;


-- ========================= Creating Database ================================= --


create database Data_warehouse;
use Data_warehouse;



-- ========================= Creating Schemas ================================= --



create schema Bronze;
go
create schema Silver;
go
create schema Gold;
go



-- ========================= Creating Bronze Layer ================================= --


IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;
GO

CREATE TABLE bronze.crm_cust_info (
    cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_marital_status  NVARCHAR(50),
    cst_gndr            NVARCHAR(50),
    cst_create_date     DATE
);
GO

IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;
GO

CREATE TABLE bronze.crm_prd_info (
    prd_id       INT,
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME
);
GO

IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
GO

CREATE TABLE bronze.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);
GO

IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE bronze.erp_loc_a101;
GO

CREATE TABLE bronze.erp_loc_a101 (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE bronze.erp_cust_az12;
GO

CREATE TABLE bronze.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2;
GO

CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50)
);
GO

select * from Bronze.crm_cust_info


-- ========================= Inserting Data into Tables ================================= --

Create or Alter procedure bronze.load_bronze as 
Begin

print '================== Loading Bronze Layer ===================='

    Truncate Table [Bronze].[crm_cust_info] 
    Bulk insert [Bronze].[crm_cust_info]
    from 'C:\projects\SQL Series\MSSQL SERVER\prj_dwarehouse\cust_info.csv'
    with(
    Firstrow = 2,
    Fieldterminator = ',' ,
    Tablock 

    );

    select * from Bronze.crm_cust_info;

    Truncate Table [Bronze].[crm_prd_info] 
    Bulk insert [Bronze].[crm_prd_info]
    from 'C:\projects\SQL Series\MSSQL SERVER\prj_dwarehouse\prd_info.csv'
    with(
    Firstrow = 2,
    Fieldterminator = ',' ,
    Tablock 

    );


    Truncate Table [Bronze].[crm_sales_details] 
    Bulk insert [Bronze].[crm_sales_details]
    from 'C:\projects\SQL Series\MSSQL SERVER\prj_dwarehouse\sales_details.csv'
    with(
    Firstrow = 2,
    Fieldterminator = ',' ,
    Tablock 

    );


    Truncate Table [Bronze].[erp_cust_az12] 
    Bulk insert [Bronze].[erp_cust_az12]
    from 'C:\projects\SQL Series\MSSQL SERVER\prj_dwarehouse\CUST_AZ12.csv'
    with(
    Firstrow = 2,
    Fieldterminator = ',' ,
    Tablock 

    );


    Truncate Table [Bronze].[erp_loc_a101] 
    Bulk insert [Bronze].[erp_loc_a101]
    from 'C:\projects\SQL Series\MSSQL SERVER\prj_dwarehouse\LOC_A101.csv'
    with(
    Firstrow = 2,
    Fieldterminator = ',' ,
    Tablock 

    );


    Truncate Table [Bronze].[erp_px_cat_g1v2]
    Bulk insert [Bronze].[erp_px_cat_g1v2]
    from 'C:\projects\SQL Series\MSSQL SERVER\prj_dwarehouse\PX_CAT_G1V2.csv'
    with(
    Firstrow = 2,
    Fieldterminator = ',' ,
    Tablock 

    )
End;


-- ========================= Creating Silver Layer ================================= --


IF OBJECT_ID('Silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE Silver.crm_cust_info;
GO

CREATE TABLE Silver.crm_cust_info (
    cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_marital_status  NVARCHAR(50),
    cst_gndr            NVARCHAR(50),
    cst_create_date     DATE,
    dwh_create_date  DateTime2 Default getdate()
);
GO

IF OBJECT_ID('Silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE Silver.crm_prd_info;
GO

CREATE TABLE Silver.crm_prd_info (
    prd_id       INT,
    cat_id    NVARCHAR(50),
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line    NVARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt  DATE,
    dwh_create_date  DateTime2 Default getdate()
);
GO
select * from Silver.crm_prd_info
IF OBJECT_ID('Silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE Silver.crm_sales_details;
GO

CREATE TABLE Silver.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt Date,
    sls_ship_dt  Date,
    sls_due_dt   Date,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT,
    dwh_create_date  DateTime2 Default getdate()
);
GO

IF OBJECT_ID('Silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE Silver.erp_loc_a101;
GO

CREATE TABLE Silver.erp_loc_a101 (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50),
    dwh_create_date  DateTime2 Default getdate()
);
GO

IF OBJECT_ID('Silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE Silver.erp_cust_az12;
GO

CREATE TABLE Silver.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50),
    dwh_create_date  DateTime2 Default getdate()
);
GO

IF OBJECT_ID('Silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE Silver.erp_px_cat_g1v2;
GO

CREATE TABLE Silver.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50),
    dwh_create_date  DateTime2 Default getdate()
);
GO



select * from [Silver].[erp_cust_az12]


-- ================== Now we start cleaning the data and load into Silver Layer ========================= --


/* Objective:  
    > check for duplicated 
    > check for Nulls 
    > check for unwanted spaces
    > Data Standardization/consistency
*/


-- check for duplicated 
select cst_id, count (*)
from [Bronze].[crm_cust_info]
group by cst_id
having COUNT(*) > 1;

-- order the data by create-date using window fnc
select *,
ROW_NUMBER() over(partition by cst_id order by cst_create_date desc ) last_flag
from [Bronze].[crm_cust_info]
where cst_id=29466 ;

 -- now checking all the records
    select * from (select *,
    ROW_NUMBER() over(partition by cst_id order by cst_create_date desc ) last_flag
    from [Bronze].[crm_cust_info]
)x where last_flag != 1;


-- Or we can use CTE
  with cte_dup as (select *,
    ROW_NUMBER() over(partition by cst_id order by cst_create_date desc ) last_flag
    from [Bronze].[crm_cust_info]
)
select * from cte_dup
where last_flag != 1;

-- ================ This is our main Exhbit ============================
 -- now here we keep the records which occurs only one time, no duplicates. 
    select * from (select *,
    ROW_NUMBER() over(partition by cst_id order by cst_create_date desc ) last_flag
    from [Bronze].[crm_cust_info]
)x where last_flag = 1;
-- =======================================================================

-- check for unwanted spaces
select * from Bronze.crm_cust_info

select cst_firstname from Bronze.crm_cust_info
 where cst_firstname != TRIM(cst_firstname);

select cst_lastname from Bronze.crm_cust_info
 where cst_lastname != TRIM(cst_lastname);

 -- Taking our main Exhbit

 select  
 cst_id,
 cst_key,
trim( cst_firstname) as cst_firstname_new ,
 trim(cst_lastname) as cst_lastname_new,

 case 
     when upper( cst_marital_status) = 's' then 'Single'
      when upper( cst_marital_status) = 'M' then 'Married'
      else 'N/A'
  end cst_marital_status_new,
 case
     when upper(cst_gndr) = 'F' then 'Female'
     when upper(cst_gndr) = 'M' then 'Male'
     else 'N/A'
 End cst_gndr_new,
cst_create_date
 from (select *,
    ROW_NUMBER() over(partition by cst_id order by cst_create_date desc ) last_flag
    from [Bronze].[crm_cust_info]
)x where last_flag = 1;

-- ========== This is clean data, now load it into silver layer ==========

print '==================<<<<<<<<<<>>>>>>>>>>>>>>>>>>>====================='
print '======== loading data into silver layer ========='
print '==================<<<<<<<<<<>>>>>>>>>>>>>>>>>>>====================='
print '=======  Truncate Table Silver.crm_cust_info ================'
Truncate Table Silver.crm_cust_info
insert into Silver.crm_cust_info(
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date
)
select  
 cst_id,
 cst_key,
trim( cst_firstname) as cst_firstname_new ,
 trim(cst_lastname) as cst_lastname_new,

 case 
     when upper( cst_marital_status) = 's' then 'Single'
      when upper( cst_marital_status) = 'M' then 'Married'
      else 'N/A'
  end cst_marital_status_new,
 case
     when upper(cst_gndr) = 'F' then 'Female'
     when upper(cst_gndr) = 'M' then 'Male'
     else 'N/A'
 End cst_gndr_new,
cst_create_date
 from (select *,
    ROW_NUMBER() over(partition by cst_id order by cst_create_date desc ) last_flag
    from [Bronze].[crm_cust_info]
)x where last_flag = 1;

select * from Silver.crm_cust_info;

-- now moving to another table in Bronze Layer >> [Bronze].[crm_prd_info] 

select * from [Bronze].[crm_prd_info];

-- checking duplicates
select prd_id, count (*) from [Bronze].[crm_prd_info]
group by prd_id having count (*)>1 or prd_id is null ;

select prd_id, 
prd_key,
replace(SUBSTRING(prd_key, 1,5) , '-','_') as catg_id, -- dividing prd_key into two part to make catd_id in seperate column
SUBSTRING(prd_key,7, LEN(prd_key)) as prd_key_new,
prd_nm,
isnull(prd_cost, 0) as prd_cost,
case 
    when upper(prd_line) = 'R' then  'Race'
    when upper(prd_line) = 'S' then  'Street'
    when upper(prd_line) = 'M' then  'Mountain'
    when upper(prd_line) = 'T' then  'Touring'
    else 'n/a'
end prd_line_new,
cast (prd_start_dt as date) as prd_start_dt,
cast(LEAD(prd_start_dt) over(partition by prd_key order by prd_start_dt) -1 as Date)
as prd_end_dt_new -- start dt of next prd is the end dt of previous prd -1
from  [Bronze].[crm_prd_info];

-- ========== This is clean data, now load it into silver layer ==========

print '==================<<<<<<<<<<>>>>>>>>>>>>>>>>>>>====================='
print '======== loading data into silver layer ========='
print '==================<<<<<<<<<<>>>>>>>>>>>>>>>>>>>====================='
print '=======  Truncate Table Silver.crm_prd_info================'
Truncate Table Silver.crm_prd_info
insert into Silver.crm_prd_info(
    prd_id, 
    cat_id,
    prd_key,
    prd_nm,
    prd_cost ,
    prd_line,
    prd_start_dt,
    prd_end_dt
   )
select prd_id, 
replace(SUBSTRING(prd_key, 1,5) , '-','_') as cat_id, -- dividing prd_key into two part to make catd_id in seperate column
SUBSTRING(prd_key,7, LEN(prd_key)) as prd_key, -- for connecting with another table
prd_nm,
isnull(prd_cost, 0) as prd_cost,
case 
    when upper(prd_line) = 'R' then  'Race'
    when upper(prd_line) = 'S' then  'Street'
    when upper(prd_line) = 'M' then  'Mountain'
    when upper(prd_line) = 'T' then  'Touring'
    else 'n/a'
end prd_line,
cast (prd_start_dt as date) as prd_start_dt,
cast(LEAD(prd_start_dt) over(partition by prd_key order by prd_start_dt) -1 as Date)
as prd_end_dt -- start dt of next prd is the end dt of previous prd -1
from  [Bronze].[crm_prd_info];


 select * from Silver.crm_prd_info;

-- now moving to another table in Bronze Layer >> Bronze.crm_sales_details

-- for checking data quality

/* 
fixing dates and prices & sales 
select distinct sls_sales,sls_quantity,sls_price from Bronze.crm_sales_details
where sls_sales ! = sls_price * sls_quantity
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <0 or sls_quantity <0 or sls_price <0 */


select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
case 
    when sls_order_dt =0 or len(sls_order_dt) ! = 8 then null
    else cast(cast(sls_order_dt as varchar) as date)
end sls_order_dt,

case 
    when sls_ship_dt =0 or len(sls_ship_dt) ! = 8 then null
    else cast(cast(sls_ship_dt as varchar) as date)
end sls_ship_dt,

case 
    when sls_due_dt =0 or len(sls_due_dt) ! = 8 then null
    else cast(cast(sls_due_dt as varchar) as date)
end sls_due_dt,

case 
    when sls_sales is null or sls_sales <0 or sls_sales != sls_quantity * abs(sls_price)
    then sls_quantity * abs(sls_price)
    else sls_sales
end as sls_sales,
sls_quantity,
case 
    when sls_price is null or sls_price <0 then sls_sales / sls_quantity
    else sls_price
end as sls_price
from Bronze.crm_sales_details;


-- ========== This is clean data, now load it into silver layer ==========

print '==================<<<<<<<<<<>>>>>>>>>>>>>>>>>>>====================='
print '======== loading data into silver layer ========='
print '==================<<<<<<<<<<>>>>>>>>>>>>>>>>>>>====================='
print '=======  Truncate Table Silver.crm_sales_details================'
Truncate Table Silver.crm_sales_details
insert into Silver.crm_sales_details(
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
case 
    when sls_order_dt =0 or len(sls_order_dt) ! = 8 then null
    else cast(cast(sls_order_dt as varchar) as date)
end sls_order_dt,

case 
    when sls_ship_dt =0 or len(sls_ship_dt) ! = 8 then null
    else cast(cast(sls_ship_dt as varchar) as date)
end sls_ship_dt,

case 
    when sls_due_dt =0 or len(sls_due_dt) ! = 8 then null
    else cast(cast(sls_due_dt as varchar) as date)
end sls_due_dt,

case 
    when sls_sales is null or sls_sales <0 or sls_sales != sls_quantity * abs(sls_price)
    then sls_quantity * abs(sls_price)
    else sls_sales
end as sls_sales,
sls_quantity,
case 
    when sls_price is null or sls_price <0 then sls_sales / sls_quantity
    else sls_price
end as sls_price
from Bronze.crm_sales_details;

select * from Silver.crm_sales_details


-- now moving to another table in Bronze Layer >> [Bronze].[erp_cust_az12]

select 
case 
    when cid like 'NAS%' then SUBSTRING(cid , 4, len(cid))
    else cid
end cid,
case 
    when bdate > GETDATE() then null
    else bdate
end bdate,
case
    when upper(gen) in ('F','female')  then 'Female'
    when upper(gen) in ('M','Male')  then 'Male'
    else 'n/a'
end gen
from [Bronze].[erp_cust_az12]
select * from [Bronze].[erp_cust_az12]

-- ========== This is clean data, now load it into silver layer ==========

print '==================<<<<<<<<<<>>>>>>>>>>>>>>>>>>>====================='
print '======== loading data into silver layer ========='
print '==================<<<<<<<<<<>>>>>>>>>>>>>>>>>>>====================='
print '=======  Truncate Table [Silver].[erp_cust_az12]================'
Truncate Table [Silver].[erp_cust_az12]
insert into [Silver].[erp_cust_az12] (
cid,bdate,gen)
select 
case 
    when cid like 'NAS%' then SUBSTRING(cid , 4, len(cid))
    else cid
end cid,
case 
    when bdate > GETDATE() then null
    else bdate
end bdate,
case
    when upper(gen) in ('F','female')  then 'Female'
    when upper(gen) in ('M','Male')  then 'Male'
    else 'n/a'
end gen
from [Bronze].[erp_cust_az12]
select * from Silver.erp_cust_az12

-- now moving to another table in Bronze Layer >> [Bronze].[erp_loc_a101]

select 
REPLACE(cid, '-' , '') as cid,
case
    when TRIM(cntry) = 'DE' then 'Germany'
    when TRIM(cntry) in ( 'US','USA') then 'United States'
    when TRIM(cntry) = '' or cntry is null then 'n/a'
    else TRIM(cntry)
end cntry
from [Bronze].[erp_loc_a101]

-- ========== This is clean data, now load it into silver layer ==========

print '==================<<<<<<<<<<>>>>>>>>>>>>>>>>>>>====================='
print '======== loading data into silver layer ========='
print '==================<<<<<<<<<<>>>>>>>>>>>>>>>>>>>====================='
print '=======  Truncate Table Silver.[erp_loc_a101] ================'
Truncate Table Silver.[erp_loc_a101] 
insert into Silver.erp_loc_a101 (
cid,cntry
)
select 
REPLACE(cid, '-' , '') as cid,
case
    when TRIM(cntry) = 'DE' then 'Germany'
    when TRIM(cntry) in ( 'US','USA') then 'United States'
    when TRIM(cntry) = '' or cntry is null then 'n/a'
    else TRIM(cntry)
end cntry
from [Bronze].[erp_loc_a101]
select * from Silver.erp_loc_a101

-- now moving to another table in Bronze Layer >> [Bronze].[erp_px_cat_g1v2]
select * from [Bronze].[erp_px_cat_g1v2]
select 
id,
cat,
subcat,
maintenance
from [Bronze].[erp_px_cat_g1v2]

-- ========== So after checking we found out that, This is a clean data, now load it into silver layer ==========

print '==================<<<<<<<<<<>>>>>>>>>>>>>>>>>>>====================='
print '======== loading data into silver layer ========='
print '==================<<<<<<<<<<>>>>>>>>>>>>>>>>>>>====================='
print '=======  Truncate Table Silver.erp_px_cat_g1v2================'
Truncate Table Silver.erp_px_cat_g1v2
insert into Silver.erp_px_cat_g1v2(
id,
cat,
subcat,
maintenance
)
-- select * from [Bronze].[erp_px_cat_g1v2]
select 
id,
cat,
subcat,
maintenance
from [Bronze].[erp_px_cat_g1v2]

select * from Silver.erp_px_cat_g1v2


-- ===================== Now Building Gold Layer ================================
/* we will start selecting  the columns from silver layer and 
check for the duplicates values after joining multiple tabbles */


select 
    ci.cst_id,
    ci.cst_key,
    ci.cst_firstname,
    ci.cst_lastname,
    ci.cst_marital_status,
    ci.cst_gndr,
   ci.cst_create_date,
   ca.bdate,
   ca.gen,
   lo.cntry
from Silver.crm_cust_info ci 
left join Silver.erp_cust_az12 ca on 
ci.cst_key=ca.cid
left join  Silver.erp_loc_a101 lo on
ci.cst_key=lo.cid
where cst_id is not null;

-- check for the duplicates values after joining multiple tabbles
select cst_id,COUNT(*) from (
select 
    ci.cst_id,
    ci.cst_key,
    ci.cst_firstname,
    ci.cst_lastname,
    ci.cst_marital_status,
    ci.cst_gndr,
   ci.cst_create_date,
   ca.bdate,
   ca.gen,
   lo.cntry
from Silver.crm_cust_info ci left join 
Silver.erp_cust_az12 ca on 
ci.cst_key=ca.cid
left join  Silver.erp_loc_a101 lo on
ci.cst_key=lo.cid
where cst_id is not null)x
group by cst_id
having count(*) >1;

-- all good, no duplicates found
/*so after joining columns, we found an issue in gender columns, they come from two diff tables, here we 
will integration to fix the issue, assuming crm data source is the master */


select 
    ci.cst_id,
    ci.cst_key,
    ci.cst_firstname,
    ci.cst_lastname,
    ci.cst_marital_status,
   case 
       when ci.cst_gndr != 'n/a' then ci.cst_gndr 
       else coalesce (ca.gen, 'n/a')
   end as new_gen,
   ci.cst_create_date,
   ca.bdate,
   lo.cntry
from Silver.crm_cust_info ci 
left join Silver.erp_cust_az12 ca on 
ci.cst_key=ca.cid
left join  Silver.erp_loc_a101 lo on
ci.cst_key=lo.cid
where cst_id is not null;

/* now aliasing the columns, making them user friendly. 
and create a surrogate key, which is unique id for each record, using row_num window fcn */

select 
ROW_NUMBER() over (order by cst_id) as customer_key,
    ci.cst_id as customer_id,
    ci.cst_key as customer_number,
    ci.cst_firstname as first_name,
    ci.cst_lastname as last_name,
    ci.cst_marital_status as marital_status,
   case 
       when ci.cst_gndr != 'n/a' then ci.cst_gndr 
       else coalesce (ca.gen, 'n/a')
   end as gender,
   ci.cst_create_date as create_date,
   ca.bdate as birth_date,
   lo.cntry as country
from Silver.crm_cust_info ci 
left join Silver.erp_cust_az12 ca on 
ci.cst_key=ca.cid
left join  Silver.erp_loc_a101 lo on
ci.cst_key=lo.cid
where cst_id is not null;


-- ====================== END OF PROJECT =================================
