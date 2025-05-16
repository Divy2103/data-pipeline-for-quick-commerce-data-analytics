create storage integration s3_int
 type = external_stage
 storage_provider = 'S3'
 enabled = true
 storage_aws_role_arn = 'arn:aws:iam::713881825440:role/Swiggy_Data_SnowFlake_Access'
 storage_allowed_locations = ('s3://swiggy-data-project/Fake Data Generated 2.0/');


desc INTEGRATION s3_int ;
-- STORAGE_AWS_IAM_USER_ARN:arn:aws:iam::515966541372:user/lz6z0000-s
-- STORAGE_AWS_ROLE_ARN:arn:aws:iam::713881825440:role/Swiggy_Data_SnowFlake_Access
-- STORAGE_AWS_EXTERNAL_ID: SN66697_SFCRole=1_CoV2aD3Hl2boBB5ht8rU3wxefqs=

use role sysadmin;
use database swiggy_db;
use schema stage_sch;


create or replace stage AWS_S3_STAGE
 storage_integration = s3_int
 url = 's3://swiggy-data-project/Fake Data Generated 2.0/'
 ;
 
list @my_s3_stage;

GRANT USAGE ON INTEGRATION s3_int TO ROLE SYSADMIN;