create storage integration s3_int
type = external_stage
storage_provider = 'S3'
enabled = true
storage_aws_role_arn = 'arn:aws:iam::713881825440:role/Swiggy_Data_SnowFlake_Access'
storage_allowed_locations = ('s3://swiggy-data-project/Fake Data Generated 2.0/');
 
 
desc INTEGRATION s3_int ;
 
create or replace stage my_s3_stage
storage_integration = s3_int
url = 's3://swiggy-data-project/Fake Data Generated 2.0/'
;
list @my_s3_stage;
 
GRANT USAGE ON INTEGRATION s3_int TO ROLE SYSADMIN;


-- Reference Blog Link => https://snowflakewiki.medium.com/connecting-snowflake-to-aws-ef7b6de1d6aa