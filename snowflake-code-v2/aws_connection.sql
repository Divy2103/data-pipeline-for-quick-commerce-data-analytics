create storage integration s3_int
 type = external_stage
 storage_provider = 'S3'
 enabled = true
 storage_aws_role_arn = 'arn:aws:iam::713881825440:role/snowflake-role'
 storage_allowed_locations = ('s3://swiggy-data-project/Fake Data Generated 2.0/')
 ;

 desc integration s3_int;

 create or replace stage SWIGGY_DB.STAGE_SCH.aws_s3_stage
 storage_integration = s3_int
 url = 's3://swiggy-data-project/Fake Data Generated 2.0/'
 ;

 list @aws_s3_stage;

 GRANT USAGE ON INTEGRATION s3_int TO ROLE SYSADMIN;