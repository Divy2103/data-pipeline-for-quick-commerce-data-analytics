-- change context
use role sysadmin;
use database swiggy_db;
use schema stage_sch;
use warehouse adhoc_wh;

-- create restaurant table under stage location, with all text value + audit column for copy command
create or replace table swiggy_db.stage_sch.restaurant (
    restaurant_id text,      
    name text ,                                       
    cuisine_type text,                                   
    pricing_for_2 text,                                 
    restaurant_phone text,                              
    operating_hours text,                                
    location_id text ,                                   
    active_flag text ,                                   
    open_status text ,                                   
    locality text, 
    restaurant_address text, 
    Ratings float,  
    Coupons variant, 
    latitude text,                                       
    longitude text,                                      
    created_date text,                                    
    modified_date text,                                   

    -- audit columns for debugging
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
comment = 'This is the restaurant raw table where data will be copied from internal stage using copy command. This is as-is data representation from the source location. All the columns are text data type except the audit columns that are added for traceability.'
;

-- Stream object to capture the changes. 
create or replace stream stage_sch.restaurant_stm 
on table stage_sch.restaurant
append_only = true
comment = 'This is the append-only stream object on restaurant table ';


-- running copy command to load the data into stage-restaurant table.
copy into stage_sch.restaurant (restaurant_id, name, cuisine_type, pricing_for_2, restaurant_phone, 
                      operating_hours, location_id, active_flag, open_status, 
                      locality, restaurant_address,Ratings,Coupons, latitude, longitude, 
                      created_date, modified_date, 
                      _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
from (
    select 
        t.$1::text as restaurantid,        
        t.$2::text as name,
        t.$3::text as cuisinetype,
        t.$4::text as pricing_for_2,
        t.$5::text as restaurant_phone,
        t.$6::text as operatinghours,
        t.$7::text as locationid,
        t.$8::text as activeflag,
        t.$9::text as openstatus,
        t.$10::text as locality,
        t.$11::text as restaurant_address,
        t.$12::float as Ratings,
        t.$13::variant as Coupons,
        t.$14::text as latitude,
        t.$15::text as longitude,
        t.$16::text as createddate,
        t.$17::text as modifieddate,
        -- audit columns for tracking & debugging
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp() as _copy_data_ts
     from @swiggy_db.stage_sch.aws_s3_stage/2025/4/9/restaurant.csv
 t
)
file_format = (format_name = 'stage_sch.csv_file_format')
on_error = abort_statement;

select * from restaurant;


-- the restaurant table where data types are defined. 
create or replace table swiggy_db.clean_sch.restaurant (
    restaurant_sk number autoincrement primary key,              
    restaurant_id number unique,                                 
    name string(100) not null,                                   
    cuisine_type string,                                         
    pricing_for_two number(10, 2),                              
    restaurant_phone string(15) ,                                
    operating_hours string(100),                                 
    location_id_fk number,                                       
    active_flag string(10),                                      -- indicates if the restaurant is active
    open_status string(10),                                      -- indicates if the restaurant is currently open
    locality string(100),                                        
    restaurant_address string,                                   
    ratings number(4,3),
    latitude number(9, 6),                                       
    longitude number(9, 6),                                      
    created_date timestamp_tz,                                  
    modified_date timestamp_tz,                                 

    -- additional audit columns
    _stg_file_name string,                                       -- file name for audit
    _stg_file_load_ts timestamp_ntz,                             -- file load timestamp for audit
    _stg_file_md5 string,                                        -- md5 hash for file content for audit
    _copy_data_ts timestamp_ntz default current_timestamp        -- timestamp when data is copied, defaults to current timestamp
)
comment = 'Restaurant entity under clean schema with appropriate data type under clean schema layer, data is populated using merge statement from the stage layer location table. This table does not support SCD2';

create or replace stream swiggy_db.clean_sch.restaurant_stm 
on table clean_sch.restaurant
comment = 'Stream on the clean restaurant table to track insert, update, and delete changes';
        
-- here is the merge statement
MERGE INTO swiggy_db.clean_sch.restaurant AS target
USING (
    SELECT 
        try_cast(restaurant_id AS number) AS restaurant_id,
        try_cast(name AS string) AS name,
        try_cast(cuisine_type AS string) AS cuisine_type,
        try_cast(pricing_for_2 AS number(10, 2)) AS pricing_for_two,
        try_cast(restaurant_phone AS string) AS restaurant_phone,
        try_cast(operating_hours AS string) AS operating_hours,
        try_cast(location_id AS number) AS location_id_fk,
        try_cast(active_flag AS string) AS active_flag,
        try_cast(open_status AS string) AS open_status,
        try_cast(locality AS string) AS locality,
        try_cast(restaurant_address AS string) AS restaurant_address,
        try_cast(Ratings as float) as ratings,
        try_cast(latitude AS number(9, 6)) AS latitude,
        try_cast(longitude AS number(9, 6)) AS longitude,
        TO_TIMESTAMP_TZ(Created_Date, 'YYYY-MM-DD HH24:MI:SS') AS created_date,
        TO_TIMESTAMP_TZ(Modified_Date, 'YYYY-MM-DD HH24:MI:SS') AS modified_date,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5
    FROM 
        swiggy_db.stage_sch.restaurant
) AS source
ON target.restaurant_id = source.restaurant_id
WHEN MATCHED THEN 
    UPDATE SET 
        target.name = source.name,
        target.cuisine_type = source.cuisine_type,
        target.pricing_for_two = source.pricing_for_two,
        target.restaurant_phone = source.restaurant_phone,
        target.operating_hours = source.operating_hours,
        target.location_id_fk = source.location_id_fk,
        target.active_flag = source.active_flag,
        target.open_status = source.open_status,
        target.locality = source.locality,
        target.restaurant_address = source.restaurant_address,
        target.ratings=source.Ratings,
        target.latitude = source.latitude,
        target.longitude = source.longitude,
        target.created_date = source.created_date,
        target.modified_date = source.modified_date,
        target._stg_file_name = source._stg_file_name,
        target._stg_file_load_ts = source._stg_file_load_ts,
        target._stg_file_md5 = source._stg_file_md5
WHEN NOT MATCHED THEN 
    INSERT (
        restaurant_id,
        name,
        cuisine_type,
        pricing_for_two,
        restaurant_phone,
        operating_hours,
        location_id_fk,
        active_flag,
        open_status,
        locality,
        restaurant_address,
        Ratings,
        latitude,
        longitude,
        created_date,
        modified_date,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5
    )
    VALUES (
        source.restaurant_id,
        source.name,
        source.cuisine_type,
        source.pricing_for_two,
        source.restaurant_phone,
        source.operating_hours,
        source.location_id_fk,
        source.active_flag,
        source.open_status,
        source.locality,
        source.restaurant_address,
        source.Ratings,
        source.latitude,
        source.longitude,
        source.created_date,
        source.modified_date,
        source._stg_file_name,
        source._stg_file_load_ts,
        source._stg_file_md5
    );

select * from clean_sch.restaurant;


-- defining dimension table for restaurant.
CREATE OR REPLACE TABLE CONSUMPTION_SCH.RESTAURANT_DIM (
    RESTAURANT_HK NUMBER primary key,       -- Hash key for the restaurant location
    RESTAURANT_ID NUMBER,                    
    NAME STRING(100),                        
    CUISINE_TYPE STRING,                     
    PRICING_FOR_TWO NUMBER(10, 2),           
    RESTAURANT_PHONE STRING(15),             
    OPERATING_HOURS STRING(100),             
    LOCATION_ID_FK NUMBER,                  -- Foreign key refering to location table
    ACTIVE_FLAG STRING(10),                 -- Indicates if the restaurant is active
    OPEN_STATUS STRING(10),                 -- Indicates if the restaurant is currently open
    LOCALITY STRING(100),                    
    RESTAURANT_ADDRESS STRING,               
    RATINGS FLOAT,                           
    LATITUDE NUMBER(9, 6),                   
    LONGITUDE NUMBER(9, 6),                  
    EFF_START_DATE TIMESTAMP_TZ,             
    EFF_END_DATE TIMESTAMP_TZ,               
    IS_CURRENT BOOLEAN                      -- Indicates the status of current version of the record
)
COMMENT = 'Dimension table for Restaurant entity with hash keys and SCD enabled.';


-- merge statement  for data insertion and scd2
MERGE INTO 
    CONSUMPTION_SCH.RESTAURANT_DIM AS target
USING 
    CLEAN_SCH.RESTAURANT_STM AS source
ON 
    target.RESTAURANT_ID = source.RESTAURANT_ID AND 
    target.NAME = source.NAME AND 
    target.CUISINE_TYPE = source.CUISINE_TYPE AND 
    target.PRICING_FOR_TWO = source.PRICING_FOR_TWO AND 
    target.RESTAURANT_PHONE = source.RESTAURANT_PHONE AND 
    target.OPERATING_HOURS = source.OPERATING_HOURS AND 
    target.LOCATION_ID_FK = source.LOCATION_ID_FK AND 
    target.ACTIVE_FLAG = source.ACTIVE_FLAG AND 
    target.OPEN_STATUS = source.OPEN_STATUS AND 
    target.LOCALITY = source.LOCALITY AND 
    target.RESTAURANT_ADDRESS = source.RESTAURANT_ADDRESS AND 
    target.Ratings=source.Ratings AND
    target.LATITUDE = source.LATITUDE AND 
    target.LONGITUDE = source.LONGITUDE
WHEN MATCHED 
    AND source.METADATA$ACTION = 'DELETE' AND source.METADATA$ISUPDATE = 'TRUE' THEN
    -- Update the existing record to close its validity period
    UPDATE SET 
        target.EFF_END_DATE = CURRENT_TIMESTAMP(),
        target.IS_CURRENT = FALSE
WHEN NOT MATCHED 
    AND source.METADATA$ACTION = 'INSERT' AND source.METADATA$ISUPDATE = 'TRUE' THEN
    -- Insert new record with current data and new effective start date
    INSERT (
        RESTAURANT_HK,
        RESTAURANT_ID,
        NAME,
        CUISINE_TYPE,
        PRICING_FOR_TWO,
        RESTAURANT_PHONE,
        OPERATING_HOURS,
        LOCATION_ID_FK,
        ACTIVE_FLAG,
        OPEN_STATUS,
        LOCALITY,
        RESTAURANT_ADDRESS,
        Ratings,
        LATITUDE,
        LONGITUDE,
        EFF_START_DATE,
        EFF_END_DATE,
        IS_CURRENT
    )
    VALUES (
        hash(SHA1_hex(CONCAT(source.RESTAURANT_ID, source.NAME, source.CUISINE_TYPE, 
            source.PRICING_FOR_TWO, source.RESTAURANT_PHONE, source.OPERATING_HOURS, 
            source.LOCATION_ID_FK, source.ACTIVE_FLAG, source.OPEN_STATUS, source.LOCALITY, 
            source.RESTAURANT_ADDRESS, source.LATITUDE, source.LONGITUDE))),
        source.RESTAURANT_ID,
        source.NAME,
        source.CUISINE_TYPE,
        source.PRICING_FOR_TWO,
        source.RESTAURANT_PHONE,
        source.OPERATING_HOURS,
        source.LOCATION_ID_FK,
        source.ACTIVE_FLAG,
        source.OPEN_STATUS,
        source.LOCALITY,
        source.RESTAURANT_ADDRESS,
        source.Ratings,
        source.LATITUDE,
        source.LONGITUDE,
        CURRENT_TIMESTAMP(),
        NULL,
        TRUE
    )
WHEN NOT MATCHED 
    AND source.METADATA$ACTION = 'INSERT' AND source.METADATA$ISUPDATE = 'FALSE' THEN
    -- Insert new record with current data and new effective start date
    INSERT (
        RESTAURANT_HK,
        RESTAURANT_ID,
        NAME,
        CUISINE_TYPE,
        PRICING_FOR_TWO,
        RESTAURANT_PHONE,
        OPERATING_HOURS,
        LOCATION_ID_FK,
        ACTIVE_FLAG,
        OPEN_STATUS,
        LOCALITY,
        RESTAURANT_ADDRESS,
        Ratings,
        LATITUDE,
        LONGITUDE,
        EFF_START_DATE,
        EFF_END_DATE,
        IS_CURRENT
    )
    VALUES (
        hash(SHA1_hex(CONCAT(source.RESTAURANT_ID, source.NAME, source.CUISINE_TYPE, 
            source.PRICING_FOR_TWO, source.RESTAURANT_PHONE, source.OPERATING_HOURS, 
            source.LOCATION_ID_FK, source.ACTIVE_FLAG, source.OPEN_STATUS, source.LOCALITY, 
            source.RESTAURANT_ADDRESS, source.LATITUDE, source.LONGITUDE))),
        source.RESTAURANT_ID,
        source.NAME,
        source.CUISINE_TYPE,
        source.PRICING_FOR_TWO,
        source.RESTAURANT_PHONE,
        source.OPERATING_HOURS,
        source.LOCATION_ID_FK,
        source.ACTIVE_FLAG,
        source.OPEN_STATUS,
        source.LOCALITY,
        source.RESTAURANT_ADDRESS,
        source.Ratings,
        source.LATITUDE,
        source.LONGITUDE,
        source.CREATED_DATE,
        NULL,
        TRUE
    );

select * from consumption_sch.restaurant_dim
limit 20;
