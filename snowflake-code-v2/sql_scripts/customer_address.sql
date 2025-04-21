use role sysadmin;
use database SWIGGY_DB;
use schema stage_sch;
use warehouse adhoc_wh;

create or replace table stage_sch.customeraddress (
    address_id text,
    customer_id text comment 'Customer FK (Source Data)',     -- foreign key reference as text (no constraint in snowflake)
    flat_or_house_no text,
    floor text,
    building text,
    landmark text,
    locality text,
    city text, 
    state text,
    pincode text, 
    coordinates text, 
    primary_flag text,
    address_type text, 
    created_date text, 
    modified_date text, 

    -- audit columns with appropriate data types
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
comment = 'This is the customer address stage/raw table where data will be copied from internal stage using copy command. This is as-is data represetation from the source location. All the columns are text data type except the audit columns that are added for traceability.';

create or replace stream stage_sch.customeraddress_stm 
on table stage_sch.customeraddress
append_only = true
comment = 'This is the append-only stream object on customer address table that only gets delta data';

select * from stage_sch.customeraddress_stm;

copy into stage_sch.customeraddress (address_id, customer_id, flat_or_house_no , floor, building, 
                               landmark, locality,city,state, pincode, coordinates, primary_flag, address_type, 
                               created_date, modified_date, 
                               _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
from (
    select 
        t.$1::text as address_id,
        t.$2::text as customer_id,
        t.$3::text as flat_or_house_no,
        t.$4::text as floor,
        t.$5::text as building,
        t.$6::text as landmark,
        t.$7::text as locality,
        t.$8::text as city,
        t.$9::text as State,
        t.$10::text as Pincode,
        t.$11::text as coordinates,
        t.$12::text as primary_flag,
        t.$13::text as address_type,
        t.$14::text as created_date,
        t.$15::text as modified_date,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
    from @swiggy_db.STAGE_SCH.AWS_S3_STAGE/2025/4/9/customer_address.csv t
)
file_format = (format_name = 'stage_sch.csv_file_format')
on_error = abort_statement;

select * from stage_sch.customeraddress;

-- 2nd layer
CREATE OR REPLACE TABLE CLEAN_SCH.CUSTOMER_ADDRESS (
    CUSTOMER_ADDRESS_SK NUMBER AUTOINCREMENT PRIMARY KEY comment 'Surrogate Key (EWH)',
    ADDRESS_ID INT comment 'Primary Key (Source Data)',
    CUSTOMER_ID_FK INT comment 'Customer FK (Source Data)',      -- Foreign key reference as string (no constraint in Snowflake)
    flat_or_house_no STRING,
    FLOOR STRING,
    BUILDING STRING,
    LANDMARK STRING,
    locality STRING,
    CITY STRING,
    STATE STRING,
    PINCODE NUMBER,
    COORDINATES STRING,
    PRIMARY_FLAG STRING,
    ADDRESS_TYPE STRING,
    CREATED_DATE TIMESTAMP_TZ,
    MODIFIED_DATE TIMESTAMP_TZ,

    -- Audit columns with appropriate data types
    _STG_FILE_NAME STRING,
    _STG_FILE_LOAD_TS TIMESTAMP,
    _STG_FILE_MD5 STRING,
    _COPY_DATA_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
comment = 'Customer address entity under clean schema with appropriate data type under clean schema layer, data is populated using merge statement from the stage layer location table. This table does not support SCD2';

-- Stream object to capture the changes. 
create or replace stream CLEAN_SCH.CUSTOMER_ADDRESS_STM
on table CLEAN_SCH.CUSTOMER_ADDRESS
comment = 'This is the stream object on customer address entity to track insert, update, and delete changes';

MERGE INTO clean_sch.customer_address AS clean
USING (
    SELECT 
        CAST(address_id AS INT) AS address_id,
        CAST(customer_id AS INT) AS customer_id_fk,
        flat_or_house_no AS flat_or_house_no,
        floor,
        building,
        landmark,
        locality,
        city,
        state,
        pincode,
        coordinates,
        primary_flag AS primary_flag,
        address_type AS address_type,
        TO_TIMESTAMP_TZ(created_date, 'YYYY-MM-DD HH24:MI:SS') AS created_date,
        TO_TIMESTAMP_TZ(modified_date, 'YYYY-MM-DD HH24:MI:SS') AS modified_date,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    FROM stage_sch.customeraddress_stm 
) AS stage
ON clean.address_id = stage.address_id
-- Insert new records
WHEN NOT MATCHED THEN
    INSERT (
        address_id,
        customer_id_fk,
        flat_or_house_no,
        floor,
        building,
        landmark,
        locality,
        city,
        state,
        pincode,
        coordinates,
        primary_flag,
        address_type,
        created_date,
        modified_date,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    )
    VALUES (
        stage.address_id,
        stage.customer_id_fk,
        stage.flat_or_house_no,
        stage.floor,
        stage.building,
        stage.landmark,
        stage.locality,
        stage.city,
        stage.state,
        stage.pincode,
        stage.coordinates,
        stage.primary_flag,
        stage.address_type,
        stage.created_date,
        stage.modified_date,
        stage._stg_file_name,
        stage._stg_file_load_ts,
        stage._stg_file_md5,
        stage._copy_data_ts
    )
-- Update existing records
WHEN MATCHED THEN
    UPDATE SET
        clean.flat_or_house_no = stage.flat_or_house_no,
        clean.floor = stage.floor,
        clean.building = stage.building,
        clean.landmark = stage.landmark,
        clean.locality = stage.locality,
        clean.city = stage.city,
        clean.state = stage.state,
        clean.pincode = stage.pincode,
        clean.coordinates = stage.coordinates,
        clean.primary_flag = stage.primary_flag,
        clean.address_type = stage.address_type,
        clean.created_date = stage.created_date,
        clean.modified_date = stage.modified_date,
        clean._stg_file_name = stage._stg_file_name,
        clean._stg_file_load_ts = stage._stg_file_load_ts,
        clean._stg_file_md5 = stage._stg_file_md5,
        clean._copy_data_ts = stage._copy_data_ts;

select * from clean_sch.customer_address;

CREATE OR REPLACE TABLE CONSUMPTION_SCH.CUSTOMER_ADDRESS_DIM (
    CUSTOMER_ADDRESS_HK NUMBER PRIMARY KEY comment 'Customer Address HK (EDW)',     -- Surrogate key (hash key)
    ADDRESS_ID INT comment 'Primary Key (Source System)',
    CUSTOMER_ID_FK STRING comment 'Customer FK (Source System)',                    -- Surrogate key from Customer Dimension (Foreign Key)
   flat_or_house_no STRING,
    FLOOR STRING,
    BUILDING STRING,
    LANDMARK STRING,
    LOCALITY STRING,
    CITY STRING,
    STATE STRING,
    PINCODE STRING,
    COORDINATES STRING,
    PRIMARY_FLAG STRING,
    ADDRESS_TYPE STRING,

    -- SCD2 Columns
    EFF_START_DATE TIMESTAMP_TZ,                                 -- Effective start date
    EFF_END_DATE TIMESTAMP_TZ,                                   -- Effective end date (NULL if active)
    IS_CURRENT BOOLEAN                                           -- Flag to indicate the current record
);

MERGE INTO 
    CONSUMPTION_SCH.CUSTOMER_ADDRESS_DIM AS target
USING 
    CLEAN_SCH.CUSTOMER_ADDRESS_STM AS source
ON 
    target.ADDRESS_ID = source.ADDRESS_ID AND
    target.CUSTOMER_ID_FK = source.CUSTOMER_ID_FK AND
    target.flat_or_house_no = source.flat_or_house_no AND
    target.FLOOR = source.FLOOR AND
    target.BUILDING = source.BUILDING AND
    target.LANDMARK = source.LANDMARK AND
    target.LOCALITY = source.LOCALITY AND
    target.CITY = source.CITY AND
    target.STATE = source.STATE AND
    target.PINCODE = source.PINCODE AND
    target.COORDINATES = source.COORDINATES AND
    target.PRIMARY_FLAG = source.PRIMARY_FLAG AND
    target.ADDRESS_TYPE = source.ADDRESS_TYPE
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
        CUSTOMER_ADDRESS_HK,
        ADDRESS_ID,
        CUSTOMER_ID_FK,
        flat_or_house_no,
        FLOOR,
        BUILDING,
        LANDMARK,
        LOCALITY,
        CITY,
        STATE,
        PINCODE,
        COORDINATES,
        PRIMARY_FLAG,
        ADDRESS_TYPE,
        EFF_START_DATE,
        EFF_END_DATE,
        IS_CURRENT
    )
    VALUES (
        hash(SHA1_hex(CONCAT(source.ADDRESS_ID, source.CUSTOMER_ID_FK, source.flat_or_house_no, 
            source.FLOOR, source.BUILDING, source.LANDMARK, 
            source.LOCALITY, source.CITY, source.STATE, source.PINCODE, 
            source.COORDINATES, source.PRIMARY_FLAG, source.ADDRESS_TYPE))),
        source.ADDRESS_ID,
        source.CUSTOMER_ID_FK,
        source.flat_or_house_no,
        source.FLOOR,
        source.BUILDING,
        source.LANDMARK,
        source.LOCALITY,
        source.CITY,
        source.STATE,
        source.PINCODE,
        source.COORDINATES,
        source.PRIMARY_FLAG,
        source.ADDRESS_TYPE,
        CURRENT_TIMESTAMP(),
        NULL,
        TRUE
    )
WHEN NOT MATCHED 
    AND source.METADATA$ACTION = 'INSERT' AND source.METADATA$ISUPDATE = 'FALSE' THEN
    -- Insert new record with current data and new effective start date
    INSERT (
        CUSTOMER_ADDRESS_HK,
        ADDRESS_ID,
        CUSTOMER_ID_FK,
        flat_or_house_no,
        FLOOR,
        BUILDING,
        LANDMARK,
        LOCALITY,
        CITY,
        STATE,
        PINCODE,
        COORDINATES,
        PRIMARY_FLAG,
        ADDRESS_TYPE,
        EFF_START_DATE,
        EFF_END_DATE,
        IS_CURRENT
    )
    VALUES (
        hash(SHA1_hex(CONCAT(source.ADDRESS_ID, source.CUSTOMER_ID_FK, source.flat_or_house_no, 
              source.FLOOR, source.BUILDING, source.LANDMARK, 
            source.LOCALITY, source.CITY, source.STATE, source.PINCODE, 
            source.COORDINATES, source.PRIMARY_FLAG, source.ADDRESS_TYPE))),
        source.ADDRESS_ID,
        source.CUSTOMER_ID_FK,
        source.flat_or_house_no,
        source.FLOOR,
        source.BUILDING,
        source.LANDMARK,
        source.LOCALITY,
        source.CITY,
        source.STATE,
        source.PINCODE,
        source.COORDINATES,
        source.PRIMARY_FLAG,
        source.ADDRESS_TYPE,
        source.CREATED_DATE,
        NULL,
        TRUE
    );

select * from CONSUMPTION_SCH.CUSTOMER_ADDRESS_DIM
limit 100;