use role sysadmin;
use database swiggy_db;
use schema stage_sch;
use warehouse adhoc_wh;

-- staging area code 
create or replace table swiggy_db.stage_sch.location (
    locationid text,
    city text,
    state text,
    zipcode text,
    activeflag text,
    createddate text,
    modifieddate text,
    -- audit columns for tracking & debugging
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
comment = 'This is the location stage/raw table where data will be copied from internal stage using copy command. This is as-is data represetation from the source location. All the columns are text data type except the audit columns that are added for traceability.'
;

create or replace stream swiggy_db.stage_sch.location_stm 
on table stage_sch.location
append_only = true
comment = 'this is the append-only stream object on location table';

copy into swiggy_db.stage_sch.location 
from (select $1 as LocationID,
$2::text as State, 
$3::text as City, 
$4::text as ZipCode,
$5::text as ActiveFlag,
$6::text as CreatedDate,
$7::text as ModifiedDate, 
metadata$filename as _stg_file_name,
metadata$file_last_modified as _stg_file_load_ts,
metadata$file_content_key as _stg_file_md5,
current_timestamp as _copy_data_ts
from @stage_sch.aws_s3_stage/2025/4/9/location.csv
)
file_format=(format_name= 'SWIGGY_DB.STAGE_SCH.CSV_FILE_FORMAT');

select * from swiggy_db.stage_sch.location_stm;

-- Level 2
create or replace table swiggy_db.clean_sch.location (
    location_sk number autoincrement primary key,
    locationid number not null unique,
    city string(100) not null,
    state string(100) not null,
    state_code string(2) not null,
    is_union_territory boolean not null default false,
    capital_city_flag boolean not null default false,
    city_tier text(6),
    zipcode string(10) not null,
    activeflag string(10) not null,
    createddate timestamp_tz not null,
    modifieddate timestamp_tz,
    
    -- additional audit columns
    _stg_file_name string,
    _stg_file_load_ts timestamp_ntz,
    _stg_file_md5 string,
    _copy_data_ts timestamp_ntz default current_timestamp
)
comment = 'Location entity under clean schema with appropriate data type under clean schema layer, data is populated using merge statement from the stage layer location table. This table does not support SCD2';

create or replace stream swiggy_db.clean_sch.location_stm 
on table clean_sch.location
comment = 'this is a standard stream object on the location table to track insert, update, and delete changes';

MERGE INTO swiggy_db.clean_sch.location AS target
USING (
    SELECT 
        CAST(LocationID AS NUMBER) AS LocationID,
        CAST(City AS STRING) AS City,
        CASE 
            WHEN CAST(State AS STRING) = 'Delhi' THEN 'New Delhi'
            ELSE CAST(State AS STRING)
        END AS State,
        -- State Code Mapping
        CASE 
            WHEN State = 'Delhi' THEN 'DL'
            WHEN State = 'Maharashtra' THEN 'MH'
            WHEN State = 'Uttar Pradesh' THEN 'UP'
            WHEN State = 'Gujarat' THEN 'GJ'
            WHEN State = 'Rajasthan' THEN 'RJ'
            WHEN State = 'Kerala' THEN 'KL'
            WHEN State = 'Punjab' THEN 'PB'
            WHEN State = 'Karnataka' THEN 'KA'
            WHEN State = 'Madhya Pradesh' THEN 'MP'
            WHEN State = 'Odisha' THEN 'OR'
            WHEN State = 'Chandigarh' THEN 'CH'
            WHEN State = 'West Bengal' THEN 'WB'
            WHEN State = 'Sikkim' THEN 'SK'
            WHEN State = 'Andhra Pradesh' THEN 'AP'
            WHEN State = 'Assam' THEN 'AS'
            WHEN State = 'Jammu and Kashmir' THEN 'JK'
            WHEN State = 'Puducherry' THEN 'PY'
            WHEN State = 'Uttarakhand' THEN 'UK'
            WHEN State = 'Himachal Pradesh' THEN 'HP'
            WHEN State = 'Tamil Nadu' THEN 'TN'
            WHEN State = 'Goa' THEN 'GA'
            WHEN State = 'Telangana' THEN 'TG'
            WHEN State = 'Chhattisgarh' THEN 'CG'
            WHEN State = 'Jharkhand' THEN 'JH'
            WHEN State = 'Bihar' THEN 'BR'
            WHEN State = 'Haryana' THEN 'HR'
            ELSE NULL
        END AS state_code,
        CASE
            -- change the state to city
            WHEN State IN ('Delhi','Jammu and Kashmir','Andaman and Nicobar','Lakshadweep') THEN 'Y'
            WHEN City IN ('Delhi', 'Chandigarh', 'Puducherry','Andaman and Nicobar','Daman and                                              Diu','Daman','Diu','Lakshadweep','Ladakh') THEN 'Y'
            ELSE 'N'
        END AS is_union_territory,
        CASE
            WHEN (State= 'New Delhi' AND City = 'Delhi') THEN TRUE
            WHEN (State = 'Andhra Pradesh' AND City = 'Amaravati') THEN TRUE
            WHEN (State = 'Arunachal Pradesh' AND City = 'Itanagar') THEN TRUE
            WHEN (State = 'Assam' AND City = 'Dispur') THEN TRUE
            WHEN (State = 'Bihar' AND City = 'Patna') THEN TRUE
            WHEN (State = 'Chhattisgarh' AND City = 'Raipur') THEN TRUE
            WHEN (State = 'Goa' AND City = 'Panaji') THEN TRUE
            WHEN (State = 'Gujarat' AND City = 'Gandhinagar') THEN TRUE
            WHEN (State = 'Haryana' AND City = 'Chandigarh') THEN TRUE
            WHEN (State = 'Himachal Pradesh' AND City = 'Shimla') THEN TRUE
            WHEN (State = 'Jharkhand' AND City = 'Ranchi') THEN TRUE
            WHEN (State = 'Karnataka' AND City = 'Bengaluru') THEN TRUE
            WHEN (State = 'Kerala' AND City = 'Thiruvananthapuram') THEN TRUE
            WHEN (State = 'Madhya Pradesh' AND City = 'Bhopal') THEN TRUE
            WHEN (State = 'Maharashtra' AND City = 'Mumbai') THEN TRUE
            WHEN (State = 'Manipur' AND City = 'Imphal') THEN TRUE
            WHEN (State = 'Meghalaya' AND City = 'Shillong') THEN TRUE
            WHEN (State = 'Mizoram' AND City = 'Aizawl') THEN TRUE
            WHEN (State = 'Nagaland' AND City = 'Kohima') THEN TRUE
            WHEN (State = 'Odisha' AND City = 'Bhubaneswar') THEN TRUE
            WHEN (State = 'Punjab' AND City = 'Chandigarh') THEN TRUE
            WHEN (State = 'Rajasthan' AND City = 'Jaipur') THEN TRUE
            WHEN (State = 'Sikkim' AND City = 'Gangtok') THEN TRUE
            WHEN (State = 'Tamil Nadu' AND City = 'Chennai') THEN TRUE
            WHEN (State = 'Telangana' AND City = 'Hyderabad') THEN TRUE
            WHEN (State = 'Tripura' AND City = 'Agartala') THEN TRUE
            WHEN (State = 'Uttar Pradesh' AND City = 'Lucknow') THEN TRUE
            WHEN (State = 'Uttarakhand' AND City = 'Dehradun') THEN TRUE
            WHEN (State = 'West Bengal' AND City = 'Kolkata') THEN TRUE
            ELSE FALSE
        END AS capital_city_flag,
        CASE 
            WHEN City IN ('Mumbai', 'Delhi', 'Bengaluru', 'Hyderabad', 'Chennai', 'Kolkata', 'Pune', 'Ahmedabad')                           THEN 'Tier-1'
            WHEN City IN ('Jaipur', 'Lucknow', 'Kanpur', 'Nagpur', 'Indore', 'Bhopal', 'Patna', 'Vadodara',                                 'Coimbatore', 'Ludhiana', 'Agra', 'Nashik', 'Ranchi', 'Meerut', 'Raipur', 'Guwahati',                             'Chandigarh') THEN 'Tier-2'
            ELSE 'Tier-3'
        END AS city_tier,
        CAST(ZipCode AS STRING) AS ZipCode,
        CAST(ActiveFlag AS STRING) AS ActiveFlag,
        TO_TIMESTAMP_TZ(CreatedDate, 'YYYY-MM-DD HH24:MI:SS') AS createddate,
        TO_TIMESTAMP_TZ(ModifiedDate, 'YYYY-MM-DD HH24:MI:SS') AS modifieddate,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        CURRENT_TIMESTAMP AS _copy_data_ts
    FROM stage_sch.location_stm where locationid is not null
) AS source
ON target.LocationID = source.LocationID
WHEN MATCHED AND (
    target.City != source.City OR
    target.State != source.State OR
    target.state_code != source.state_code OR
    target.is_union_territory != source.is_union_territory OR
    target.capital_city_flag != source.capital_city_flag OR
    target.city_tier != source.city_tier OR
    target.ZipCode != source.ZipCode OR
    target.ActiveFlag != source.ActiveFlag OR
    target.modifieddate != source.modifieddate
) THEN 
    UPDATE SET 
        target.City = source.City,
        target.State = source.State,
        target.state_code = source.state_code,
        target.is_union_territory = source.is_union_territory,
        target.capital_city_flag = source.capital_city_flag,
        target.city_tier = source.city_tier,
        target.ZipCode = source.ZipCode,
        target.ActiveFlag = source.ActiveFlag,
        target.modifieddate = source.modifieddate,
        target._stg_file_name = source._stg_file_name,
        target._stg_file_load_ts = source._stg_file_load_ts,
        target._stg_file_md5 = source._stg_file_md5,
        target._copy_data_ts = source._copy_data_ts
WHEN NOT MATCHED THEN
    INSERT (
        LocationID,
        City,
        State,
        state_code,
        is_union_territory,
        capital_city_flag,
        city_tier,
        ZipCode,
        ActiveFlag,
        createddate,
        modifieddate,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
    )
    VALUES (
        source.LocationID,
        source.City,
        source.State,
        source.state_code,
        source.is_union_territory,
        source.capital_city_flag,
        source.city_tier,
        source.ZipCode,
        source.ActiveFlag,
        source.createddate,
        source.modifieddate,
        source._stg_file_name,
        source._stg_file_load_ts,
        source._stg_file_md5,
        source._copy_data_ts
    );

select * from clean_sch.location;

select * from clean_sch.location_stm;

-- consumption area
create or replace table swiggy_db.consumption_sch.location_dim (
    location_hk NUMBER primary key,                    -- hash key for the dimension
    locationid number(38,0) not null,                  -- business key
    city varchar(100) not null,                        -- city
    state varchar(100) not null,                       -- state
    state_code varchar(2) not null,                    -- state code
    is_union_territory boolean not null default false, -- union territory flag
    capital_city_flag boolean not null default false,  -- capital city flag
    city_tier varchar(6),                              -- city tier
    zipcode varchar(10) not null,                      -- zip code
    activeflag varchar(10) not null,                   -- active flag (indicating current record)
    eff_start_dt timestamp_tz(9) not null,             -- effective start date for scd2
    eff_end_dt timestamp_tz(9),                        -- effective end date for scd2
    current_flag boolean not null default true         -- indicator of the current record
)
comment = 'Dimension table for restaurant location with scd2 (slowly changing dimension) enabled and hashkey as surrogate key';

select * from swiggy_db.clean_sch.location_STM;

MERGE INTO 
        swiggy_db.CONSUMPTION_SCH.location_DIM AS target
    USING 
        swiggy_db.CLEAN_SCH.location_stm AS source
    ON 
        target.LOCATIONID = source.LOCATIONID and 
        target.ACTIVEFLAG = source.ACTIVEFLAG
    WHEN MATCHED 
        AND source.METADATA$ACTION = 'DELETE' and source.METADATA$ISUPDATE = 'TRUE' 
    THEN
        -- Update the existing record to close its validity period
        UPDATE SET 
            target.EFF_END_DT = CURRENT_TIMESTAMP(),
            target.CURRENT_FLAG = FALSE
    WHEN NOT MATCHED 
        AND source.METADATA$ACTION = 'INSERT' and source.METADATA$ISUPDATE = 'TRUE'
    THEN
        -- Insert new record with current data and new effective start date
        INSERT (
            location_HK,
            LOCATIONID,
            CITY,
            STATE,
            STATE_CODE,
            IS_UNION_TERRITORY,
            CAPITAL_CITY_FLAG,
            CITY_TIER,
            ZIPCODE,
            ACTIVEFLAG,
            EFF_START_DT,
            EFF_END_DT,
            CURRENT_FLAG
        )
        VALUES (
            hash(SHA1_hex(CONCAT(source.CITY, source.STATE, source.STATE_CODE, source.ZIPCODE))),
            source.LOCATIONID,
            source.CITY,
            source.STATE,
            source.STATE_CODE,
            source.IS_UNION_TERRITORY,
            source.CAPITAL_CITY_FLAG,
            source.CITY_TIER,
            source.ZIPCODE,
            source.ACTIVEFLAG,
            CURRENT_TIMESTAMP(),
            NULL,
            TRUE
        )
    WHEN NOT MATCHED 
        AND source.METADATA$ACTION = 'INSERT' and source.METADATA$ISUPDATE = 'FALSE' 
    THEN
        -- Insert new record with current data and new effective start date
        INSERT (
            location_HK,
            LOCATIONID,
            CITY,
            STATE,
            STATE_CODE,
            IS_UNION_TERRITORY,
            CAPITAL_CITY_FLAG,
            CITY_TIER,
            ZIPCODE,
            ACTIVEFLAG,
            EFF_START_DT,
            EFF_END_DT,
            CURRENT_FLAG
        )
        VALUES (
            hash(SHA1_hex(CONCAT(source.CITY, source.STATE, source.STATE_CODE, source.ZIPCODE))),
            source.LOCATIONID,
            source.CITY,
            source.STATE,
            source.STATE_CODE,
            source.IS_UNION_TERRITORY,
            source.CAPITAL_CITY_FLAG,
            source.CITY_TIER,
            source.ZIPCODE,
            source.ACTIVEFLAG,
            source.CREATEDDATE,
            NULL,
            TRUE
        );

select * from swiggy_db.consumption_sch.location_dim
limit 100;