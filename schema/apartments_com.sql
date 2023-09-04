CREATE TABLE IF NOT EXISTS APARTMENTS_PROPERTY (
    COUNTY VARCHAR NOT NULL,
    PARCEL_ID VARCHAR NOT NULL,
    URL VARCHAR,
    HTML_CONTENTS VARCHAR,
    LAST_DOWNLOADED VARCHAR,
    CONSTRAINT URL_PK PRIMARY KEY (COUNTY, PARCEL_ID));

CREATE TABLE IF NOT EXISTS ADDRESS_INFO (
    COUNTY VARCHAR NOT NULL,
    PARCEL_ID VARCHAR NOT NULL,
    USE_TYPE VARCHAR,
    CITY VARCHAR,
    STREET_NAME VARCHAR,
    STREET_NUMBER VARCHAR,
    UNIT_NUMBER VARCHAR,
    ZIPCODE VARCHAR,
    ZIPCODE_EXTENSION VARCHAR,
    URL VARCHAR,
    LAST_DOWNLOADED VARCHAR
    CONSTRAINT URL_PK PRIMARY KEY (COUNTY, PARCEL_ID));

CREATE TABLE IF NOT EXISTS TAX_INFO (
    COUNTY VARCHAR NOT NULL,
    PARCEL_ID VARCHAR NOT NULL,
    HTML_CONTENTS VARCHAR,
    LAST_DOWNLOADED VARCHAR,
    USE_TYPE VARCHAR,
    CONSTRAINT TAX_INFO_PK PRIMARY KEY (COUNTY, PARCEL_ID));

CREATE TABLE IF NOT EXISTS TAX_INFO_STATUS (
    COUNTY VARCHAR NOT NULL,
    PARCEL_ID VARCHAR NOT NULL,
    CURRENT_TAX_BILL FLOAT,
    DELINQUENT_TAX_BILL FLOAT,
    CONSTRAINT TAX_INFO_STATUS_PK PRIMARY KEY (COUNTY, PARCEL_ID));


CREATE TABLE IF NOT EXISTS PARCEL_INFO (
    PARCEL_ID VARCHAR NOT NULL,
    COUNTY VARCHAR NOT NULL,
    CONSTRAINT PARCEL_PK PRIMARY KEY (PARCEL_ID, COUNTY));


CREATE TABLE IF NOT EXISTS ADDRESS_INFO (
    PARCEL_ID VARCHAR NOT NULL,
    COUNTY VARCHAR,
    STREET_NUM VARCHAR,
    UNIT_NUM VARCHAR,
    STREET_NAME VARCHAR,
    CITY VARCHAR,
    ZIP VARCHAR,
    ZIP_EXTENSION VARCHAR,
    USE_CODE VARCHAR,
    USE_TYPE VARCHAR,
    CONSTRAINT PARCEL_PK PRIMARY KEY (COUNTY, PARCEL_ID));


CREATE TABLE IF NOT EXISTS OWNER_INFO (
    OWNER_NAME VARCHAR NOT NULL,
    STREET_ADDRESS VARCHAR,
    CITY VARCHAR,
    STATE VARCHAR,
    UNIT_NUMBER VARCHAR,
    ZIPCODE VARCHAR,
    ZIPCODE_EXTENSION VARCHAR,
    CARE_OF VARCHAR,
    ATTN_NAME VARCHAR,
    CONSTRAINT OWNER_INFO_PK PRIMARY KEY (OWNER_NAME, STREET_ADDRESS, CITY, STATE));


CREATE TABLE IF NOT EXISTS VALUE_INFO (
    COUNTY VARCHAR NOT NULL,
    PARCEL_ID VARCHAR,
    TAXES_LAND_VALUE INTEGER,
    TAXES_IMPROVEMENT_VALUE INTEGER,
    CLCA_LAND_VALUE INTEGER,
    CLCA_IMPROVEMENT_VALUE INTEGER,
    FIXTURES_VALUE INTEGER,
    PERSONAL_PROPERTY_VALUE INTEGER,
    HPP_VALUE INTEGER,
    HOMEOWNERS_EXEMPTION_VALUE INTEGER,
    OTHER_EXEMPTION_VALUE INTEGER,
    NET_TOTAL_VALUE INTEGER,
    LAST_DOC_PREFIX VARCHAR,
    LAST_DOC_SERIES VARCHAR,
    LAST_DOC_DATE VARCHAR,
    LAST_DOC_INPUT_DATE VARCHAR,
    CONSTRAINT PARCEL_PK PRIMARY KEY (COUNTY, PARCEL_ID));


drop table ROLL_INFO;
CREATE TABLE IF NOT EXISTS ROLL_INFO (
    COUNTY VARCHAR NOT NULL,
    PARCEL_ID VARCHAR,
    SOURCE_INFO_DATE VARCHAR,
    USE_TYPE VARCHAR,
    PRI_TRA VARCHAR,
    SEC_TRA VARCHAR,
    ADDRESS_STREET_NUM VARCHAR,
    ADDRESS_STREET_NAME VARCHAR,
    ADDRESS_UNIT_NUM VARCHAR,
    ADDRESS_CITY VARCHAR,
    ADDRESS_ZIP VARCHAR,
    ADDRESS_ZIP_EXTENSION VARCHAR,
    TAXES_LAND_VALUE INTEGER,
    TAXES_IMPROVEMENT_VALUE INTEGER,
    CLCA_LAND_VALUE INTEGER,
    CLCA_IMPROVEMENT_VALUE INTEGER,
    FIXTURES_VALUE INTEGER,
    PERSONAL_PROPERTY_VALUE INTEGER,
    HPP_VALUE INTEGER,
    HOMEOWNERS_EXEMPTION_VALUE INTEGER,
    OTHER_EXEMPTION_VALUE INTEGER,
    NET_TOTAL_VALUE INTEGER,
    LAST_DOC_PREFIX VARCHAR,
    LAST_DOC_SERIES VARCHAR,
    LAST_DOC_DATE VARCHAR,
    LAST_DOC_INPUT_DATE VARCHAR,
    OWNER_NAME VARCHAR,
    MA_CARE_OF VARCHAR,
    MA_ATTN_NAME VARCHAR,
    MA_STREET_ADDRESS VARCHAR,
    MA_UNIT_NUMBER VARCHAR,
    MA_CITY VARCHAR,
    MA_STATE VARCHAR,
    MA_ZIP_CODE VARCHAR,
    MA_ZIP_CODE_EXTENSION VARCHAR,
    MA_BARECODE_WALK_SEQ VARCHAR,
    MA_BARCODE_CHECK_DIGIT VARCHAR,
    MA_EFFECTIVE_DATE VARCHAR,
    MA_SOURCE_CODE VARCHAR,
    USE_CODE VARCHAR,
    ECON_UNIT_FLAG VARCHAR,
    APN_INACTIVE_DATE VARCHAR,
    CONSTRAINT PARCEL_PK PRIMARY KEY (COUNTY, PARCEL_ID, SOURCE_INFO_DATE));

drop table ROLL_INFO_AGG;
CREATE TABLE IF NOT EXISTS ROLL_INFO_AGG (
    COUNTY VARCHAR NOT NULL,
    PARCEL_ID VARCHAR,
    SOURCE_INFO_DATE VARCHAR,
    USE_TYPE_LIST VARCHAR ARRAY,
    PRI_TRA_LIST VARCHAR ARRAY,
    SEC_TRA_LIST VARCHAR ARRAY,
    ADDRESS_STREET_NUM_LIST VARCHAR ARRAY,
    ADDRESS_STREET_NAME_LIST VARCHAR ARRAY,
    ADDRESS_UNIT_NUM_LIST VARCHAR ARRAY,
    ADDRESS_CITY_LIST VARCHAR ARRAY,
    ADDRESS_ZIP_LIST VARCHAR ARRAY,
    ADDRESS_ZIP_EXTENSION_LIST VARCHAR ARRAY,
    TAXES_LAND_VALUE_LIST INTEGER ARRAY,
    TAXES_IMPROVEMENT_VALUE_LIST INTEGER ARRAY,
    CLCA_LAND_VALUE_LIST INTEGER ARRAY,
    CLCA_IMPROVEMENT_VALUE_LIST INTEGER ARRAY,
    FIXTURES_VALUE_LIST INTEGER ARRAY,
    PERSONAL_PROPERTY_VALUE_LIST INTEGER ARRAY,
    HPP_VALUE_LIST INTEGER ARRAY,
    HOMEOWNERS_EXEMPTION_VALUE_LIST INTEGER ARRAY,
    OTHER_EXEMPTION_VALUE_LIST INTEGER ARRAY,
    NET_TOTAL_VALUE_LIST INTEGER ARRAY,
    LAST_DOC_PREFIX_LIST VARCHAR ARRAY,
    LAST_DOC_SERIES_LIST VARCHAR ARRAY,
    LAST_DOC_DATE_LIST VARCHAR ARRAY,
    LAST_DOC_INPUT_DATE_LIST VARCHAR ARRAY,
    OWNER_NAME_LIST VARCHAR ARRAY,
    MA_CARE_OF_LIST VARCHAR ARRAY,
    MA_ATTN_NAME_LIST VARCHAR ARRAY,
    MA_STREET_ADDRESS_LIST VARCHAR ARRAY,
    MA_UNIT_NUMBER_LIST VARCHAR ARRAY,
    MA_CITY_LIST VARCHAR ARRAY,
    MA_STATE_LIST VARCHAR ARRAY,
    MA_ZIP_CODE_LIST VARCHAR ARRAY,
    MA_ZIP_CODE_EXTENSION_LIST VARCHAR ARRAY,
    MA_BARECODE_WALK_SEQ_LIST VARCHAR ARRAY,
    MA_BARCODE_CHECK_DIGIT_LIST VARCHAR ARRAY,
    MA_EFFECTIVE_DATE_LIST VARCHAR ARRAY,
    MA_SOURCE_CODE_LIST VARCHAR ARRAY,
    USE_CODE_LIST VARCHAR ARRAY,
    ECON_UNIT_FLAG_LIST VARCHAR ARRAY,
    APN_INACTIVE_DATE_LIST VARCHAR ARRAY,
    OWNER_NAME_CHANGE BOOLEAN,
    MA_STREET_ADDRESS_CHANGE BOOLEAN,
    MA_CITY_CHANGE BOOLEAN,
    MA_STATE_CHANGE BOOLEAN,
    MA_DIFFERENT_CITY BOOLEAN,
    MA_DIFFERENT_ADDR BOOLEAN,
    CONSTRAINT PARCEL_PK PRIMARY KEY (COUNTY, PARCEL_ID, SOURCE_INFO_DATE));





CREATE TABLE IF NOT EXISTS ROLL_AGG_INFO (
    COUNTY VARCHAR NOT NULL,
    PARCEL_ID VARCHAR,
    USE_TYPE VARCHAR,
    DISTINCT_OWNERS INTEGER,
    DISTINCT_OWNER_ADDRESS INTEGER,
    CONSTRAINT PARCEL_PK PRIMARY KEY (COUNTY, PARCEL_ID, USE_TYPE));



CREATE TABLE IF NOT EXISTS OWNER_NAME_PROPERTY_COUNT (
    OWNER_NAME VARCHAR NOT NULL,
    MA_STREET_ADDRESS VARCHAR,
    MA_CITY_STATE VARCHAR,
    PARCEL_COUNT INTEGER,
    CONSTRAINT OWNER_NAME_COUNT_PK PRIMARY KEY (OWNER_NAME, MA_STREET_ADDRESS, MA_CITY_STATE));





select * from TAX_INFO_STATUS join ROLL_AGG_INFO on
(TAX_INFO_STATUS.COUNTY = ROLL_AGG_INFO.COUNTY AND TAX_INFO_STATUS.PARCEL_ID = ROLL_AGG_INFO.PARCEL_ID)
where CURRENT_TAX_BILL is not null and DELINQUENT_TAX_BILL is not null and (DISTINCT_OWNERS > 1 or DISTINCT_OWNER_ADDRESS > 1)
order by CURRENT_TAX_BILL/ DELINQUENT_TAX_BILL DESC;

!outputformat vertical

