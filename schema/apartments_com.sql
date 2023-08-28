CREATE TABLE IF NOT EXISTS APARTMENTS_PROPERTY (
    URL VARCHAR NOT NULL,
    SITE_LAST_MOD VARCHAR,
    HTML_CONTENTS VARCHAR,
    LAST_DOWNLOADED VARCHAR,
    POPULATION BIGINT
    CONSTRAINT URL_PK PRIMARY KEY (URL));

CREATE TABLE IF NOT EXISTS TAX_INFO (
    PARCEL_ID VARCHAR NOT NULL,
    COUNTY VARCHAR,
    HTML_CONTENTS VARCHAR,
    LAST_DOWNLOADED VARCHAR,
    USE_TYPE VARCHAR,
    CONSTRAINT TAX_INFO_PK PRIMARY KEY (COUNTY, PARCEL_ID));

CREATE TABLE IF NOT EXISTS TAX_INFO_STATUS (
    PARCEL_ID VARCHAR NOT NULL,
    COUNTY VARCHAR,
    CURRENT_TAX_BILL FLOAT,
    DELINQUENT_TAX_BILL FLOAT,
    CONSTRAINT TAX_INFO_STATUS_PK PRIMARY KEY (COUNTY, PARCEL_ID));


CREATE TABLE IF NOT EXISTS PARCEL_INFO (
    PARCEL_ID VARCHAR NOT NULL,
    COUNTY VARCHAR,
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
