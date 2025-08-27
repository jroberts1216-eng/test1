-- ref: https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_EXTERNAL_SCHEMA.html
-- Creates an external schema in Redshift that references the Glue Data Catalog
-- Available database tables are crawled from DMS data loaded in S3

CREATE EXTERNAL SCHEMA IF NOT EXISTS renovomaster_spectrum
FROM DATA CATALOG
DATABASE 'dev-renovomaster'
IAM_ROLE 'arn:aws:iam::719035735300:role/DevRenovoLiveRedshift-roleC7B7E775-I988PNwRIVCE'
region 'us-east-1';

CREATE SCHEMA IF NOT EXISTS renovomaster;

CREATE EXTERNAL SCHEMA IF NOT EXISTS clrenovo_spectrum
FROM DATA CATALOG
DATABASE 'dev-clrenovo'
IAM_ROLE 'arn:aws:iam::719035735300:role/DevRenovoLiveRedshift-roleC7B7E775-I988PNwRIVCE'
region 'us-east-1';

CREATE SCHEMA IF NOT EXISTS clrenovo;

CREATE EXTERNAL SCHEMA IF NOT EXISTS cldev_spectrum
FROM DATA CATALOG
DATABASE 'dev-cldev'
IAM_ROLE 'arn:aws:iam::719035735300:role/DevRenovoLiveRedshift-roleC7B7E775-I988PNwRIVCE'
region 'us-east-1';

CREATE SCHEMA IF NOT EXISTS cldev;

CREATE EXTERNAL SCHEMA IF NOT EXISTS metadata_spectrum
FROM DATA CATALOG
DATABASE 'dev-renovolive-metadata'
IAM_ROLE 'arn:aws:iam::719035735300:role/DevRenovoLiveRedshift-roleC7B7E775-I988PNwRIVCE'
region 'us-east-1';

CREATE SCHEMA IF NOT EXISTS metadata;

CREATE SCHEMA IF NOT EXISTS analytics;

CREATE SCHEMA IF NOT EXISTS admin;