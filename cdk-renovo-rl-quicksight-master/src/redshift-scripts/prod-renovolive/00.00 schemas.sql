-- ref: https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_EXTERNAL_SCHEMA.html
-- Creates an external schema in Redshift that references the Glue Data Catalog
-- Available database tables are crawled from DMS data loaded in S3

CREATE EXTERNAL SCHEMA IF NOT EXISTS renovomaster_spectrum
FROM DATA CATALOG
DATABASE 'prod-renovomaster'
IAM_ROLE 'arn:aws:iam::719035735300:role/ProdRenovoLiveRedshift-roleC7B7E775-qgFqFvU9MLnh'
region 'us-east-1';

CREATE SCHEMA IF NOT EXISTS renovomaster;

CREATE EXTERNAL SCHEMA IF NOT EXISTS clrenovo_spectrum
FROM DATA CATALOG
DATABASE 'prod-clrenovo'
IAM_ROLE 'arn:aws:iam::719035735300:role/ProdRenovoLiveRedshift-roleC7B7E775-qgFqFvU9MLnh'
region 'us-east-1';

CREATE SCHEMA IF NOT EXISTS clrenovo;

CREATE EXTERNAL SCHEMA IF NOT EXISTS cldev_spectrum
FROM DATA CATALOG
DATABASE 'prod-cldev'
IAM_ROLE 'arn:aws:iam::719035735300:role/ProdRenovoLiveRedshift-roleC7B7E775-qgFqFvU9MLnh'
region 'us-east-1';

CREATE SCHEMA IF NOT EXISTS cldev;

CREATE EXTERNAL SCHEMA IF NOT EXISTS clCarle_spectrum
FROM DATA CATALOG
DATABASE 'prod-clCarle'
IAM_ROLE 'arn:aws:iam::719035735300:role/
ProdRenovoLiveRedshift-roleC7B7E775-qgFqFvU9MLnh'
region 'us-east-1';

CREATE SCHEMA IF NOT EXISTS clCarle;

CREATE EXTERNAL SCHEMA IF NOT EXISTS clCentro_spectrum
FROM DATA CATALOG
DATABASE 'prod-clCentro'
IAM_ROLE 'arn:aws:iam::719035735300:role/
ProdRenovoLiveRedshift-roleC7B7E775-qgFqFvU9MLnh'
region 'us-east-1';

CREATE SCHEMA IF NOT EXISTS clCentro;

CREATE EXTERNAL SCHEMA IF NOT EXISTS clChrist_spectrum
FROM DATA CATALOG
DATABASE 'prod-clChrist'
IAM_ROLE 'arn:aws:iam::719035735300:role/
ProdRenovoLiveRedshift-roleC7B7E775-qgFqFvU9MLnh'
region 'us-east-1';

CREATE SCHEMA IF NOT EXISTS clChrist;

CREATE EXTERNAL SCHEMA IF NOT EXISTS cldemo_spectrum
FROM DATA CATALOG
DATABASE 'prod-cldemo'
IAM_ROLE 'arn:aws:iam::719035735300:role/
ProdRenovoLiveRedshift-roleC7B7E775-qgFqFvU9MLnh'
region 'us-east-1';

CREATE SCHEMA IF NOT EXISTS cldemo;

CREATE EXTERNAL SCHEMA IF NOT EXISTS cldecmc_spectrum
FROM DATA CATALOG
DATABASE 'prod-cldecmc'
IAM_ROLE 'arn:aws:iam::719035735300:role/
ProdRenovoLiveRedshift-roleC7B7E775-qgFqFvU9MLnh'
region 'us-east-1';

CREATE SCHEMA IF NOT EXISTS cldecmc;

CREATE EXTERNAL SCHEMA IF NOT EXISTS clradon_spectrum
FROM DATA CATALOG
DATABASE 'prod-clradon'
IAM_ROLE 'arn:aws:iam::719035735300:role/
ProdRenovoLiveRedshift-roleC7B7E775-qgFqFvU9MLnh'
region 'us-east-1';

CREATE SCHEMA IF NOT EXISTS clradon;

CREATE EXTERNAL SCHEMA IF NOT EXISTS clradonva_spectrum
FROM DATA CATALOG
DATABASE 'prod-clradonva'
IAM_ROLE 'arn:aws:iam::719035735300:role/
ProdRenovoLiveRedshift-roleC7B7E775-qgFqFvU9MLnh'
region 'us-east-1';

CREATE SCHEMA IF NOT EXISTS clradonva;

CREATE EXTERNAL SCHEMA IF NOT EXISTS clvertexuk_spectrum
FROM DATA CATALOG
DATABASE 'prod-clvertexuk'
IAM_ROLE 'arn:aws:iam::719035735300:role/
ProdRenovoLiveRedshift-roleC7B7E775-qgFqFvU9MLnh'
region 'us-east-1';

CREATE SCHEMA IF NOT EXISTS clvertexuk;

CREATE EXTERNAL SCHEMA IF NOT EXISTS clzoli_spectrum
FROM DATA CATALOG
DATABASE 'prod-clzoli'
IAM_ROLE 'arn:aws:iam::719035735300:role/
ProdRenovoLiveRedshift-roleC7B7E775-qgFqFvU9MLnh'
region 'us-east-1';

CREATE SCHEMA IF NOT EXISTS clzoli;

CREATE EXTERNAL SCHEMA IF NOT EXISTS metadata_spectrum
FROM DATA CATALOG
DATABASE 'prod-renovolive-metadata'
IAM_ROLE 'arn:aws:iam::719035735300:role/ProdRenovoLiveRedshift-roleC7B7E775-qgFqFvU9MLnh'
region 'us-east-1';

CREATE SCHEMA IF NOT EXISTS metadata;
