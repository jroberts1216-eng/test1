# CDK Project for QuickSight in RenovoLive

## Working with Projen

### Overview

Please see the `projen` project [[1](https://github.com/projen/projen)] for complete details. We have set some defaults for this project to disable GitHub Actions workflows since we do not support GitHub Actions usage in our org. Files managed by `projen` should include a comment reflecting that. This includes the `package.json`. You should modify the [projen rc file](.projenrc.ts) to modify dependencies, scripts/tasks, etc.

### Stacks

#### QuickSight Configuration ([quicksight.ts](src/quicksight.ts))

This stack contains basic QuickSight configuration. Most importantly it configures QuickSight VPC connectivity that allows QuickSight to interact with resources in our Services VPC.

#### DMS Instance Setup ([dms.ts](src/dms.ts))

Creates the DMS instance and the required VPC connectivity for it to interact with source and destination targets. This instance is a worker instance that talks to a source data system and replicates data to a destination data system. In our case from RenovoLive's MSSQL server to S3 as CSV files.

**Q: Why not serverless?**
**A:** Our compute requirement is too low to justify serverless which carries nearly double the cost of a regular instance for a given capacity. Since we cant go below 1 DCU even when there is no activity we pay a baseline cost that is higher then our current cost for a medium instance with multi-AZ. Because the cost is nearly double per GB of memory we wouldn't see real value from DMS Serverless until we have consistent requirements for extra large capacity instances (or 4 DCUs) during the regular work hours and would see the benefits of scaling down to a single DCU when outside of peak load.

**Q: Why is the instance not created in the same stack as its tasks?**
**A:** We need to maximize the flexibility around how we utilize DMS instances. We may want to combine multiple client DBs under a single replication instance depending on the load. Especially so for development environments or SaaS clients where the load is minimal versus `clRenovo` in prod.

#### DMS Targets and Tasks ([dms.ts](src/dms.ts), [rls3destinationendpoint.ts](src/rls3destinationendpoint.ts), [rlsourceendpoint.ts](src/rlsourceendpoint.ts))

DMS targets utilize some helper constructs for using DMS with RenovoLive targets and destinations. Task configuration uses these along with a helper class (`RLDataExportStacks`) to configure data export to S3 for RenovoLive databases and allocating that workload to DMS compute capacity.

**Q: How is the source database configured to use DMS?**
**A:** The source database uses guidance from AWS on configuration of a DMS users and the permissions it requires in order to access the outputs from change tracking via SQL Server CDC and the usage of backup transaction logs. TODO: More detail here

### Known Areas of Potential Failure

#### DMS Instance Memory Capacity

When completing full load tasks from tables that have a large number of columns we sometimes see sudden drops in available memory. DMS will continue to consume memory until out of memory exceptions occur. There typically look like the following:

- The task stops with no indication in the logs that an error has occurred
- The DMS console specifically states that the task ran out of memory
- The DMS task fails specific tables with errors about the number of expected items in a column and the actual number being mismatched

This can be resolved by confirming the memory metric for the task reflects running out of memory and searching the logs for related errors then adjusting the size of the DMS instance (vertical scaling) to accomodate. You may also need to reduce parallel table loads or split tasks across more DMS instances (horizontal scaling).

We have seen errors even when tables are processed individually, such as SEs, SE notes, and more so reducing the parallel table quantity hasn't been beneficial for us.

### Other guidance

- table leveling https://docs.aws.amazon.com/glue/latest/dg/crawler-table-level.html
- find out what role Redshift Serverless is using by default https://docs.aws.amazon.com/redshift/latest/dg/r_DEFAULT_IAM_ROLE.html
- schema changes for existing tables

### TODO

- Move redshift API calls from lambda functions into Step functions where it makes sense
  - Higher cost (fractions of a cent so not a huge problem), but easier to visualize and understand
- Would it be more beneficial to break out full loading of tables so a full restart doesn't wipe out every single table when a new table is added?
- Deployments are not currently backed by any CI/CD process
- Import deduplicated unified data to Redshift regularly and use this as the full_load basis for views
  - This avoids breakage when full load tasks need re-run in DMS
  - This should help performance since deduplication queries will get more and more expensive as full load and CDC data have a wider gap on freshness and we don't really want to have to re-run full loads since ideally we run a full load then reduce the DMS instance size for CDC only purposes whch is much less demanding

### Tips/Guidance

- Run `npx projen` after modifying the `projen` rc file
- Run `yarn build` after modifying CDK code to update snapshots before you commit changes
- As a bare minimum your code should have a snapshot test

#### Adding additional tables

- Add table to the [database details](src/databases/renovolive.ts)
- Stop all CDC DMS tasks for your target env
- Re-deploy DMS stacks `cdk diff <dev>RenovoLiveDbDms*`
- Resume, not restart, the CDC tasks

#### Providing primary key information for data loading and deduplication

1. Run this query to get the info

```sql
WITH PrimaryKeys AS (
    SELECT
        'cl' AS dbType,
        LOWER(t.TABLE_SCHEMA) AS TABLE_SCHEMA,
        LOWER(t.TABLE_NAME) AS TABLE_NAME,
        LOWER(c.COLUMN_NAME) AS COLUMN_NAME
    FROM 
        clRenovo.INFORMATION_SCHEMA.TABLES t
    INNER JOIN 
        clRenovo.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc ON t.TABLE_NAME = tc.TABLE_NAME
        AND t.TABLE_SCHEMA = tc.TABLE_SCHEMA
        AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
    INNER JOIN 
        clRenovo.INFORMATION_SCHEMA.KEY_COLUMN_USAGE c ON c.TABLE_NAME = t.TABLE_NAME
        AND c.TABLE_SCHEMA = t.TABLE_SCHEMA
        AND c.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
    WHERE 
        t.TABLE_TYPE = 'BASE TABLE'
    AND 
        t.TABLE_SCHEMA = 'dbo'

    UNION ALL

    SELECT
        'renovomaster' AS dbType,
        LOWER(t.TABLE_SCHEMA) AS TABLE_SCHEMA,
        LOWER(t.TABLE_NAME) AS TABLE_NAME,
        LOWER(c.COLUMN_NAME) AS COLUMN_NAME
    FROM 
        RenovoMaster.INFORMATION_SCHEMA.TABLES t
    INNER JOIN 
        RenovoMaster.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc ON t.TABLE_NAME = tc.TABLE_NAME
        AND t.TABLE_SCHEMA = tc.TABLE_SCHEMA
        AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
    INNER JOIN 
        RenovoMaster.INFORMATION_SCHEMA.KEY_COLUMN_USAGE c ON c.TABLE_NAME = t.TABLE_NAME
        AND c.TABLE_SCHEMA = t.TABLE_SCHEMA
        AND c.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
    WHERE 
        t.TABLE_TYPE = 'BASE TABLE'
    AND 
        t.TABLE_SCHEMA = 'dbo'
)
SELECT 
    LOWER(dbType) AS dbType,
    TABLE_SCHEMA,
    TABLE_NAME,
    LOWER(STRING_AGG(COLUMN_NAME, ' | ')) AS PRIMARY_KEY_COLUMNS
FROM 
    PrimaryKeys
GROUP BY 
    dbType, TABLE_SCHEMA, TABLE_NAME
ORDER BY 
    dbType, TABLE_SCHEMA, TABLE_NAME;
```

1. Load the data to s3 at [renovolive-rawdata/renovolive/<env>/pks](https://us-east-1.console.aws.amazon.com/s3/buckets/renovolive-rawdata?region=us-east-1&bucketType=general&prefix=renovolive/pks/)

## References

1 - [projen/projen](https://github.com/projen/projen)
