import {
  aws_iam as iam,
  Stack,
  StackProps,
  aws_kms as kms,
  aws_glue as gluecfn,
  aws_s3 as s3,
  aws_secretsmanager as secretsmanager,
  App,
} from 'aws-cdk-lib';
import * as glue from '@aws-cdk/aws-glue-alpha';
import { RenovoLiveEnv, DatabaseDetails } from './common';
import { NagSuppressions } from 'cdk-nag';

export enum GlueTriggerTypes {
  ON_DEMAND = 'ON_DEMAND',
  SCHEDULED = 'SCHEDULED',
  CONDITIONAL = 'CONDITIONAL',
  EVENT = 'EVENT',
}

export enum GlueCrawlerStates {
  SUCCEEDED = 'SUCCEEDED',
  FAILED = 'FAILED',
  CANCELLED = 'CANCELLED',
}

export interface RLDbGlueStackProps extends StackProps {
  /**
   * The RenovoLive databases for which Glue will
   * be configured for
   */
  readonly databases: DatabaseDetails[];
  /**
   * The raw data bucket where Glue will read data from
   */
  readonly rawDataBucket: s3.IBucket;
  /**
   * The bucket where glue scripts are stored
   */
  readonly glueScriptsBucket: s3.IBucket;
  /**
   * The glue scratch bucket where Glue jobs will write temporary data
   */
  readonly glueScratchBucket: s3.IBucket;
  /**
   * The KMS key used for data encryption
   */
  readonly dataKmsKey: kms.IKey;
  /**
   * The RenovoLive environment for which Glue will be configured
   */
  readonly renovoLiveEnv: RenovoLiveEnv;
  /**
   * The Glue connection to the Redshift serverless cluster
   * used by Glue jobs
   */
  readonly glueConnection: glue.IConnection;
  /**
   * The secret used by the Glue connection to access the Redshift cluster
   */
  readonly glueConnectionSecret: secretsmanager.ISecret;
  /**
   * The redshift role used by Glue jobs when creating external schemas
   */
  readonly redshiftRole: iam.IRole;
}

export class RLDbGlueStack extends Stack {
  constructor(scope: App, id: string, props: RLDbGlueStackProps) {
    super(scope, id, props);

    for (const clientDb of props.databases) {
      new glue.Database(this, `glueDb${clientDb.databaseName}`, {
        databaseName: `${props.renovoLiveEnv}-${clientDb.databaseName.toLowerCase()}`,
      });
    }

    const unifieddb = new glue.Database(this, 'rlUnifiedDb', {
      databaseName: `${props.renovoLiveEnv}-renovolive-unifieddb`,
    });

    const role = new iam.Role(this, 'glueJobRole', {
      description: 'Role for Glue jobs for RL data',
      assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'),
      ]
    });

    NagSuppressions.addResourceSuppressions(role, [
      {
        id: 'AwsSolutions-IAM4',
        reason: 'This role is assumed by AWS Glue service and needs the service policy managed \
        by AWS',
      },
      {
        id: 'AwsSolutions-IAM5',
        reason: 'This role uses the service policy managed by AWS which includes wildcard \
        permissions for Glue service required to manage Cloudwatch logs and Glue assets.',
      },
    ], true);

    const jobS3Policy = new iam.ManagedPolicy(this, 'jobS3Policy', {
      statements: [
        new iam.PolicyStatement({
          sid: 'S3ObjectRetrieval',
          actions: [
            's3:GetObject',
          ],
          resources: [
            props.rawDataBucket.arnForObjects('*'),
          ],
        }),
        new iam.PolicyStatement({
          sid: 'S3ListBucket',
          actions: [
            's3:ListBucket',
          ],
          resources: [
            props.rawDataBucket.bucketArn,
          ],
        }),
        new iam.PolicyStatement({
          sid: 'S3ScratchBucketAccess',
          actions: [
            's3:PutObject',
            's3:GetObject',
            's3:DeleteObject',
            's3:ListBucket',
          ],
          resources: [
            props.glueScratchBucket.bucketArn,
            props.glueScratchBucket.arnForObjects('*'),
          ],
        }),
        new iam.PolicyStatement({
          sid: 'EncryptDecryptAccessForS3Data',
          actions: [
            'kms:Decrypt',
            'kms:Encrypt',
            'kms:GenerateDataKey',
          ],
          resources: [
            props.dataKmsKey.keyArn,
          ],
        }),
        new iam.PolicyStatement({
          sid: 'AccessRedShiftConnectionSecret',
          actions: [
            'secretsmanager:GetSecretValue',
          ],
          resources: [
            props.glueConnectionSecret.secretArn,
            props.glueConnectionSecret.secretArn + '-*',
          ],
        })
      ]
    });

    NagSuppressions.addResourceSuppressions(jobS3Policy, [
      {
        id: 'AwsSolutions-IAM5',
        reason: 'This policy allows Glue to read data from the S3 bucket containing raw data',
      },
    ]);

    role.addManagedPolicy(jobS3Policy);

    new gluecfn.CfnCrawler(this, `${props.renovoLiveEnv}-pks`, {
      name: `${props.renovoLiveEnv}-crawler-pks`,
      role: role.roleArn,
      databaseName: unifieddb.databaseName,
      targets: {
        s3Targets: [
          {
            path: `s3://${props.rawDataBucket.bucketName}/renovolive/pks/`,
            sampleSize: 1,
          },
        ],
      },
      schemaChangePolicy: {
        updateBehavior: 'UPDATE_IN_DATABASE',
        deleteBehavior: 'DEPRECATE_IN_DATABASE',
      },
    });

    for (const db of props.databases) {
      const workflow = new gluecfn.CfnWorkflow(this, `glueWorkflow-${db.databaseName}`, {
        name: `${props.renovoLiveEnv}-renovolive-${db.databaseName}-workflow`,
        description: `Workflow for RL data for ${db.databaseName}`,
        maxConcurrentRuns: 1,
      });

      const crawlerFullLoad = new gluecfn.CfnCrawler(this, `${props.renovoLiveEnv}-${db.databaseName}-full-load`, {
        name: `${props.renovoLiveEnv}-${db.databaseName}-crawler-full-load`,
        role: role.roleArn,
        databaseName: `${props.renovoLiveEnv}-${db.databaseName.toLowerCase()}`,
        targets: {
          s3Targets: [
            {
              path: `s3://${props.rawDataBucket.bucketName}/renovolive/${props.renovoLiveEnv}/${db.databaseName}/full_load/`,
              sampleSize: 1,
            },
          ],
        },
        schemaChangePolicy: {
          updateBehavior: 'UPDATE_IN_DATABASE',
          deleteBehavior: 'DEPRECATE_IN_DATABASE',
        },
        tablePrefix: `full_load_`,
      });

      const crawlerCdc = new gluecfn.CfnCrawler(this, `${props.renovoLiveEnv}-${db.databaseName}-cdc`, {
        name: `${props.renovoLiveEnv}-${db.databaseName}-crawler-cdc`,
        role: role.roleArn,
        databaseName: `${props.renovoLiveEnv}-${db.databaseName.toLowerCase()}`,
        targets: {
          s3Targets: [
            {
              path: `s3://${props.rawDataBucket.bucketName}/renovolive/${props.renovoLiveEnv}/${db.databaseName}/cdc/`,
            },
          ],
        },
        schemaChangePolicy: {
          updateBehavior: 'UPDATE_IN_DATABASE',
          deleteBehavior: 'DEPRECATE_IN_DATABASE',
        },
        tablePrefix: `cdc_`,
        /**
         * Set table level configuration to 5 to avoid tables being split into multiple tables.
         * The number correlates to the absolute path level in the S3 bucket so for example:
         * 
         * renovolive[1]/dev[2]/clrenovo[3]/load_type[4]/dbo[5]/table[6]
         */
        configuration: JSON.stringify({
          "Version": 1.0,
          "Grouping": {
            "TableLevelConfiguration": 6
          }
        }),
      });

      new gluecfn.CfnTrigger(this, `startTrigger-${props.renovoLiveEnv}-${db.databaseName}`, {
        name: `startTrigger-${props.renovoLiveEnv}-${db.databaseName}`,
        workflowName: workflow.name,
        description: `Manual trigger for ${props.renovoLiveEnv} ${db.databaseName}`,
        type: GlueTriggerTypes.ON_DEMAND,
        actions: [
          {
            crawlerName: crawlerFullLoad.name,
          },
          {
            crawlerName: crawlerCdc.name,
          },
        ],
      });

      let triggers: gluecfn.CfnTrigger[] = [];

      let triggerNum = 0;

      for (const table of db.tables) {
        const job = new glue.Job(this, `glueJob-${props.renovoLiveEnv}-${db.databaseName}-${table.tableName}`, {
          jobName: `${props.renovoLiveEnv}-renovolive-${db.databaseName}-${table.tableName}-full-load`,
          role,
          description: `Job to full load table ${table.tableName} for RL data from ${db.databaseName}`,
          executable: glue.JobExecutable.pythonEtl({
            glueVersion: glue.GlueVersion.V4_0,
            pythonVersion: glue.PythonVersion.THREE,
            script: glue.Code.fromBucket(props.glueScriptsBucket, 'renovolive_table_load.py'),
          }),
          defaultArguments: {
            '--enable-glue-datacatalog': 'true',
            '--redshift-connection': props.glueConnection.connectionName,
            '--redshift-role-arn': props.redshiftRole.roleArn,
            '--database': db.databaseName,
            '--table': table.tableName,
            '--enable-continuous-cloudwatch-log': 'true',
          },
          connections: [
            props.glueConnection,
          ]
        });

        const trigger = new gluecfn.CfnTrigger(this, `${props.renovoLiveEnv}-${db.databaseName}-${table.tableName}`, {
          workflowName: workflow.name,
          name: `${props.renovoLiveEnv}-${db.databaseName}-${table.tableName}-trigger`,
          description: `Trigger for job ${props.renovoLiveEnv} ${db.databaseName} ${table.tableName}`,
          type: GlueTriggerTypes.CONDITIONAL,
          actions: [
            {
              jobName: job.jobName,
            },
          ],
          predicate: {
            conditions: [
              {
                logicalOperator: 'EQUALS',
                crawlerName: crawlerFullLoad.name,
                crawlState: GlueCrawlerStates.SUCCEEDED,
              },
            ],
          },
          startOnCreation: true, // This property name is so misleading.. triggers are "CREATED" but must be "ACTIVATED"
        });

        /**
         * Hacky workaround for issues with concurrent trigger creation in CloudFormation
         */
        if (triggerNum > 0) {
          trigger.addDependency(triggers[triggerNum - 1]);
        }

        triggers.push(trigger);
        triggerNum++;

        NagSuppressions.addResourceSuppressions(job.role, [
          {
            id: 'AwsSolutions-IAM5',
            reason: 'Job construct in CDK creates a policy to access the S3 bucket containing the scripts \
            which uses wildcard permissions. We dont have control over this without escape hatch hacks \
            in the CDK code',
          },
          {
            id: 'NIST.800.53.R5-IAMNoInlinePolicy',
            reason: 'The inline policy is created by the CDK construct and is required for the job to access \
            the S3 bucket containing the scripts. We dont have control over this without escape hatch hacks \
            in the CDK code',
          },
        ], true)
      }
    }

    // const controllerJob = new glue.Job(this, 'glueControllerJob', {
    //   jobName: `${props.renovoLiveEnv}-controller-job`,
    //   role,
    //   description: 'Controller job for RL data',
    //   executable: glue.JobExecutable.pythonEtl({
    //     glueVersion: glue.GlueVersion.V4_0,
    //     pythonVersion: glue.PythonVersion.THREE,
    //     script: glue.Code.fromBucket(props.glueScriptsBucket, 'scripts/renovolive_controller.py'),
    //   }),
    //   defaultArguments: {
    //     '--enable-glue-datacatalog': 'true',
    //   },
    //   maxRetries: 0,
    //   timeout: Duration.hours(2),
    //   workerCount: 1,
    //   workerType: glue.WorkerType.G_025X,
    // });

    // NagSuppressions.addResourceSuppressions(controllerJob.role, [
    //   {
    //     id: 'AwsSolutions-IAM5',
    //     reason: 'Job construct in CDK creates a policy to access the S3 bucket containing the scripts \
    //     which uses wildcard permissions. We dont have control over this without escape hatch hacks \
    //     in the CDK code',
    //   },
    //   {
    //     id: 'NIST.800.53.R5-IAMNoInlinePolicy',
    //     reason: 'The inline policy is created by the CDK construct and is required for the job to access \
    //     the S3 bucket containing the scripts. We dont have control over this without escape hatch hacks \
    //     in the CDK code',
    //   },
    // ], true);
  }
}

// export interface GlueStackProps extends StackProps {
//   readonly roleName: string;
//   readonly crawlerNameClRenovo: string;
//   readonly crawlerNameRenovoMaster: string;
//   readonly crawlerNameUnifiedDb : string;
//   readonly glueScriptPath: string; // Local path to the glue script directory
//   readonly rawBucket: s3.IBucket; // Reference to the S3 bucket for DMS from s3 stack
//   readonly glueScriptsBucket: s3.IBucket; // Reference to the S3 bucket for glue from s3 stack
// }

// export class GlueStack extends Stack {
//   public readonly glueRole: iam.Role;

//   constructor(scope: cdk.App, id: string, props: GlueStackProps) {
//     super(scope, id, props);

//     new s3deployment.BucketDeployment(this, 'DeployGlueScripts', {
//       sources: [s3deployment.Source.asset(path.join(__dirname, props.glueScriptPath))],
//       destinationBucket: props.glueScriptsBucket,
//       destinationKeyPrefix: 'scripts/',
//     });

//     const parquetPaths = [
//       { path: '/renovolive/clRenovo/', dbName: 'clrenovo-db', crawlerName: props.crawlerNameClRenovo },
//       { path: '/renovolive/RenovoMaster/', dbName: 'renovomaster-db', crawlerName: props.crawlerNameRenovoMaster },
//     ];

//     const glueDatabases = parquetPaths.map(parquet =>
//       new glue.CfnDatabase(this, `glue${parquet.dbName}`, {
//         catalogId: Stack.of(this).account,
//         databaseInput: {
//           name: parquet.dbName,
//         },
//       }),
//     );

//     // Create unifieddb
//     new glue.CfnDatabase(this, 'glueUnifiedDb', {
//       catalogId: Stack.of(this).account,
//       databaseInput: {
//         name: 'unifieddb',
//       },
//     });

//     // Create Glue crawler role to access S3 bucket
//     const glueCrawlerRole = new iam.Role(this, 'glueCrawlerRole', {
//       roleName: props.roleName,
//       description: 'Assigns the managed policy AWSGlueServiceRole to AWS Glue Crawler so it can crawl S3 buckets',
//       managedPolicies: [
//         iam.ManagedPolicy.fromManagedPolicyArn(this, 'glueServicePolicy', 'arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole'),
//       ],
//       assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
//     });
//     this.glueRole = glueCrawlerRole;

//     // Add SecretsManager access for the Glue job
//     new iam.Policy(this, 'SecretsManagerAccessPolicy', {
//       roles: [glueCrawlerRole],
//       statements: [
//         new iam.PolicyStatement({
//           effect: iam.Effect.ALLOW,
//           actions: ['secretsmanager:GetSecretValue'],
//           resources: [`arn:aws:secretsmanager:${this.region}:${this.account}:secret:redshift!renovo-np-admin-*`],
//         }),
//       ],
//     });

//     // Add policy to role to grant access to S3 asset bucket
//     new iam.Policy(this, 'iamPolicyForAssets', {
//       force: true,
//       policyName: 'gluePolicyWorkflowAssetAccess',
//       roles: [glueCrawlerRole],
//       statements: [
//         new iam.PolicyStatement({
//           effect: iam.Effect.ALLOW,
//           actions: [
//             's3:GetObject',
//             's3:PutObject',
//             's3:DeleteObject',
//             's3:ListBucket',
//           ],
//           resources: [
//             `arn:aws:s3:::${props.rawBucket.bucketName}/*`,
//             `arn:aws:s3:::${props.glueScriptsBucket.bucketName}/*`,
//           ],
//         }),
//       ],
//     });

//     parquetPaths.forEach((parquet, index) => {
//       new glue.CfnCrawler(this, `glueCrawlerS3${parquet.dbName}`, {
//         name: parquet.crawlerName,
//         role: glueCrawlerRole.roleArn,
//         targets: {
//           s3Targets: [
//             {
//               path: `s3://${props.rawBucket.bucketName}${parquet.path}`,
//             },
//           ],
//         },
//         databaseName: glueDatabases[index].ref,
//         schemaChangePolicy: {
//           updateBehavior: 'UPDATE_IN_DATABASE',
//           deleteBehavior: 'DEPRECATE_IN_DATABASE',
//         },
//       });
//     });

//     const glue_controller = new glue.CfnJob(this, 'glueJobAsset', {
//       name: 'Controller_cdk',
//       description: 'Controller Glue Job',
//       role: glueCrawlerRole.roleArn,
//       executionProperty: {
//         maxConcurrentRuns: 50,
//       },
//       command: {
//         name: 'glueetl',
//         pythonVersion: '3',
//         scriptLocation: `s3://${props.glueScriptsBucket.bucketName}/scripts/Controller.py`,
//       },
//       defaultArguments: {
//         '--enable-glue-datacatalog': true,
//       },
//       maxRetries: 0,
//       timeout: 120,
//       numberOfWorkers: 10,
//       glueVersion: '4.0',
//       workerType: 'G.1X',
//     });

//     // Add a trigger to schedule the job on a cron expression
//     new glue.CfnTrigger(this, 'triggerController', {
//       name: 'Trigger_Controller',
//       type: 'SCHEDULED',
//       schedule: 'cron(0 0 * * ? *)',
//       actions: [
//         {
//           jobName: glue_controller.name,
//         },
//       ],
//       startOnCreation: true,
//     });

//     new glue.CfnJob(this, 'glueJobRefreshMaterializedViews', {
//       name: 'RefreshMaterializedViews_cdk',
//       description: 'Glue job to refresh materialized views',
//       role: glueCrawlerRole.roleArn,
//       executionProperty: {
//         maxConcurrentRuns: 50,
//       },
//       command: {
//         name: 'glueetl',
//         pythonVersion: '3',
//         scriptLocation: `s3://${props.glueScriptsBucket.bucketName}/scripts/RefreshMaterializedViews.py`,
//       },
//       maxRetries: 0,
//       timeout: 120,
//       numberOfWorkers: 10,
//       glueVersion: '4.0',
//       workerType: 'G.1X',
//       connections: {
//         connections: ['redshift_con_cdk'],
//       },
//       defaultArguments: {
//         '--extra-py-files': 's3://aws-glue-studio-transforms-510798373988-prod-us-east-1/gs_common.py,s3://aws-glue-studio-transforms-510798373988-prod-us-east-1/gs_now.py',
//         '--enable-glue-datacatalog': true,
//         // Add other default arguments if necessary
//       },
//     });

//     new glue.CfnJob(this, 'glueJobIncClTables', {
//       name: 'Inc_cl_tables_cdk',
//       description: 'Glue job for incremental cl tables',
//       role: glueCrawlerRole.roleArn,
//       executionProperty: {
//         maxConcurrentRuns: 50,
//       },
//       command: {
//         name: 'glueetl',
//         pythonVersion: '3',
//         scriptLocation: `s3://${props.glueScriptsBucket.bucketName}/scripts/Inc_cl_tables_cdk.py`,
//       },
//       maxRetries: 0,
//       timeout: 120,
//       numberOfWorkers: 10,
//       glueVersion: '4.0',
//       workerType: 'G.1X',
//       defaultArguments: {
//         '--conf': 'spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.catalog.glue_catalog.warehouse=file:///tmp/spark-warehouse --conf spark.sql.parquet.datetimeRebaseModeInRead=LEGACY',
//         '--datalake-formats': 'iceberg',
//         '--source_db': 'clrenovo-db',
//         '--table_name': 'clients',
//         '--table_pk': 'clientid',
//         '--type': 'incremental',
//         '--enable-glue-datacatalog': true,
//       },
//     });

//     new glue.CfnJob(this, 'glueJobIncRenovoTables', {
//       name: 'Inc_renovo_tables_cdk',
//       description: 'Glue job for incremental renovo tables',
//       role: glueCrawlerRole.roleArn,
//       executionProperty: {
//         maxConcurrentRuns: 50,
//       },
//       command: {
//         name: 'glueetl',
//         pythonVersion: '3',
//         scriptLocation: `s3://${props.glueScriptsBucket.bucketName}/scripts/Inc_renovo_tables_cdk.py`,
//       },
//       maxRetries: 0,
//       timeout: 120,
//       numberOfWorkers: 10,
//       glueVersion: '4.0',
//       workerType: 'G.1X',
//       connections: {
//         connections: ['redshift_con_cdk'],
//       },
//       defaultArguments: {
//         '--extra-py-files': 's3://aws-glue-studio-transforms-510798373988-prod-us-east-1/gs_common.py,s3://aws-glue-studio-transforms-510798373988-prod-us-east-1/gs_now.py',
//         '--enable-glue-datacatalog': true,
//       },
//     });


//   }
// }
