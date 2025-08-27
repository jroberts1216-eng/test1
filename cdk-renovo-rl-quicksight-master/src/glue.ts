import {
  aws_iam as iam,
  Stack,
  StackProps,
  aws_kms as kms,
  aws_glue as gluecfn,
  aws_s3 as s3,
  aws_stepfunctions as stepfunctions,
  aws_stepfunctions_tasks as tasks,
  aws_events as events,
  aws_lambda as lambda,
  aws_redshiftserverless as redshiftserverless,
  aws_logs as logs,
  aws_events_targets as targets,
  aws_sns as sns,
  aws_sqs as sqs,
  aws_sns_subscriptions as snsSubscriptions,
  aws_scheduler as scheduler,
  aws_scheduler_targets as schedulerTargets,
  RemovalPolicy,
  App,
  Duration,
} from 'aws-cdk-lib';
import * as glue from '@aws-cdk/aws-glue-alpha';
import { RenovoLiveEnv, DatabaseDetails } from './common';
import { NagSuppressions } from 'cdk-nag';
import * as path from 'path';

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
   * The KMS key used for data encryption
   */
  readonly dataKmsKey: kms.IKey;
  /**
   * The RenovoLive environment for which Glue will be configured
   */
  readonly renovoLiveEnv: RenovoLiveEnv;
  /**
   * The redshift role used by Glue jobs when creating external schemas
   */
  readonly redshiftRole: iam.IRole;
  /**
   * The redshift workgroup where data load work will be done
   */
  readonly redshiftWorkgroup: redshiftserverless.CfnWorkgroup;
  /**
   * The database name for the redshift database
   * 
   * @default renovolive
   */
  readonly redshiftDatabaseName?: string;
  /**
   * The name of the secret containing the redshift credentials
   * 
   * @default - redshift!${props.redshiftWorkgroupName}
   */
  readonly redshiftSecretName?: string;
  /**
   * The SNS topic where raw data ingestion notifications will be received
   */
  readonly rawIngestionTopic: sns.ITopic;
}

export class RLDbGlueStack extends Stack {
  constructor(scope: App, id: string, props: RLDbGlueStackProps) {
    super(scope, id, props);

    const roleName = `${props.renovoLiveEnv}-renovolive-glue-crawler-role`;

    const role = new iam.Role(this, 'glueJobRole', {
      roleName,
      description: 'Role for Glue crawlers for RL data',
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
      ]
    });

    NagSuppressions.addResourceSuppressions(jobS3Policy, [
      {
        'id': 'AwsSolutions-IAM5',
        'reason': 'The policy is required for the Glue job to access the S3 bucket containing the data \
        and the KMS key used for encryption. A wild card is used because we do not know all object names.',
      }
    ]);

    role.addManagedPolicy(jobS3Policy);

    const logsKey = new kms.Key(this, 'etlLogsKey', {
      enableKeyRotation: true,
      alias: `alias/renovolive/${props.renovoLiveEnv}/etl/logs`,
      policy: new iam.PolicyDocument({
        statements: [
          new iam.PolicyStatement({
            effect: iam.Effect.ALLOW,
            actions: [
              'kms:*',
            ],
            resources: ['*'],
            principals: [new iam.AccountRootPrincipal],
          }),
          new iam.PolicyStatement({
            principals: [new iam.ServicePrincipal('logs.amazonaws.com')],
            effect: iam.Effect.ALLOW,
            actions: [
              'kms:Decrypt',
              'kms:Encrypt',
              'kms:ReEncrypt*',
              'kms:GenerateDataKey*',
              'kms:Describe*',
            ],
            resources: ['*'],
            conditions: {
              ArnEquals: {
                'kms:EncryptionContext:aws:logs:arn': `arn:aws:logs:${Stack.of(this).region}:${Stack.of(this).account}:*`,
              }
            }
          }),
        ]
      }),
      removalPolicy: RemovalPolicy.RETAIN_ON_UPDATE_OR_DELETE
    });

    const metadataDb = new glue.Database(this, 'metadata-glue-db', {
      databaseName: `${props.renovoLiveEnv}-renovolive-metadata`,
    });

    const securityConfig = new glue.SecurityConfiguration(this, 'glueSecurityConfig', {
      cloudWatchEncryption: {
        kmsKey: logsKey,
        mode: glue.CloudWatchEncryptionMode.KMS,
      },
      securityConfigurationName: `${props.renovoLiveEnv}-renovolive-security-config`,
      s3Encryption: {
        kmsKey: props.dataKmsKey,
        mode: glue.S3EncryptionMode.KMS,
      },
    });

    const rlPksClassifierName = `${props.renovoLiveEnv}-renovolive-pks-classifier`;

    const rlPksClassifier = new gluecfn.CfnClassifier(this, 'rlPksClassifier', {
      csvClassifier: {
        name: rlPksClassifierName,
        delimiter: ',',
        quoteSymbol: '"',
        containsHeader: 'PRESENT',
        header: ['dbType', 'tableschema', 'tablename', 'primarykeys'],
      }
    });

    const pksCrawler = new gluecfn.CfnCrawler(this, `${props.renovoLiveEnv}-pks`, {
      name: `${props.renovoLiveEnv}-renovolive-pks-crawler`,
      role: role.roleArn,
      databaseName: metadataDb.databaseName,
      targets: {
        s3Targets: [
          {
            path: `s3://${props.rawDataBucket.bucketName}/renovolive/${props.renovoLiveEnv}/pks/`,
            sampleSize: 1,
          },
        ],
      },
      schemaChangePolicy: {
        updateBehavior: 'UPDATE_IN_DATABASE',
        deleteBehavior: 'DEPRECATE_IN_DATABASE',
      },
      classifiers: [
        rlPksClassifierName,
      ],
      crawlerSecurityConfiguration: securityConfig.securityConfigurationName,
    });

    pksCrawler.node.addDependency(rlPksClassifier);

    for (const clientDb of props.databases) {
      new glue.Database(this, `glueDb${clientDb.databaseName}`, {
        databaseName: `${props.renovoLiveEnv}-${clientDb.databaseName.toLowerCase()}`,
      });
    }

    const logsEncryptionPolicy = new iam.ManagedPolicy(this, 'logsEncryptionPolicy', {
      statements: [
        new iam.PolicyStatement({
          actions: [
            'logs:AssociateKmsKey',
          ],
          resources: [
            `arn:aws:logs:${Stack.of(this).region}:${Stack.of(this).account}:log-group:/aws-glue/crawlers-role/${roleName}-${securityConfig.securityConfigurationName}:log-stream:*`,
          ],
        }),
      ]
    });

    NagSuppressions.addResourceSuppressions(logsEncryptionPolicy, [
      {
        'id': 'AwsSolutions-IAM5',
        'reason': 'The policy is required for the Glue role to associate the KMS key with the Cloudwatch log group \
        and log stream. The policy requires a wildcard for the log stream name.',
      }
    ]);

    role.addManagedPolicy(logsEncryptionPolicy);

    const tableCreationHandlerLogGroup = new logs.LogGroup(this, 'tableCreationHandlerLogGroup', {
      logGroupName: `/renovolive/etl/${props.renovoLiveEnv}/lambda/${props.renovoLiveEnv}-table-creation-handler`,
      removalPolicy: RemovalPolicy.DESTROY,
      retention: logs.RetentionDays.TWO_YEARS,
      encryptionKey: logsKey,
    });

    const tableCreationHandlerRole = new iam.Role(this, 'tableViewCreationHandlerRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
    });

    const tableCreationHandlerExecutionPolicy = new iam.ManagedPolicy(this, 'tableCreationHandlerExecutionPolicy', {
      statements: [
        new iam.PolicyStatement({
          actions: [
            'logs:CreateLogStream',
            'logs:PutLogEvents',
          ],
          resources: [
            tableCreationHandlerLogGroup.logGroupArn,
            tableCreationHandlerLogGroup.logGroupArn + ':*',
          ],
        }),
      ],
    });

    tableCreationHandlerRole.addManagedPolicy(tableCreationHandlerExecutionPolicy);

    NagSuppressions.addResourceSuppressions(tableCreationHandlerExecutionPolicy, [
      {
        'id': 'AwsSolutions-IAM5',
        'reason': 'The policy is required for the Lambda function to access the Cloudwatch logs \
        and the log permissions requires a wildcard for log stream creation and log events.',
      },
    ]);

    const tableCreationEventHandler = new lambda.Function(this, 'CreateTableEventHandler', {
      functionName: `${props.renovoLiveEnv}-renovolive-table-creation-event-handler`,
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'index.handler',
      code: lambda.Code.fromAsset(path.join(__dirname, './handlers/table_creation_handler/')),
      logGroup: tableCreationHandlerLogGroup,
      role: tableCreationHandlerRole,
      reservedConcurrentExecutions: 100,
      timeout: Duration.seconds(300),
      memorySize: 128,
    });

    NagSuppressions.addResourceSuppressions(tableCreationEventHandler, [
      {
        'id': 'NIST.800.53.R5-LambdaDLQ',
        'reason': 'The Lambda function does not require a DLQ for this use case as it is run from \
        a step function and does not require a DLQ for error handling since Step Functions can be \
        redriven on failure.',
      },
      {
        'id': 'NIST.800.53.R5-LambdaInsideVPC',
        'reason': 'The Lambda function does not require VPC access for this use case and does not interact with any \
        resources that would be better accessed from within a VPC. Such as S3.',
      },
    ]);

    const realtimeViewLogGroup = new logs.LogGroup(this, 'realtimeViewLogGroup', {
      logGroupName: `/renovolive/etl/${props.renovoLiveEnv}/lambda/${props.renovoLiveEnv}-realtime-view-creation-handler`,
      removalPolicy: RemovalPolicy.DESTROY,
      retention: logs.RetentionDays.TWO_YEARS,
      encryptionKey: logsKey,
    });

    const realtimeViewCreationHandlerRole = new iam.Role(this, 'realtimeViewCreationHandlerRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
    });

    const realtimeViewCreationHandlerExecutionPolicy = new iam.ManagedPolicy(this, 'realtimeViewCreationHandlerExecutionPolicy', {
      statements: [
        new iam.PolicyStatement({
          actions: [
            'logs:CreateLogStream',
            'logs:PutLogEvents',
          ],
          resources: [
            realtimeViewLogGroup.logGroupArn,
            realtimeViewLogGroup.logGroupArn + ':*',
          ],
        }),
      ],
    });

    realtimeViewCreationHandlerRole.addManagedPolicy(realtimeViewCreationHandlerExecutionPolicy);

    NagSuppressions.addResourceSuppressions(realtimeViewCreationHandlerExecutionPolicy, [
      {
        'id': 'AwsSolutions-IAM5',
        'reason': 'The policy is required for the Lambda function to access the Cloudwatch logs \
        and the log permissions requires a wildcard for log stream creation and log events.',
      },
    ]);

    const realtimeViewCreationHandlerDataApiPolicy = new iam.ManagedPolicy(this, 'realtimeViewCreationHandlerDataApiPolicy', {
      statements: [
        new iam.PolicyStatement({
          sid: 'RedshiftDataExecute',
          actions: [
            'redshift-data:ExecuteStatement',
          ],
          resources: [
            props.redshiftWorkgroup.attrWorkgroupWorkgroupArn,
          ],
        }),
        new iam.PolicyStatement({
          sid: 'RedshiftDataGetResults',
          actions: [
            'redshift-data:DescribeStatement',
            'redshift-data:GetStatementResult',
          ],
          resources: [
            '*'
          ],
          conditions: {
            StringEquals: {
              'redshift-data:statement-owner-iam-userid': '${aws:userid}',
            }
          }
        }),
      ],
    });

    realtimeViewCreationHandlerRole.addManagedPolicy(realtimeViewCreationHandlerDataApiPolicy);

    NagSuppressions.addResourceSuppressions(realtimeViewCreationHandlerDataApiPolicy, [
      {
        'id': 'AwsSolutions-IAM5',
        'reason': 'The policy is required for the Lambda function to access the Redshift query results \
        which does not support a resource restriction but a condition is used to restrict access to the \
        user who executed the query.',
      },
    ]);

    const realtimeViewCreationHandlerSecretAccessPolicy = new iam.ManagedPolicy(this, 'realtimeViewCreationHandlerSecretAccessPolicy', {
      statements: [
        new iam.PolicyStatement({
          sid: 'RedshiftDataSecretAccess',
          actions: [
            'secretsmanager:GetSecretValue',
            'secretsmanager:DescribeSecret',
          ],
          resources: [
            `arn:aws:secretsmanager:${Stack.of(this).region}:${Stack.of(this).account}:secret:${props.redshiftSecretName ?? `redshift!${props.redshiftWorkgroup.attrWorkgroupWorkgroupName}`}-admin*`,
          ],
        }),
      ],
    });

    realtimeViewCreationHandlerRole.addManagedPolicy(realtimeViewCreationHandlerSecretAccessPolicy);

    NagSuppressions.addResourceSuppressions(realtimeViewCreationHandlerSecretAccessPolicy, [
      {
        'id': 'AwsSolutions-IAM5',
        'reason': 'The policy is required for the Lambda function to access the Redshift connection secret \
        which requires finding the full arn from the secret name. So we must wildcard the unknown portion.',
      },
    ]);

    const realtimeViewCreationHandler = new lambda.Function(this, 'CreateRealtimeView', {
      functionName: `${props.renovoLiveEnv}-renovolive-real-time-view-creation-handler`,
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'index.handler',
      code: lambda.Code.fromAsset(path.join(__dirname, './handlers/real_time_view_creation/')),
      environment: {
        REDSHIFT_WORKGROUP_NAME: props.redshiftWorkgroup.attrWorkgroupWorkgroupName,
        REDSHIFT_DATABASE: props.redshiftDatabaseName ?? 'renovolive',
        REDSHIFT_SECRET_NAME: props.redshiftSecretName ?? `redshift!${props.redshiftWorkgroup.attrWorkgroupWorkgroupName}-admin`
      },
      logGroup: realtimeViewLogGroup,
      role: realtimeViewCreationHandlerRole,
      reservedConcurrentExecutions: 100,
      timeout: Duration.seconds(300),
      memorySize: 128,
    })

    NagSuppressions.addResourceSuppressions(realtimeViewCreationHandler, [
      {
        'id': 'NIST.800.53.R5-LambdaDLQ',
        'reason': 'The Lambda function does not require a DLQ for this use case as it is run from \
        a step function and does not require a DLQ for error handling since Step Functions can be \
        redriven on failure.',
      },
      {
        'id': 'NIST.800.53.R5-LambdaInsideVPC',
        'reason': 'The Lambda function does not require VPC access for this use case and does not interact with any \
        resources that would be better accessed from within a VPC. Such as S3.',
      },
    ]);

    const snapshotTableCreationLogGroup = new logs.LogGroup(this, 'snapshotTableCreationLogGroup', {
      logGroupName: `/renovolive/etl/${props.renovoLiveEnv}/lambda/${props.renovoLiveEnv}-snapshot-table-creation-handler`,
      removalPolicy: RemovalPolicy.DESTROY,
      retention: logs.RetentionDays.TWO_YEARS,
      encryptionKey: logsKey,
    });

    const snapshotTableCreationHandlerRole = new iam.Role(this, 'snapshotTableCreationHandlerRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
    });

    const snapshotTableCreationHandlerExecutionPolicy = new iam.ManagedPolicy(this, 'snapshotTableCreationHandlerExecutionPolicy', {
      statements: [
        new iam.PolicyStatement({
          actions: [
            'logs:CreateLogStream',
            'logs:PutLogEvents',
          ],
          resources: [
            snapshotTableCreationLogGroup.logGroupArn,
            snapshotTableCreationLogGroup.logGroupArn + ':*',
          ],
        }),
      ],
    });

    snapshotTableCreationHandlerRole.addManagedPolicy(snapshotTableCreationHandlerExecutionPolicy);

    NagSuppressions.addResourceSuppressions(snapshotTableCreationHandlerExecutionPolicy, [
      {
        'id': 'AwsSolutions-IAM5',
        'reason': 'The policy is required for the Lambda function to access the Cloudwatch logs \
        and the log permissions requires a wildcard for log stream creation and log events.',
      },
    ]);

    const snapshotTableCreationHandlerDataApiPolicy = new iam.ManagedPolicy(this, 'snapshotTableCreationHandlerDataApiPolicy', {
      statements: [
        new iam.PolicyStatement({
          sid: 'RedshiftDataExecute',
          actions: [
            'redshift-data:ExecuteStatement',
          ],
          resources: [
            props.redshiftWorkgroup.attrWorkgroupWorkgroupArn,
          ],
        }),
        new iam.PolicyStatement({
          sid: 'RedshiftDataGetResults',
          actions: [
            'redshift-data:DescribeStatement',
            'redshift-data:GetStatementResult',
          ],
          resources: [
            '*'
          ],
          conditions: {
            StringEquals: {
              'redshift-data:statement-owner-iam-userid': '${aws:userid}',
            }
          }
        }),
      ],
    });

    snapshotTableCreationHandlerRole.addManagedPolicy(snapshotTableCreationHandlerDataApiPolicy);

    NagSuppressions.addResourceSuppressions(snapshotTableCreationHandlerDataApiPolicy, [
      {
        'id': 'AwsSolutions-IAM5',
        'reason': 'The policy is required for the Lambda function to access the Redshift query results \
        which does not support a resource restriction but a condition is used to restrict access to the \
        user who executed the query.',
      },
    ]);

    const snapshotTableCreationHandlerSecretAccessPolicy = new iam.ManagedPolicy(this, 'snapshotTableCreationHandlerSecretAccessPolicy', {
      statements: [
        new iam.PolicyStatement({
          sid: 'RedshiftDataSecretAccess',
          actions: [
            'secretsmanager:GetSecretValue',
            'secretsmanager:DescribeSecret',
          ],
          resources: [
            `arn:aws:secretsmanager:${Stack.of(this).region}:${Stack.of(this).account}:secret:${props.redshiftSecretName ?? `redshift!${props.redshiftWorkgroup.attrWorkgroupWorkgroupName}`}-admin*`,
          ],
        }),
      ],
    });

    snapshotTableCreationHandlerRole.addManagedPolicy(snapshotTableCreationHandlerSecretAccessPolicy);

    NagSuppressions.addResourceSuppressions(snapshotTableCreationHandlerSecretAccessPolicy, [
      {
        'id': 'AwsSolutions-IAM5',
        'reason': 'The policy is required for the Lambda function to access the Redshift connection secret \
        which requires finding the full arn from the secret name. So we must wildcard the unknown portion.',
      },
    ]);

    const snapshotTableCreationHandler = new lambda.Function(this, 'CreateSnapshotTable', {
      functionName: `${props.renovoLiveEnv}-renovolive-snapshot-table-creation-handler`,
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'index.handler',
      code: lambda.Code.fromAsset(path.join(__dirname, './handlers/snapshot_table_creation/')),
      environment: {
        REDSHIFT_WORKGROUP_NAME: props.redshiftWorkgroup.attrWorkgroupWorkgroupName,
        REDSHIFT_DATABASE: props.redshiftDatabaseName ?? 'renovolive',
        REDSHIFT_SECRET_NAME: props.redshiftSecretName ?? `redshift!${props.redshiftWorkgroup.attrWorkgroupWorkgroupName}-admin`
      },
      logGroup: snapshotTableCreationLogGroup,
      role: snapshotTableCreationHandlerRole,
      reservedConcurrentExecutions: 100,
      timeout: Duration.minutes(15),
      memorySize: 128,
    })

    NagSuppressions.addResourceSuppressions(snapshotTableCreationHandler, [
      {
        'id': 'NIST.800.53.R5-LambdaDLQ',
        'reason': 'The Lambda function does not require a DLQ for this use case as it is run from \
        a step function and does not require a DLQ for error handling since Step Functions can be \
        redriven on failure.',
      },
      {
        'id': 'NIST.800.53.R5-LambdaInsideVPC',
        'reason': 'The Lambda function does not require VPC access for this use case and does not interact with any \
        resources that would be better accessed from within a VPC. Such as S3.',
      },
    ]);

    const pkLookupHandlerLogGroup = new logs.LogGroup(this, 'pkLookupHandlerLogGroup', {
      logGroupName: `/renovolive/etl/${props.renovoLiveEnv}/lambda/${props.renovoLiveEnv}-pkLookup-handler`,
      removalPolicy: RemovalPolicy.DESTROY,
      retention: logs.RetentionDays.TWO_YEARS,
      encryptionKey: logsKey,
    });

    const pkLookupHandlerRole = new iam.Role(this, 'pkLookupHandlerRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
    });

    const pkLookupHandlerExecutionPolicy = new iam.ManagedPolicy(this, 'pkLookupHandlerExecutionPolicy', {
      statements: [
        new iam.PolicyStatement({
          actions: [
            'logs:CreateLogStream',
            'logs:PutLogEvents',
          ],
          resources: [
            pkLookupHandlerLogGroup.logGroupArn,
            pkLookupHandlerLogGroup.logGroupArn + ':*',
          ],
        }),
      ],
    });

    pkLookupHandlerRole.addManagedPolicy(pkLookupHandlerExecutionPolicy);

    NagSuppressions.addResourceSuppressions(pkLookupHandlerExecutionPolicy, [
      {
        'id': 'AwsSolutions-IAM5',
        'reason': 'The policy is required for the Lambda function to access the Cloudwatch logs \
        and the log permissions requires a wildcard for log stream creation and log events.',
      },
    ]);

    const pkLookupHandlerDataApiPolicy = new iam.ManagedPolicy(this, 'pkLookupHandlerDataApiPolicy', {
      statements: [
        new iam.PolicyStatement({
          sid: 'RedshiftDataExecute',
          actions: [
            'redshift-data:ExecuteStatement',
          ],
          resources: [
            props.redshiftWorkgroup.attrWorkgroupWorkgroupArn,
          ],
        }),
        new iam.PolicyStatement({
          sid: 'RedshiftDataGetResults',
          actions: [
            'redshift-data:DescribeStatement',
            'redshift-data:GetStatementResult',
          ],
          resources: [
            '*'
          ],
          conditions: {
            StringEquals: {
              'redshift-data:statement-owner-iam-userid': '${aws:userid}',
            }
          }
        }),
      ],
    });

    pkLookupHandlerRole.addManagedPolicy(pkLookupHandlerDataApiPolicy);

    NagSuppressions.addResourceSuppressions(pkLookupHandlerDataApiPolicy, [
      {
        'id': 'AwsSolutions-IAM5',
        'reason': 'The policy is required for the Lambda function to access the Redshift query results \
        which does not support a resource restriction but a condition is used to restrict access to the \
        user who executed the query.',
      },
    ]);

    const pkLookupHandlerSecretAccessPolicy = new iam.ManagedPolicy(this, 'pkLookupHandlerSecretAccessPolicy', {
      statements: [
        new iam.PolicyStatement({
          sid: 'RedshiftDataSecretAccess',
          actions: [
            'secretsmanager:GetSecretValue',
            'secretsmanager:DescribeSecret',
          ],
          resources: [
            `arn:aws:secretsmanager:${Stack.of(this).region}:${Stack.of(this).account}:secret:${props.redshiftSecretName ?? `redshift!${props.redshiftWorkgroup.attrWorkgroupWorkgroupName}`}-admin*`,
          ],
        }),
      ],
    });

    pkLookupHandlerRole.addManagedPolicy(pkLookupHandlerSecretAccessPolicy);

    NagSuppressions.addResourceSuppressions(pkLookupHandlerSecretAccessPolicy, [
      {
        'id': 'AwsSolutions-IAM5',
        'reason': 'The policy is required for the Lambda function to access the Redshift connection secret \
        which requires finding the full arn from the secret name. So we must wildcard the unknown portion.',
      },
    ]);

    const pkLookupHandler = new lambda.Function(this, 'PkLookup', {
      functionName: `${props.renovoLiveEnv}-renovolive-pk-lookup-handler`,
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'index.handler',
      code: lambda.Code.fromAsset(path.join(__dirname, './handlers/primary_key_lookup_handler/')),
      environment: {
        REDSHIFT_WORKGROUP_NAME: props.redshiftWorkgroup.attrWorkgroupWorkgroupName,
        REDSHIFT_DATABASE: props.redshiftDatabaseName ?? 'renovolive',
        REDSHIFT_SECRET_NAME: props.redshiftSecretName ?? `redshift!${props.redshiftWorkgroup.attrWorkgroupWorkgroupName}-admin`,
      },
      logGroup: pkLookupHandlerLogGroup,
      role: pkLookupHandlerRole,
      reservedConcurrentExecutions: 100,
      timeout: Duration.seconds(300),
      memorySize: 128,
    })

    NagSuppressions.addResourceSuppressions(pkLookupHandler, [
      {
        'id': 'NIST.800.53.R5-LambdaDLQ',
        'reason': 'The Lambda function does not require a DLQ for this use case as it is run from \
        a step function and does not require a DLQ for error handling since Step Functions can be \
        redriven on failure.',
      },
      {
        'id': 'NIST.800.53.R5-LambdaInsideVPC',
        'reason': 'The Lambda function does not require VPC access for this use case and does not interact with any \
        resources that would be better accessed from within a VPC. Such as S3.',
      },
    ]);

    const eventQueuesAccessPolicy = new iam.ManagedPolicy(this, `${props.renovoLiveEnv}-eventQueuesAccessPolicy`, {
      statements: [
        new iam.PolicyStatement({
          actions: [
            'sqs:ChangeMessageVisibility',
            'sqs:ReceiveMessage',
            'sqs:DeleteMessage',
            'sqs:GetQueueAttributes',
            'sqs:SetQueueAttributes',
            'sqs:GetQueueUrl',
            'sqs:PurgeQueue',
          ],
          resources: [
            `arn:aws:sqs:${Stack.of(this).region}:${Stack.of(this).account}:${props.renovoLiveEnv}-*-full-load-object-queue`,
            `arn:aws:sqs:${Stack.of(this).region}:${Stack.of(this).account}:${props.renovoLiveEnv}-*-cdc-object-queue`,
          ],
        }),
      ]
    });

    NagSuppressions.addResourceSuppressions(eventQueuesAccessPolicy, [
      {
        'id': 'AwsSolutions-IAM5',
        'reason': 'The policy is required for the Glue crawler to access the SQS queues \
        to receive messages from the S3 event notifications. Wildcards are used to allow access to all queues \
        created by the stack, but the wildcard is limited to the database name within the full queue ARN.',
      },
    ]);

    role.addManagedPolicy(eventQueuesAccessPolicy);

    for (const db of props.databases) {
      const workflow = new gluecfn.CfnWorkflow(this, `glueWorkflow-${db.databaseName}`, {
        name: `${props.renovoLiveEnv}-renovolive-${db.databaseName}-workflow`,
        description: `Workflow for RL data for ${db.databaseName}`,
        maxConcurrentRuns: 1,
      });

      const fullLoadNotificationQueue = new sqs.Queue(this, `${props.renovoLiveEnv}-${db.databaseName}-full-load-object-queue`, {
        queueName: `${props.renovoLiveEnv}-${db.databaseName}-full-load-object-queue`,
        encryption: sqs.QueueEncryption.SQS_MANAGED,
        retentionPeriod: Duration.days(14),
        enforceSSL: true,
        deadLetterQueue: {
          maxReceiveCount: 3,
          queue: new sqs.Queue(this, `${props.renovoLiveEnv}-${db.databaseName}-full-load-object-dlq`, {
            queueName: `${props.renovoLiveEnv}-${db.databaseName}-full-load-object-dlq`,
            encryption: sqs.QueueEncryption.SQS_MANAGED,
            retentionPeriod: Duration.days(14),
            enforceSSL: true,
          }),
        }
      });

      props.rawIngestionTopic.addSubscription(new snsSubscriptions.SqsSubscription(fullLoadNotificationQueue, {
        filterPolicyWithMessageBody: {
          Records: sns.FilterOrPolicy.policy({
            s3: sns.FilterOrPolicy.policy({
              object: sns.FilterOrPolicy.policy({
                key: sns.FilterOrPolicy.filter(sns.SubscriptionFilter.stringFilter({
                  matchPrefixes: [`renovolive/${props.renovoLiveEnv}/${db.databaseName}/full_load/`],
                })),
              }),
            }),
          })
        }
      }));

      const crawlerFullLoad = new gluecfn.CfnCrawler(this, `${props.renovoLiveEnv}-${db.databaseName}-full-load`, {
        name: `${props.renovoLiveEnv}-${db.databaseName}-crawler-full-load`,
        role: role.roleArn,
        databaseName: `${props.renovoLiveEnv}-${db.databaseName.toLowerCase()}`,
        targets: {
          s3Targets: [
            {
              path: `s3://${props.rawDataBucket.bucketName}/renovolive/${props.renovoLiveEnv}/${db.databaseName}/full_load/`,
              sampleSize: 1,
              eventQueueArn: fullLoadNotificationQueue.queueArn,
            },
          ],
        },
        configuration: JSON.stringify({
          "Version": 1.0,
          "CrawlerOutput": {"Partitions": {"AddOrUpdateBehavior": "InheritFromTable"}}
        }),
        tablePrefix: `full_load_`,
        crawlerSecurityConfiguration: securityConfig.securityConfigurationName,
        recrawlPolicy: {
          recrawlBehavior: 'CRAWL_EVENT_MODE',
        },
        schedule: {
          scheduleExpression: 'cron(0/30 * * * ? *)',
        }
      });

      const cdcNotificationQueue = new sqs.Queue(this, `${props.renovoLiveEnv}-${db.databaseName}-cdc-object-queue`, {
        queueName: `${props.renovoLiveEnv}-${db.databaseName}-cdc-object-queue`,
        encryption: sqs.QueueEncryption.SQS_MANAGED,
        retentionPeriod: Duration.days(14),
        enforceSSL: true,
        deadLetterQueue: {
          maxReceiveCount: 3,
          queue: new sqs.Queue(this, `${props.renovoLiveEnv}-${db.databaseName}-cdc-object-dlq`, {
            queueName: `${props.renovoLiveEnv}-${db.databaseName}-cdc-object-dlq`,
            encryption: sqs.QueueEncryption.SQS_MANAGED,
            retentionPeriod: Duration.days(14),
            enforceSSL: true,
          }),
        }
      });

      props.rawIngestionTopic.addSubscription(new snsSubscriptions.SqsSubscription(cdcNotificationQueue, {
        filterPolicyWithMessageBody: {
          Records: sns.FilterOrPolicy.policy({
            s3: sns.FilterOrPolicy.policy({
              object: sns.FilterOrPolicy.policy({
                key: sns.FilterOrPolicy.filter(sns.SubscriptionFilter.stringFilter({
                  matchPrefixes: [`renovolive/${props.renovoLiveEnv}/${db.databaseName}/cdc/`],
                })),
              }),
            }),
          })
        },
      }));

      const crawlerCdc = new gluecfn.CfnCrawler(this, `${props.renovoLiveEnv}-${db.databaseName}-cdc`, {
        name: `${props.renovoLiveEnv}-${db.databaseName}-crawler-cdc`,
        role: role.roleArn,
        databaseName: `${props.renovoLiveEnv}-${db.databaseName.toLowerCase()}`,
        targets: {
          s3Targets: [
            {
              path: `s3://${props.rawDataBucket.bucketName}/renovolive/${props.renovoLiveEnv}/${db.databaseName}/cdc/`,
              eventQueueArn: cdcNotificationQueue.queueArn,
            },
          ],
        },
        tablePrefix: `cdc_`,
        /**
         * Set table level configuration to 5 to avoid tables being split into multiple tables.
         * The number correlates to the absolute path in the S3 bucket so for example:
         * 
         * renovolive[1]/dev[2]/clrenovo[3]/load_type[4]/dbo[5]/table[6]
         */
        configuration: JSON.stringify({
          "Version": 1.0,
          "Grouping": {
            "TableLevelConfiguration": 6
          },
          "CrawlerOutput": {"Tables": {"AddOrUpdateBehavior": "MergeNewColumns"}}
        }),
        crawlerSecurityConfiguration: securityConfig.securityConfigurationName,
        recrawlPolicy: {
          recrawlBehavior: 'CRAWL_EVENT_MODE',
        },
        schedule: {
          scheduleExpression: 'cron(0/30 * * * ? *)',
        }
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

      const stepFunctionsRole = new iam.Role(this, `${db.databaseName}-StateMachineRole`, {
        assumedBy: new iam.ServicePrincipal('states.amazonaws.com'),
      });

      /**
       * replace trust policy to address confused deputy
       * 
       * @see https://docs.aws.amazon.com/step-functions/latest/dg/procedure-create-iam-role.html#prevent-cross-service-confused-deputy
       */
      const stepFunctionsRoleCfn = stepFunctionsRole.node.defaultChild as iam.CfnRole;

      stepFunctionsRoleCfn.assumeRolePolicyDocument = {
        Version: '2012-10-17',
        Statement: [
          {
            Effect: 'Allow',
            Principal: {
              Service: 'states.amazonaws.com',
            },
            Action: 'sts:AssumeRole',
            Condition: {
              ArnLike: {
                'aws:SourceArn': `arn:aws:states:${Stack.of(this).region}:${Stack.of(this).account}:stateMachine:*`,
              },
              StringEquals: {
                'aws:SourceAccount': Stack.of(this).account,
              },
            }
          },
        ],
      };

      const stepFunctionLogs = new logs.LogGroup(this, `${db.databaseName}-DataLoaderStateMachineLogGroup`, {
        logGroupName: `/renovolive/etl/${props.renovoLiveEnv}/${db.databaseName}/state-machine/dataloader`,
        removalPolicy: RemovalPolicy.DESTROY,
        retention: logs.RetentionDays.TWO_YEARS,
        encryptionKey: logsKey,
      });

      const startState = new stepfunctions.Pass(this, `${db.databaseName}-StartState`, {
        parameters: {
          "databaseName.$": `$.detail.databaseName`,
          "changedTables.$": `$.detail.changedTables`,
        },
      });

      const tableCreationEventHandlerTask = new tasks.LambdaInvoke(this, `${db.databaseName}-TableCreationEventHandler`, {
        stateName: 'TableCreationEventProcessor',
        lambdaFunction: tableCreationEventHandler,
        payload: stepfunctions.TaskInput.fromObject({
          database: stepfunctions.JsonPath.stringAt('$.databaseName'),
          tables: stepfunctions.JsonPath.stringAt('$.changedTables'),
        }),
        outputPath: '$.Payload',
      });

      const getTableTask = new tasks.CallAwsService(this, `${db.databaseName}-GetFullLoadTableDetailsTask`, {
        stateName: 'GetFullLoadTableDetails',
        service: 'glue',
        action: 'getTable',
        parameters: {
          DatabaseName: `${props.renovoLiveEnv}-${db.databaseName.toLowerCase()}`,
          Name: stepfunctions.JsonPath.stringAt('$.full_load_table'),
        },
        resultSelector: {
          'database.$': '$.Table.DatabaseName',
          'table.$': '$.Table.Name',
          'columns.$': '$.Table.StorageDescriptor.Columns[*].Name',
        },
        resultPath: '$.tableDetails',
        iamResources: [
          `arn:aws:glue:${Stack.of(this).region}:${Stack.of(this).account}:database/${props.renovoLiveEnv}-${db.databaseName.toLowerCase()}`,
          `arn:aws:glue:${Stack.of(this).region}:${Stack.of(this).account}:table/${props.renovoLiveEnv}-${db.databaseName.toLowerCase()}/*`,
          `arn:aws:glue:${Stack.of(this).region}:${Stack.of(this).account}:catalog`,
        ]
      });

      const primaryKeyLookupTask = new tasks.LambdaInvoke(this, `${db.databaseName}-PrimaryKeyLookupTask`, {
        stateName: 'PrimaryKeyLookup',
        lambdaFunction: pkLookupHandler,
        payload: stepfunctions.TaskInput.fromObject({
          database: stepfunctions.JsonPath.stringAt('$.database'),
          table: stepfunctions.JsonPath.stringAt('$.table'),
        }),
        resultSelector: {
          'primaryKeys.$': '$.Payload.primary_keys',
        },
        resultPath: '$.primaryKeyDetails',
      });

      const snapshotTableCreationTask = new tasks.LambdaInvoke(this, `${db.databaseName}-SnapshotTableCreationTask`, {
        stateName: 'SnapshotTableCreation',
        lambdaFunction: snapshotTableCreationHandler,
        payload: stepfunctions.TaskInput.fromObject({
          database: stepfunctions.JsonPath.stringAt('$.database'),
          table: stepfunctions.JsonPath.listAt('$.table'),
          pks: stepfunctions.JsonPath.listAt('$.primaryKeyDetails.primaryKeys'),
          columns: stepfunctions.JsonPath.listAt('$.tableDetails.columns'),
          event_table_type: stepfunctions.JsonPath.stringAt('$.event_table_type'),
        }),
        resultSelector: {
          'payload.$': '$.Payload',
        },
        resultPath: '$.SnapshotCreationDetails',
      });

      snapshotTableCreationTask.addRetry({
        backoffRate: 2,
        interval: Duration.seconds(10),
        maxAttempts: 5,
      })

      const realtimeViewCreationTask = new tasks.LambdaInvoke(this, `${db.databaseName}-RealtimeViewTask`, {
        stateName: 'RealtimeViewCreation',
        lambdaFunction: realtimeViewCreationHandler,
        payload: stepfunctions.TaskInput.fromObject({
          database: stepfunctions.JsonPath.stringAt('$.database'),
          table: stepfunctions.JsonPath.listAt('$.table'),
          pks: stepfunctions.JsonPath.listAt('$.primaryKeyDetails.primaryKeys'),
          columns: stepfunctions.JsonPath.listAt('$.tableDetails.columns'),
          event_table_type: stepfunctions.JsonPath.stringAt('$.event_table_type'),
        }),
        resultSelector: {
          'payload.$': '$.Payload',
        },
        resultPath: '$.ViewCreationDetails',
      });

      realtimeViewCreationTask.addRetry({
        backoffRate: 2,
        interval: Duration.seconds(10),
        maxAttempts: 5,
      })

      const definition = startState
        .next(tableCreationEventHandlerTask)
        .next(getTableTask)
        .next(primaryKeyLookupTask)
        .next(snapshotTableCreationTask)
        .next(realtimeViewCreationTask)

      const stateMachine = new stepfunctions.StateMachine(this, `${db.databaseName}-RenovoLiveRedshiftDataLoader`, {
        stateMachineName: `${props.renovoLiveEnv}-renovolive-${db.databaseName}-redshift-data-loader`,
        definitionBody: stepfunctions.DefinitionBody.fromChainable(definition),
        logs: {
          destination: stepFunctionLogs,
          level: stepfunctions.LogLevel.ALL,
        },
        tracingEnabled: true,
        encryptionConfiguration: new stepfunctions.CustomerManagedEncryptionConfiguration(props.dataKmsKey),
        role: stepFunctionsRole,
      });

      NagSuppressions.addResourceSuppressionsByPath(this, `${stateMachine.role.node.path}/DefaultPolicy/Resource`, [
        {
          id: 'AwsSolutions-IAM5',
          reason: 'The state machine role is automatically assigned a policy by the CDK. \
          This policy is used for the state machine to work with the resources it will interact with. \
          We apply restrictions to avoid the confused deputy problem and the policy decisions automatically \
          are not overly permissive.',
        },
        {
          id: 'NIST.800.53.R5-IAMNoInlinePolicy',
          reason: 'The state machine role is automatically assigned a policy by the CDK. \
          This policy is used for the state machine to work with the resources it will interact with. \
          We apply restrictions to avoid the confused deputy problem and the policy decisions automatically \
          are not overly permissive.',
        },
      ]);

      const eventsRole = new iam.Role(this, `${db.databaseName}-EventsRole`, {
        assumedBy: new iam.ServicePrincipal('events.amazonaws.com'),
      });

      const startExecutionPolicy = new iam.ManagedPolicy(this, `${db.databaseName}-StartExecutionPolicy`, {
        statements: [
          new iam.PolicyStatement({
            effect: iam.Effect.ALLOW,
            actions: ['states:StartExecution'],
            resources: [stateMachine.stateMachineArn],
          }),
        ],
      });

      eventsRole.addManagedPolicy(startExecutionPolicy);

      // EventBridge rule to trigger Step Function on Glue table creation
      new events.Rule(this, `${db.databaseName}-TableCreationRule`, {
        eventPattern: {
          source: ['aws.glue'],
          detailType: ['Glue Data Catalog Database State Change'],
          detail: {
            "databaseName": [`${props.renovoLiveEnv}-${db.databaseName.toLowerCase()}`],
            "typeOfChange": ["CreateTable"]
          }
        },
        targets: [new targets.SfnStateMachine(stateMachine, {role: eventsRole.withoutPolicyUpdates()})],
      });

      const startStateManualRefreshAll = new stepfunctions.Pass(this, `${db.databaseName}-StartStateManualRefreshAll`, {});

      const getTablesTask = new tasks.CallAwsService(this, `${db.databaseName}-GetTablesTask`, {
        service: 'glue',
        action: 'getTables',
        parameters: {
          DatabaseName: `${props.renovoLiveEnv}-${db.databaseName.toLowerCase()}`,
          Expression: 'full_load_*', // Only get full load tables. There should be no CDC only cases
        },
        resultPath: '$.Tables',
        iamResources: [
          `arn:aws:glue:${Stack.of(this).region}:${Stack.of(this).account}:database/${props.renovoLiveEnv}-${db.databaseName.toLowerCase()}`,
          `arn:aws:glue:${Stack.of(this).region}:${Stack.of(this).account}:table/${props.renovoLiveEnv}-${db.databaseName.toLowerCase()}/*`,
          `arn:aws:glue:${Stack.of(this).region}:${Stack.of(this).account}:catalog`,
        ]
      });

      const mapTables = new stepfunctions.Map(this, `${db.databaseName}-MapTablesState`, {
        itemsPath: stepfunctions.JsonPath.stringAt('$.Tables.TableList'),
        resultPath: '$.detail',
      });

      const mapTablesChain = new stepfunctions.Pass(this, `${db.databaseName}-MapTablesItemProcessor`, {});

      const getTableTaskForRecreate = new tasks.CallAwsService(this, `${db.databaseName}-GetFullLoadTableDetailsTaskForRecreate`, {
        stateName: 'GetFullLoadTableDetails',
        service: 'glue',
        action: 'getTable',
        parameters: {
          DatabaseName: stepfunctions.JsonPath.stringAt('$.DatabaseName'),
          Name: stepfunctions.JsonPath.stringAt('$.Name'),
        },
        resultSelector: {
          'database.$': '$.Table.DatabaseName',
          'table.$': '$.Table.Name',
          'columns.$': '$.Table.StorageDescriptor.Columns[*].Name',
        },
        resultPath: '$.tableDetails',
        iamResources: [
          `arn:aws:glue:${Stack.of(this).region}:${Stack.of(this).account}:database/${props.renovoLiveEnv}-${db.databaseName.toLowerCase()}`,
          `arn:aws:glue:${Stack.of(this).region}:${Stack.of(this).account}:table/${props.renovoLiveEnv}-${db.databaseName.toLowerCase()}/*`,
          `arn:aws:glue:${Stack.of(this).region}:${Stack.of(this).account}:catalog`,
        ]
      });

      const primaryKeyLookupTaskForRecreate = new tasks.LambdaInvoke(this, `${db.databaseName}-PrimaryKeyLookupTaskForRecreate`, {
        stateName: 'PrimaryKeyLookup',
        lambdaFunction: pkLookupHandler,
        payload: stepfunctions.TaskInput.fromObject({
          database: stepfunctions.JsonPath.stringAt('$.DatabaseName'),
          table: stepfunctions.JsonPath.stringAt('$.Name'),
        }),
        resultSelector: {
          'primaryKeys.$': '$.Payload.primary_keys',
        },
        resultPath: '$.primaryKeyDetails',
      });

      const snapshotTableRecreateTask = new tasks.LambdaInvoke(this, `${db.databaseName}-SnapshotTableRecreateTask`, {
        lambdaFunction: snapshotTableCreationHandler,
        payload: stepfunctions.TaskInput.fromObject({
          database: stepfunctions.JsonPath.stringAt('$.DatabaseName'),
          table: stepfunctions.JsonPath.stringAt('$.Name'),
          pks: stepfunctions.JsonPath.stringAt('$.primaryKeyDetails.primaryKeys'),
          columns: stepfunctions.JsonPath.listAt('$.tableDetails.columns'),
          event_table_type: 'full_load',
        }),
        resultSelector: {
          'payload.$': '$.Payload',
        },
        resultPath: '$.SnapshotCreationDetails',
      });

      snapshotTableRecreateTask.addRetry({
        backoffRate: 2,
        interval: Duration.seconds(10),
        maxAttempts: 5,
      })

      const realtimeViewRefreshTask = new tasks.LambdaInvoke(this, `${db.databaseName}-RealtimeViewTaskRefresh`, {
        lambdaFunction: realtimeViewCreationHandler,
        payload: stepfunctions.TaskInput.fromObject({
          database: stepfunctions.JsonPath.stringAt('$.DatabaseName'),
          table: stepfunctions.JsonPath.listAt('$.Name'),
          pks: stepfunctions.JsonPath.listAt('$.primaryKeyDetails.primaryKeys'),
          columns: stepfunctions.JsonPath.listAt('$.tableDetails.columns'),
          event_table_type: 'full_load',
        }),
        resultSelector: {
          'payload.$': '$.Payload',
        },
        resultPath: '$.ViewCreationDetails',
      });

      realtimeViewRefreshTask.addRetry({
        backoffRate: 2,
        interval: Duration.seconds(10),
        maxAttempts: 5,
      })

      mapTables.itemProcessor(
        mapTablesChain
        .next(getTableTaskForRecreate)
        .next(primaryKeyLookupTaskForRecreate)
        .next(realtimeViewRefreshTask)
        .next(snapshotTableRecreateTask)
      );

      const manualRefreshAllDefinition = startStateManualRefreshAll
        .next(getTablesTask)
        .next(mapTables);

      const refreshAllStateMachine = new stepfunctions.StateMachine(this, `${db.databaseName}-RenovoLiveRedshiftDataLoaderRefreshAll`, {
        stateMachineName: `${props.renovoLiveEnv}-renovolive-${db.databaseName}-redshift-data-loader-refresh-all`,
        definitionBody: stepfunctions.DefinitionBody.fromChainable(manualRefreshAllDefinition),
        logs: {
          destination: stepFunctionLogs,
          level: stepfunctions.LogLevel.ALL,
        },
        tracingEnabled: true,
        encryptionConfiguration: new stepfunctions.CustomerManagedEncryptionConfiguration(props.dataKmsKey),
        role: stepFunctionsRole,
      });

      const refreshAllStartExecutionPolicy = new iam.ManagedPolicy(this, `${db.databaseName}-RefreshAllStartExecutionPolicy`, {
        statements: [
          new iam.PolicyStatement({
            effect: iam.Effect.ALLOW,
            actions: ['states:StartExecution'],
            resources: [refreshAllStateMachine.stateMachineArn],
          }),
        ],
      });

      eventsRole.addManagedPolicy(refreshAllStartExecutionPolicy);

      new scheduler.Schedule(this, `${db.databaseName}-NightlyRefreshSchedule`, {
        scheduleName: `${props.renovoLiveEnv}-renovolive-${db.databaseName}-nightly-refresh`,
        description: `Nightly refresh schedule for ${db.databaseName}`,
        schedule: scheduler.ScheduleExpression.rate(Duration.days(1)),
        timeWindow: scheduler.TimeWindow.flexible(Duration.hours(1)),
        target: new schedulerTargets.StepFunctionsStartExecution(refreshAllStateMachine, {}),
      });
    }
  }
}
