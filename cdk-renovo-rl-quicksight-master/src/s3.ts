import {
  Stack,
  StackProps,
  aws_s3 as s3,
  aws_kms as kms,
  RemovalPolicy,
  Duration,
  aws_iam as iam,
  aws_s3_deployment as s3deployment,
  aws_sns as sns,
  aws_s3_notifications as s3notifications,
} from 'aws-cdk-lib';
import { NagSuppressions } from 'cdk-nag';
import { Construct } from 'constructs';
import * as path from 'path';

export interface RawDataBucketProps extends s3.BucketProps {
  /**
   * The KMS key to use for encryption.
   */
  readonly encryptionKey: kms.IKey;
  /**
   * The bucket to store access logs in.
   */
  readonly accessLogBucket?: s3.IBucket;
  /**
   * The prefix for access logs.
   */
  readonly accessLogPrefix?: string;
}

export class RawDataBucket extends s3.Bucket {
  constructor(scope: Construct, id: string, props: RawDataBucketProps) {
    super(scope, id, {
      ...props,
      encryption: s3.BucketEncryption.KMS,
      encryptionKey: props.encryptionKey,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      enforceSSL: true,
      bucketKeyEnabled: true,
      versioned: false,
      serverAccessLogsBucket: props.accessLogBucket,
      serverAccessLogsPrefix: props.accessLogPrefix,
    });

    NagSuppressions.addResourceSuppressions(this, [
      {
        id: 'NIST.800.53.R5-S3BucketReplicationEnabled',
        reason: 'Replication is not required for this bucket since it is used for raw data transfered from a data source that is already replicated and re-creating\
        the data is easy if lost.',
      },
      {
        id: 'NIST.800.53.R5-S3BucketVersioningEnabled',
        reason: 'Versioning is not required for this bucket since it is used for raw data transfer as part of a data pipeline and changes to this data are expected but \
        reverting to a previous version is not necessary. Retrieving the data from the source is sufficient and restoration at the source would be the path for recovery.',
      },
    ]);

    this.addToResourcePolicy(new iam.PolicyStatement({
      actions: ['s3:PutObject'],
      resources: [this.arnForObjects('*')],
      principals: [new iam.AnyPrincipal()],
      effect: iam.Effect.DENY,
      sid: 'DenyUnEncryptedObjectUploads',
      conditions: {
        StringNotEqualsIfExists: {
          "s3:x-amz-server-side-encryption": "aws:kms",
        },
        Null: {
          "s3:x-amz-server-side-encryption": false,
        }
      }
    }));

    this.addToResourcePolicy(new iam.PolicyStatement({
      actions: ['s3:PutObject'],
      resources: [this.arnForObjects('*')],
      principals: [new iam.AnyPrincipal()],
      effect: iam.Effect.DENY,
      sid: 'DenyObjectUploadsWithWrongKMSKey',
      conditions: {
        StringNotEquals: {
          "s3:x-amz-server-side-encryption-aws-kms-key-id": props.encryptionKey.keyArn,
        }
      }
    }));
  }
}

export interface DataBucketsStackProps extends StackProps {
  /**
   * The name of the raw data bucket
   */
  readonly rawDataBucketName: string;
  /**
   * The name of the glue scripts bucket
   */
  readonly glueScriptsBucketName: string;
  /**
   * The name of the glue scratch bucket
   */
  readonly glueScratchBucketName: string;
  /**
   * The KMS key alias to use for data encryption.
   */
  readonly kmsKeysNamespace: string;
}

export class DataBucketsStack extends Stack {
  /**
   * The S3 bucket for raw data.
   */
  public readonly rawDataBucket: s3.Bucket;
  /**
   * The access logs bucket.
   */
  public readonly accessLogsBucket: s3.Bucket;
  /**
   * The Glue scripts bucket
   */
  public readonly glueScriptsBucket: s3.Bucket;
  /**
   * The Glue scratch bucket used for temporary storage.
   */
  public readonly glueScratchBucket: s3.Bucket;
  /**
   * The KMS key for data encryption.
   */
  public readonly dataKmsKey: kms.IKey;
  /**
   * The KMS key for access logs encryption.
   */
  public readonly accessLogsKmsKey: kms.IKey;
  /**
   * The SNS topic where notifications are sent for the raw data bucket.
   */
  public readonly rawIngestionTopic: sns.Topic;

  constructor(scope: Construct, id: string, props: DataBucketsStackProps) {
    super(scope, id, props);

    this.accessLogsKmsKey = new kms.Key(this, 'AccessLogsEncryptionKey', {
      alias: `${props.kmsKeysNamespace}/access-logs`,
      enableKeyRotation: true,
    });

    this.dataKmsKey = new kms.Key(this, 'DataEncryptionKey', {
      alias: `${props.kmsKeysNamespace}/data`,
      enableKeyRotation: true,
    });

    this.accessLogsBucket = new s3.Bucket(this, 'AccessLogsBucket', {
      removalPolicy: RemovalPolicy.RETAIN,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      bucketKeyEnabled: true,
      versioned: true,
      encryption: s3.BucketEncryption.KMS,
      encryptionKey: this.accessLogsKmsKey,
      lifecycleRules: [
        {
          noncurrentVersionExpiration: Duration.days(30),
          transitions: [
            {
              storageClass: s3.StorageClass.INTELLIGENT_TIERING,
              transitionAfter: Duration.days(30),
            }
          ]
        }
      ],
      enforceSSL: true,
    });

    this.accessLogsBucket.addToResourcePolicy(new iam.PolicyStatement({
      actions: ['s3:PutObject'],
      resources: [this.accessLogsBucket.arnForObjects('*')],
      principals: [new iam.AnyPrincipal()],
      effect: iam.Effect.DENY,
      sid: 'DenyUnEncryptedObjectUploads',
      conditions: {
        StringNotEqualsIfExists: {
          "s3:x-amz-server-side-encryption": "aws:kms",
        },
        Null: {
          "s3:x-amz-server-side-encryption": false,
        }
      }
    }));

    this.accessLogsBucket.addToResourcePolicy(new iam.PolicyStatement({
      actions: ['s3:PutObject'],
      resources: [this.accessLogsBucket.arnForObjects('*')],
      principals: [new iam.AnyPrincipal()],
      effect: iam.Effect.DENY,
      sid: 'DenydObjectUploadsWithWrongKMSKey',
      conditions: {
        StringNotEquals: {
          "s3:x-amz-server-side-encryption-aws-kms-key-id": this.accessLogsKmsKey.keyArn,
        }
      }
    }));
  
    NagSuppressions.addResourceSuppressions(this.accessLogsBucket, [
      {
        id: 'NIST.800.53.R5-S3BucketReplicationEnabled',
        reason: 'This bucket is used for access logs and it not currently a target for us to replicate.',
      },
    ]);

    const notificationsHandlerRole = new iam.Role(this, 'NotificationsHandlerRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
    });

    const notificationsHandlerRolePolicy = new iam.ManagedPolicy(this, 'NotificationsHandlerRolePolicy', {
      statements: [
        new iam.PolicyStatement({
          actions: ['s3:PutBucketNotification'],
          resources: [`arn:aws:s3:::${props.rawDataBucketName}`],
        }),
        new iam.PolicyStatement({
          actions: [
            'logs:CreateLogGroup',
            'logs:CreateLogStream',
            'logs:PutLogEvents',
          ],
          resources: [
            `arn:aws:logs:${this.region}:${this.account}:log-group:/aws/lambda/*`,
            `arn:aws:logs:${this.region}:${this.account}:log-group:/aws/lambda/*:*`,
          ],
        })
      ],
    });

    notificationsHandlerRole.addManagedPolicy(notificationsHandlerRolePolicy);

    NagSuppressions.addResourceSuppressions(notificationsHandlerRolePolicy, [
      {
        'id': 'AwsSolutions-IAM5',
        'reason': 'Wildcard permissions are expected in Lambda basic execution roles in order to allow for automatic management of logs. \
        but the permissions are limited to the default lambda logging paths and not all logs.',
      }
    ]);

    this.rawDataBucket = new RawDataBucket(this, 'RawDataBucket', {
      bucketName: props.rawDataBucketName,
      encryptionKey: this.dataKmsKey,
      accessLogBucket: this.accessLogsBucket,
      accessLogPrefix: `${props.rawDataBucketName}/`,
      notificationsHandlerRole: notificationsHandlerRole.withoutPolicyUpdates(),
    });

    this.rawIngestionTopic = new sns.Topic(this, 'RawDataBucketNotificationsTopic', {
      enforceSSL: true,
    });

    NagSuppressions.addResourceSuppressions(this.rawIngestionTopic, [
      {
        'id': 'AwsSolutions-SNS2',
        'reason': 'This queue will not contain any sensitive data and is just \
        used for notifications for the raw data bucket that is getting timestamped files \
        from DMS',
      },
      {
        'id': 'NIST.800.53.R5-SNSEncryptedKMS',
        'reason': 'This queue will not contain any sensitive data and is just \
        used for notifications for the raw data bucket that is getting timestamped files \
        from DMS',
      }
    ]);

    this.rawDataBucket.addEventNotification(s3.EventType.OBJECT_CREATED, new s3notifications.SnsDestination(this.rawIngestionTopic));

    /**
     * Treated like a raw data bucket since scripts should be sourced
     * from Git and not stored in the bucket.
     */
    this.glueScriptsBucket = new RawDataBucket(this, 'GlueScriptsBucket', {
      bucketName: props.glueScriptsBucketName,
      encryptionKey: this.dataKmsKey,
      accessLogBucket: this.accessLogsBucket,
      accessLogPrefix: `${props.glueScriptsBucketName}/`,
    });

    this.glueScratchBucket = new RawDataBucket(this, 'GlueScratchBucket', {
      bucketName: props.glueScratchBucketName,
      encryptionKey: this.dataKmsKey,
      accessLogBucket: this.accessLogsBucket,
      accessLogPrefix: props.glueScriptsBucketName,
      versioned: false,
      lifecycleRules: [
        {
          expiration: Duration.days(7),
          noncurrentVersionExpiration: Duration.days(7),
          abortIncompleteMultipartUploadAfter: Duration.days(1),
        }
      ]
    });

    const deployment = new s3deployment.BucketDeployment(this, 'DeployGlueScripts', {
      sources: [s3deployment.Source.asset(path.join(__dirname, '../src/glue-scripts'))],
      destinationBucket: this.glueScriptsBucket,
      retainOnDelete: true,
      destinationKeyPrefix: 'deployed-scripts/',
    });

    /**
     * Slightly hacky way to suppress the CDK Nag warnings for the deployment
     * without using a hardcoded resource path.
     */
    const suppressionPath = deployment.handlerRole.node.path.split('/').slice(0,2).join('/');

    NagSuppressions.addResourceSuppressionsByPath(this,
      suppressionPath,
      [
        {
          id: 'AwsSolutions-IAM4',
          reason: 'This is an AWS authored construct for deploying data to S3 and some \
          configuration is beyond our direct control.',
        },
        {
          id: 'AwsSolutions-IAM5',
          reason: 'Wildcard permissions are expected in Lambda basic execution roles since \
          automatic management of logs is expected which involves creation of a log group.',
        },
        {
          id: 'NIST.800.53.R5-IAMNoInlinePolicy',
          reason: 'This is an AWS authored construct for deploying data to S3 and and it \
          leverages the CDK grant behavior that creates inline policies.',
        },
        {
          id: 'AwsSolutions-L1',
          reason: 'This is an AWS authored construct for deploying data to S3 and the runtime \
          version is managed by the CDK.',
        },
        {
          id: 'NIST.800.53.R5-LambdaConcurrency',
          reason: 'This is an AWS authored construct for deploying data to S3 and concurrency \
          is of no concern given its only triggered during an infrastructure deployment which \
          is not expected to be concurrent.',
        },
        {
          id: 'NIST.800.53.R5-LambdaDLQ',
          reason: 'This is an AWS authored construct for deploying data to S3 and is only run \
          during an infrastructure deployment and does not require a DLQ.',
        },
        {
          id: 'NIST.800.53.R5-LambdaInsideVPC',
          reason: 'This is an AWS authored construct for deploying data to S3 and has no need \
          for VPC access.',
        },
      ], 
      true
    );
  }
}

// export interface S3StackProps extends StackProps {
//   readonly bucketNamePrefix: string;
// }

// export class S3Stack extends Stack {
//   public readonly rawBucket: s3.Bucket;
//   public readonly glueScriptsBucket: s3.Bucket;

//   constructor(scope: Construct, id: string, props: S3StackProps) {
//     super(scope, id, props);

//     this.rawBucket = new s3.Bucket(this, 'RawBucket', {
//       bucketName: `${props.bucketNamePrefix}-${this.account}-${this.region}`,
//       encryption: s3.BucketEncryption.S3_MANAGED,
//       versioned: false,
//       blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
//       enforceSSL: true,
//     });

//     this.glueScriptsBucket = new s3.Bucket(this, 'GlueScriptsBucket', {
//       encryption: s3.BucketEncryption.S3_MANAGED,
//       versioned: true,
//       blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
//       removalPolicy: cdk.RemovalPolicy.DESTROY,
//       enforceSSL: true,
//     });
//   }
// }
