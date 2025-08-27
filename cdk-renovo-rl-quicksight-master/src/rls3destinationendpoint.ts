import {
  aws_dms as dms,
  aws_s3 as s3,
  aws_iam as iam,
  aws_kms as kms,
} from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { RenovoLiveEnv } from './common';
import { DmsTaskType } from './dms';

export interface RlS3DestinationCfnEndpointProps extends Omit<dms.CfnEndpointProps, 'endpointType' | 'engineName'> {
  /**
  * The SQL Server database name that will be used as the prefix for the S3 folder
  */
  sourceDatabaseName: string;
  /**
  * The S3 bucket that will be used as the target
  */
  bucket: s3.IBucket;
  /**
  * The IAM role that will be used to access the S3 bucket
  */
  role: iam.IRole;
  /**
   * The RenovoLive env
   */
  renovoLiveEnv: RenovoLiveEnv;
  /**
   * The KMS encryption key ID to use for the S3 destination
   */
  kmsKey: kms.IKey;
  /**
   * The type of CDC task that will be run with this endpoint
   */
  taskType: DmsTaskType;
}

export class RlS3DestinationCfnEndpoint extends dms.CfnEndpoint {
  constructor(scope: Construct, id: string, props: RlS3DestinationCfnEndpointProps) {
    const endpointIdentifier = `${props.renovoLiveEnv}-rl-s3-${props.sourceDatabaseName.toLowerCase()}-${props.taskType.replace('-', '')}`;
    const endpointType = 'target';
    const engineName = 's3';

    // Add the RENOVOLIVE prefix to the S3 bucket folder
    const bucketFolder = `renovolive/${props.renovoLiveEnv}/${props.sourceDatabaseName}/${props.taskType.replace('-', '_')}/`;

    const datePartitionEnabled = props.taskType === DmsTaskType.CDC;

    const s3Settings = {
      csvRowDelimiter: '\n',
      csvDelimiter: ',',
      compressionType: 'none',
      dataFormat: 'parquet',
      dictPageSizeLimit: 3072000,
      encodingType: 'plain-dictionary',
      enableStatistics: false,
      datePartitionEnabled,
      addColumnName: true,
      serviceAccessRoleArn: props.role.roleArn,
      bucketName: props.bucket.bucketName,
      bucketFolder: bucketFolder,
      parquetTimestampInMillisecond: true,
      timestampColumnName: 'dms_timestamp',
      includeOpsForFullLoad: true,
      encryptionMode: 'SSE_KMS',
      serverSideEncryptionKmsKeyId: props.kmsKey.keyArn,
      includeOpForFullLoad: true,
    };

    super(scope, id, {
      endpointIdentifier,
      endpointType,
      engineName,
      s3Settings,
      ...props,
    });
  }
}
