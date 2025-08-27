import * as cdk from 'aws-cdk-lib';
import {
  aws_ec2 as ec2,
  aws_iam as iam,
  aws_s3 as s3,
  aws_kms as kms,
  aws_glue as gluecfn,
  aws_redshiftserverless as redshiftserverless,
  aws_secretsmanager as secretsmanager,
  Stack,
  StackProps,
} from 'aws-cdk-lib';
import { RenovoLiveEnv } from './common';
import { NagSuppressions } from 'cdk-nag';
import * as glue from '@aws-cdk/aws-glue-alpha';
import { DatabaseDetails } from './common';

export interface RenovoLiveRedshiftStackProps extends StackProps {
  /**
   * The ID of the VPC where the Redshift serverless cluster will be deployed
   */
  readonly vpcId: string;
  /**
   * The RenovoLive env this Redshift serverless cluster will be deployed for
   */
  readonly renovoLiveEnv: RenovoLiveEnv;
  /**
   * The S3 bucket that will be used as the raw data source
   */
  readonly rawBucket: s3.IBucket;
  /**
   * The KMS key used by the raw data bucket for encryption
   */
  readonly rawBucketKmsKey: kms.IKey;
  /**
   * The security group that QuickSight will use to connect to Redshift
   */
  readonly quickSightSecurityGroup: ec2.ISecurityGroup;
  /**
   * The databases being targeted by the Redshift serverless cluster
   */
  readonly databases: DatabaseDetails[];
}

export class RenovoLiveRedshiftStack extends Stack {
  /**
   * The Glue connection to the Redshift serverless cluster
   */
  public readonly connection: glue.IConnection;
  /**
   * The role used by the Redshift serverless workgroup
   */
  public readonly role: iam.IRole;
  /**
   * The secrets manager secret used by the Glue connection
   */
  public readonly connectionSecret: secretsmanager.ISecret;
  /**
   * The Redshift workgroup 
   */
  public readonly workgroup: redshiftserverless.CfnWorkgroup;

  constructor(scope: cdk.App, id: string, props: RenovoLiveRedshiftStackProps) {
    super(scope, id, props);

    const vpc = ec2.Vpc.fromLookup(this, 'vpc', {
      vpcId: props.vpcId,
    });

    const sg = new ec2.SecurityGroup(this, 'sg', {
      vpc,
      allowAllOutbound: true,
      description: 'Redshift security group',
    });

    const connectionSg = new ec2.SecurityGroup(this, 'glueConnectionSg', {
      vpc,
      allowAllOutbound: true,
      description: 'Glue connection security group',
    });

    sg.addIngressRule(ec2.Peer.securityGroupId(connectionSg.securityGroupId), ec2.Port.tcp(5439), 'allow Glue resources to connect to Redshift on port 5439');
    sg.addIngressRule(ec2.Peer.securityGroupId(props.quickSightSecurityGroup.securityGroupId), ec2.Port.tcp(5439), 'allow QuickSight to connect to Redshift on port 5439');
    connectionSg.addIngressRule(connectionSg, ec2.Port.allTcp(), 'allow all to to self');

    // IAM role for namespace with scoped policies
    this.role = new iam.Role(this, 'role', {
      assumedBy: new iam.CompositePrincipal(
        new iam.ServicePrincipal('redshift.amazonaws.com'),
        new iam.ServicePrincipal('redshift-serverless.amazonaws.com'),
      ),
    });

    let dbResources = [];
    for (const db of props.databases) {
      dbResources.push(`arn:aws:glue:${this.region}:${this.account}:database/${props.renovoLiveEnv}-${db.databaseName.toLowerCase()}`);
      dbResources.push(`arn:aws:glue:${this.region}:${this.account}:table/${props.renovoLiveEnv}-${db.databaseName.toLowerCase()}/*`);
    }

    const dataAccessPolicy = new iam.ManagedPolicy(this, 'dataAccessPolicy', {
      statements: [
        new iam.PolicyStatement({
          actions: [
            's3:ListBucket',
            's3:GetBucketAcl',
            's3:GetBucketCors',
            's3:GetBucketLocation',
            's3:GetEncryptionConfiguration',
          ],
          effect: iam.Effect.ALLOW,
          resources: [
            `arn:aws:s3:::${props.rawBucket.bucketName}`,
          ],
        }),
        new iam.PolicyStatement({
          actions: [
            's3:GetObject',
          ],
          effect: iam.Effect.ALLOW,
          resources: [
            `arn:aws:s3:::${props.rawBucket.bucketName}/renovolive/${props.renovoLiveEnv}/*`,
            `arn:aws:s3:::${props.rawBucket.bucketName}/renovolive/${props.renovoLiveEnv}/pks/*`
          ],
        }),
        new iam.PolicyStatement({
          sid: 'KMSKeyAccessForS3',
          actions: [
            'kms:Decrypt',
            'kms:Encrypt',
            'kms:GenerateDataKey',
          ],
          effect: iam.Effect.ALLOW,
          resources: [props.rawBucketKmsKey.keyArn],
        }),
        new iam.PolicyStatement({
          sid: 'GlueDataAccess',
          actions: [
            'glue:GetDatabase',
            'glue:GetDatabases',
            'glue:GetTable',
            'glue:GetTables',
            'glue:GetPartition',
            'glue:GetPartitions',
            'glue:BatchGetPartition',
          ],
          resources: [
            `arn:aws:glue:${this.region}:${this.account}:catalog`,
            `arn:aws:glue:${this.region}:${this.account}:database/${props.renovoLiveEnv}-renovolive-metadata`,
            `arn:aws:glue:${this.region}:${this.account}:table/${props.renovoLiveEnv}-renovolive-metadata/*`
          ].concat(dbResources),
          effect: iam.Effect.ALLOW,
        }),
      ],
    });

    NagSuppressions.addResourceSuppressions(dataAccessPolicy, [
      {
        id: 'AwsSolutions-IAM5',
        reason: 'This policy is required for the Redshift serverless cluster to access the raw data source in S3 and is scoped to the specific bucket and prefix',
      },
    ]);

    this.role.addManagedPolicy(dataAccessPolicy);

    // Namespace creation with ManageAdminPassword
    const namespace = new redshiftserverless.CfnNamespace(this, 'namespace', {
      namespaceName: `${props.renovoLiveEnv}-renovolive`,
      dbName: 'renovolive',
      manageAdminPassword: true,
      defaultIamRoleArn: this.role.roleArn,
      iamRoles: [this.role.roleArn],
      logExports: ['userlog', 'connectionlog', 'useractivitylog'],
    });

    const workgroupPort = 5439;

    this.workgroup = new redshiftserverless.CfnWorkgroup(this, 'workgroup', {
      workgroupName: `${props.renovoLiveEnv}-renovolive`,
      baseCapacity: 8,
      maxCapacity: 8,
      enhancedVpcRouting: true,
      namespaceName: namespace.namespaceName,
      publiclyAccessible: false,
      securityGroupIds: [sg.securityGroupId],
      port: workgroupPort,
      subnetIds: vpc.selectSubnets({
        subnetGroupName: 'Private',
      }).subnetIds,
    });

    this.workgroup.addDependency(namespace);

    this.connectionSecret = secretsmanager.Secret.fromSecretNameV2(this, 'redshiftSecret', `redshift!${this.workgroup.workgroupName}-admin`);

    this.connection = new glue.Connection(this, 'redshiftConnection', {
      connectionName: `${props.renovoLiveEnv}-renovolive-redshift-connection`,
      type: glue.ConnectionType.JDBC,
      description: 'JDBC connection to Redshift serverless for RenovoLive data',
      properties: {
        JDBC_CONNECTION_URL: `jdbc:redshift://${this.workgroup.attrWorkgroupEndpointAddress}:${workgroupPort}/renovolive`,
        JDBC_ENFORCE_SSL: 'true',
        SECRET_ID: this.connectionSecret.secretName,
      },
      subnet: vpc.selectSubnets({
        subnetGroupName: 'Private',
      }).subnets[0],
      securityGroups: [connectionSg],
    });

    const connectionCfn = this.connection.node.defaultChild as gluecfn.CfnConnection;

    connectionCfn.addDependency(this.workgroup);
  }
}
