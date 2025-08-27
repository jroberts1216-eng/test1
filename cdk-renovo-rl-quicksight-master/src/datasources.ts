import {
  aws_quicksight as quicksight,
  Stack,
  StackProps,
  aws_redshiftserverless as redshift,
} from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { RenovoLiveEnv, RLDBCredSecretArns, RLDBEndpoints } from './common';

export interface RenovoLiveDataSourcesProps extends StackProps {
  /**
   * The RenovoLive environment
   */
  readonly renovoLiveEnv: RenovoLiveEnv;
  /**
   * The Redshift workgroup to connect to
   */
  readonly redshiftWorkgroup: redshift.CfnWorkgroup;
  /**
   * The VPC connection to use for QuickSight to connect to VPC resources
   */
  readonly quickSightVpcConnection: quicksight.CfnVPCConnection;
  /**
   * The ARN of the folder to place the data sources in
   */
  readonly folderArn: string;
  /**
   * The credentials secret's full ARN needed for the redshift workgroup
   */
  readonly redshiftSecretArn: string;
  /**
   * The RenovoLive database endpoint to use for the SQL Server data source
   */
  readonly sqlServerEndpoint: RLDBEndpoints;
  /**
   * The credentials secret's full ARN needed for the SQL Server data source
   */
  readonly sqlServerSecretArn: RLDBCredSecretArns;
}

export class RenovoLiveDataSources extends Stack {
  /**
   * The Redshift data source for RL
   */
  public readonly redshiftDataSource: quicksight.CfnDataSource;
  /**
   * The SQL Server data source for RL
   */
  public readonly sqlServerDataSource: quicksight.CfnDataSource

  constructor(scope: Construct, id: string, props: RenovoLiveDataSourcesProps) {
    super(scope, id, props);

    this.redshiftDataSource = new quicksight.CfnDataSource(this, 'RedshiftDataSource', {
      awsAccountId: this.account,
      dataSourceId: `${props.renovoLiveEnv}-renovolive-redshift`,
      name: `${props.renovoLiveEnv}-renovolive-redshift`,
      type: 'REDSHIFT',
      dataSourceParameters: {
        redshiftParameters: {
          database: 'renovolive',
          host: props.redshiftWorkgroup.attrWorkgroupEndpointAddress,
          port: 5439,
        },
      },
      credentials: {
        secretArn: props.redshiftSecretArn,
      },
      folderArns: [
        props.folderArn,
      ],
      vpcConnectionProperties: {
        vpcConnectionArn: props.quickSightVpcConnection.attrArn,
      },
    });

    this.sqlServerDataSource = new quicksight.CfnDataSource(this, 'SqlServerDataSource', {
      awsAccountId: this.account,
      dataSourceId: `${props.renovoLiveEnv}-renovolive-sqlserver`,
      name: `${props.renovoLiveEnv}-renovolive-sqlserver`,
      type: 'SQLSERVER',
      dataSourceParameters: {
        sqlServerParameters: {
          database: 'RenovoMaster',
          host: props.sqlServerEndpoint,
          port: 1433,
        },
      },
      credentials: {
        secretArn: props.sqlServerSecretArn,
      },
      folderArns: [
        props.folderArn,
      ],
      vpcConnectionProperties: {
        vpcConnectionArn: props.quickSightVpcConnection.attrArn
      },
    });
  }
}