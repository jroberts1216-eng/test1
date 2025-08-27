import {
  aws_dms as dms,
  aws_secretsmanager as secretsmanager,
  aws_iam as iam,
} from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { RenovoLiveEnv } from './common';

export interface RlSourceCfnEndpointProps extends Omit<dms.CfnEndpointProps, 'endpointType' | 'engineName'> {
  /**
   * The SQL Server database name
   */
  databaseName: string;
  /**
   * The Secrets Manager secret that contains the connection details
   */
  secret: secretsmanager.ISecret;
  /**
   * The IAM access role arn that allows DMS to access the secret in Secrets Manager
   */
  role: iam.IRole;
  /**
   * The RenovoLive env
   */
  renovoLiveEnv: RenovoLiveEnv;
}

export class RlSourceCfnEndpoint extends dms.CfnEndpoint {
  constructor(scope: Construct, id: string, props: RlSourceCfnEndpointProps) {
    const endpointIdentifier = `${props.renovoLiveEnv}-rl-${props.databaseName.toLowerCase()}`;
    const endpointType = 'source';
    const engineName = 'sqlserver';
    const microsoftSqlServerSettings = {
      secretsManagerSecretId: props.secret.secretArn,
      secretsManagerAccessRoleArn: props.role.roleArn,
    };
    const extraConnectionAttributes = 'setUpMsCdcForTables=true';
    super(scope, id, {
      endpointIdentifier,
      microsoftSqlServerSettings,
      extraConnectionAttributes,
      endpointType,
      engineName,
      ...props,
    });
  }
}
