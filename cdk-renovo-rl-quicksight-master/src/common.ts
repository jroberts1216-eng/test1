import {
  DmsSelectionRuleProps,
  DmsTransformationRuleProps,
} from './dms';

export enum Accounts {
  SERVICES = '719035735300',
  RLDEV = '462159781845',
  RLPROD = '416804266253',
};

export enum VpcIds {
  SERVICES = 'vpc-0860cddb8bcdc6fb9',
};

export enum RenovoLiveEnv {
  DEV = 'dev',
  PROD = 'prod',
};

export enum RenovoLiveDBSchemas {
  DBO = 'dbo',
  ANY = '%',
};

export enum RLDBEndpoints {
  DEV = 'app-dev-rl-renovolive-com-2.cbbvzhqawqdz.us-east-1.rds.amazonaws.com',
  PROD = 'app-prod-rl-renovolive-com.c0ztckwal0sp.us-east-1.rds.amazonaws.com',
}

export enum RLDBCredSecretArns {
  DEV = 'arn:aws:secretsmanager:us-east-1:719035735300:secret:RenovoLive/Dev/QuickSight/DataSource/RLDBCreds-A8VVA3',
  PROD = 'arn:aws:secretsmanager:us-east-1:719035735300:secret:RenovoLive/Prod/QuickSight/DataSource/RLDBCreds-sAq870',
}

export const DataSetReadOnlyPermissions = [
  'quicksight:DescribeDataSet',
  'quicksight:DescribeDataSetPermissions',
  'quicksight:PassDataSet',
  'quicksight:DescribeIngestion',
  'quicksight:ListIngestions'
]

export const DataSetReadWritePermissions = [
  'quicksight:DescribeDataSet',
  'quicksight:DescribeDataSetPermissions',
  'quicksight:PassDataSet',
  'quicksight:DescribeIngestion',
  'quicksight:ListIngestions',
  'quicksight:UpdateDataSet',
  'quicksight:DeleteDataSet',
  'quicksight:CreateIngestion',
  'quicksight:CancelIngestion',
  'quicksight:UpdateDataSetPermissions'
]


/**
 * The details of a table for ingestion
 * 
 * This includes the DMS related selection and transformation
 * rules that will be applied to the table you give for
 * tableName
 */
export interface TableDetails {
  /**
   * The name of the table
   */
  readonly tableName: string;
  /**
   * The table schema
   */
  readonly schema: RenovoLiveDBSchemas;
  /**
   * The primary key of the table
   */
  readonly primaryKey: string;
  /**
   * The selection rules for the table
   */
  readonly selectionRules?: DmsSelectionRuleProps[];
  /**
   * The transformation rules for the table
   */
  readonly transformationRules?: DmsTransformationRuleProps[];
}

/**
 * The details of a database for ingestion
 * 
 * This includes all table details related to this database
 * and the DMS related selection and transformation rules for
 * those tables as well as any additional rules that apply
 * to the database as a whole such as wildcarded transformation
 * rules
 */
export interface DatabaseDetails {
  /**
   * The name of the database
   */
  readonly databaseName: string;
  /**
   * The tables that will be loaded from the database
   */
  readonly tables: TableDetails[];
  /**
   * Additional selection rules for the database that
   * don't apply to a specific table
   */
  readonly selectionRules?: DmsSelectionRuleProps[];
  /**
   * Additional transformation rules for the database that
   * don't apply to a specific table
   */
  readonly transformationRules?: DmsTransformationRuleProps[];
}
