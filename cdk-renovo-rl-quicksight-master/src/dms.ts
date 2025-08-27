import * as path from 'path';
import {
  Stack,
  StackProps,
  aws_ec2 as ec2,
  aws_dms as dms,
  aws_events as events,
  aws_events_targets as targets,
  aws_secretsmanager as secretsmanager,
  aws_iam as iam,
  aws_s3 as s3,
  aws_sns as sns,
  aws_sqs as sqs,
  aws_kms as kms,
  aws_lambda as lambda,
  aws_lambda_event_sources as lambdaEventSources,
  Duration,
  Names,
} from 'aws-cdk-lib';
import { NagSuppressions } from 'cdk-nag';
import { Construct } from 'constructs';
import { VpcIds, RenovoLiveEnv, Accounts, RenovoLiveDBSchemas, DatabaseDetails } from './common';
import { RlCfnReplicationTask } from './replicationtask';
import { RlS3DestinationCfnEndpoint } from './rls3destinationendpoint';
import { RlSourceCfnEndpoint } from './rlsourceendpoint';

export enum DmsTaskType {
  FULL_LOAD = 'full-load',
  CDC = 'cdc',
  FULL_LOAD_AND_CDC = 'full-load-and-cdc',
}

export enum DmsSelectionRuleActions {
  INCLUDE = 'include',
  EXCLUDE = 'exclude',
  EXPLICIT = 'explicit',
}

export enum DmsTransformationRuleActions {
  ADD_COLUMN = 'add-column',
  INCLUDE_COLUMN = 'include-column',
  REMOVE_COLUMN = 'remove-column',
  RENAME = 'rename',
  CONVERT_LOWERCASE = 'convert-lowercase',
  CONVERT_UPPERCASE = 'convert-uppercase',
  ADD_PREFIX = 'add-prefix',
  REMOVE_PREFIX = 'remove-prefix',
  REPLACE_PREFIX = 'replace-prefix',
  ADD_SUFFIX = 'add-suffix',
  REMOVE_SUFFIX = 'remove-suffix',
  REPLACE_SUFFIX = 'replace-prefix',
  DEFINE_PRIMARY_KEY = 'define-primary-key',
  CHANGE_DATA_TYPE = 'change-data-type',
  ADD_BEFORE_IMAGE_COLUMNS = 'add-before-image-columns',
}

export enum DmsDataTypes {
  BYTES = 'bytes',
  DATE = 'date',
  TIME = 'time',
  DATETIME = 'datetime',
  INT1 = 'int1',
  INT2 = 'int2',
  INT4 = 'int4',
  INT8 = 'int8',
  NUMERIC = 'numeric',
  REAL4 = 'real4',
  REAL8 = 'real8',
  STRING = 'string',
  UINT1 = 'uint1',
  UINT2 = 'uint2',
  UINT4 = 'uint4',
  UINT8 = 'uint8',
  WSTRING = 'wstring',
  BLOB = 'blob',
  NCLOB = 'nclob',
  CLOB = 'clob',
  BOOLEAN = 'boolean',
  SET = 'set',
  LIST = 'list',
  MAP = 'map',
  TUPLE = 'tuple',
}

export enum DmsTransformationRulePrimaryKeyDefOriginTypes {
  PRIMARY_KEY = 'primary-key',
  UNIQUE_INDEX = 'unique-index',
}

export enum DmsTransformationRuleBeforeImageDefColumnFilter {
  ALL = 'all',
  PK_ONLY = 'pk-only',
  NON_LOB = 'non-lob',
}

export enum DmsTransformationRuleRuleTargets {
  SCHEMA = 'schema',
  TABLE = 'table',
  COLUMN = 'column',
}

export interface DmsRuleObjectLocatorProperty {
  /**
   * The schema name to match
   */
  readonly schemaName?: RenovoLiveDBSchemas;
  /**
   * The table name to match
   */
  readonly tableName?: string;
}

export interface DmsTransformationRuleObjectLocatorProperty extends DmsRuleObjectLocatorProperty {
  /**
   * The column name to match
   */
  readonly columnName?: string;
  /**
   * The data type to match
   */
  readonly dataType?: string;
}

export interface DmsTransformationRuleDataType {
  /**
   * The data type to use if the rule-action is add-column or the replacement data type if therule-action is change-data-type
   */
  readonly type: DmsDataTypes;
  /**
   * If the added column or replacement data type has a precision, an integer value to specify the precision.
   */
  readonly precision?: number;
  /**
   * If the added column or replacement data type has a scale, an integer value or date time value to specify the scale.
   */
  readonly scale?: string;
  /**
   * The length of new column data (when used with add-column)
   */
  readonly length?: number;
}

export interface DmsTransformationRulePrimaryKeyDef {
  /**
   * The name of the new primary key or unique index for the table or view
   */
  readonly name: string;
  /**
   * The type of unique key to define
   */
  readonly origin?: DmsTransformationRulePrimaryKeyDefOriginTypes;
  /**
   * An array of strings listing the names of columns in the order they appear in the primary key or unique index.
   */
  readonly columns: string[];
}

export interface DmsTransformationRuleBeforeImageDef {
  /**
   * A value prepended to a column name. The default value is BI_
   */
  readonly columnPrefix?: string;
  /**
   * A value appended to the column name. The default is empty.
   */
  readonly columnSuffix?: string;
  /**
   * The column filter to apply to the before image columns
   */
  readonly columnFilter?: DmsTransformationRuleBeforeImageDefColumnFilter;
}

export interface DmsSelectionRuleProps {
  /**
   * The DMS object locator property
   *
   * The name of each schema and table to match
   */
  readonly objectLocator: DmsRuleObjectLocatorProperty;
  /**
   * The action to take on the rule
   */
  readonly ruleAction: DmsSelectionRuleActions;
}

export interface DmsTransformationRuleProps {
  /**
   * The DMS object locator property
   *
   * The name of each schema, table, column, and data type to match
   */
  readonly objectLocator: DmsTransformationRuleObjectLocatorProperty;
  /**
   * The type of object you are transforming
   */
  readonly ruleTarget: DmsTransformationRuleRuleTargets;
  /**
   * The action to take on the rule
   */
  readonly ruleAction: DmsTransformationRuleActions;
  /**
   * The new value for actions that require input, such as rename.
   */
  readonly value?: string;
  /**
   * The old value for actions that require replacement, such as replace-prefix.
   */
  readonly oldValue?: string;
  /**
   * An alphanumeric value that follows SQLite syntax. When used with the rule-action
   * set to rename-schema, the expression parameter specifies a new schema. When used
   * with the rule-action set to rename-table, expression specifies a new table. When
   * used with the rule-action set to rename-column, expression specifies a new column
   * name value. When used with the rule-action set to add-column, expression specifies
   * data that makes up a new column. Note that only expressions are supported for this
   * parameter. Operators and commands are not supported.
   */
  readonly expression?: string;
  /**
   * The data type to use in a rule action
   */
  readonly dataType?: DmsTransformationRuleDataType;
  /**
   * This parameter can define the name, type, and content of a unique key on the transformed
   * table or view. It does so when the rule-action is set to define-primary-key and the
   * rule-target is set to table. By default, the unique key is defined as a primary key.
   */
  readonly primaryKeyDef?: DmsTransformationRulePrimaryKeyDef;
  /**
   * This parameter defines a naming convention to identify the before-image columns and
   * specifies a filter to identify which source columns can have before-image columns
   * created for them on the target. You can specify this parameter when the rule-action
   * is set to add-before-image-columns and the rule-target is set to column.
   */
  readonly beforeImageDef?: DmsTransformationRuleBeforeImageDef;
}

export function convertDatabaseDetailsToDmsRules(databaseDetails: DatabaseDetails): any[] {
  const rules: any[] = [];

  let idCounter = 0;

  function generateNumericId(): number {
    return ++idCounter;
  }

  // Add database-level selection and transformation rules if they exist
  if (databaseDetails.selectionRules) {
    databaseDetails.selectionRules.forEach(rule => {
      let currentId = generateNumericId();
      rules.push(convertRuleToDmsRule(rule, 'selection', currentId));
    });
  }
  if (databaseDetails.transformationRules) {
    databaseDetails.transformationRules.forEach(rule => {
      let currentId = generateNumericId();
      rules.push(convertRuleToDmsRule(rule, 'transformation', currentId));
    });
  }

  // Iterate over each table and add table-specific rules
  for (const table of databaseDetails.tables) {
    if (table.selectionRules) {
      table.selectionRules.forEach(rule => {
        let currentId = generateNumericId();
        let modifiedRule = { ...rule, objectLocator: { ...rule.objectLocator, tableName: table.tableName } };
        rules.push(convertRuleToDmsRule(modifiedRule, 'selection', currentId));
      });
    }
    if (table.transformationRules) {
      table.transformationRules.forEach(rule => {
        let currentId = generateNumericId();
        let modifiedRule = { ...rule, objectLocator: { ...rule.objectLocator, tableName: table.tableName } };
        rules.push(convertRuleToDmsRule(modifiedRule, 'transformation', currentId));
      });
    }
  }

  const ruleSet: any = {
    rules: rules,
  };

  return ruleSet;
}

// Helper function to convert properties of an object to kebab-case and add additional properties
function convertRuleToDmsRule(rule: any, ruleType: string, ruleId: number): any {
  function convertObjectKeysToKebabCase(obj: any): any {
    if (typeof obj !== 'object' || obj === null) {
      return obj;
    }
    if (Array.isArray(obj)) {
      return obj.map(item => convertObjectKeysToKebabCase(item));
    }
    const newObj: any = {};
    for (const key in obj) {
      if (obj.hasOwnProperty(key)) {
        const newKey = toKebabCase(key);
        newObj[newKey] = convertObjectKeysToKebabCase(obj[key]);
      }
    }
    return newObj;
  }
  const newRule: any = {
    'rule-type': ruleType,
    'rule-id': ruleId,
    'rule-name': ruleId,
  };

  const kebabCaseRule = convertObjectKeysToKebabCase(rule);
  for (const key in kebabCaseRule) {
    if (kebabCaseRule.hasOwnProperty(key)) {
      newRule[key] = kebabCaseRule[key];
    }
  }
  if (rule.tableName) {
    newRule['object-locator'] = {
      'table-name': rule.tableName,
    };
  }
  return newRule;
}


// Helper function to convert camelCase to kebab-case
function toKebabCase(str: string): string {
  return str.replace(/([a-z])([A-Z])/g, '$1-$2').toLowerCase();
}

export interface DmsStackProps extends StackProps {
  /**
  * The vpc where the DMS instance will be deployed
  */
  readonly vpcId: VpcIds;
  /**
  * The DMS instance type to use
  */
  readonly instanceType: ec2.InstanceType;
}

export const ingestionTransformationRules: DmsTransformationRuleProps[] = [
  {
    ruleTarget: DmsTransformationRuleRuleTargets.COLUMN,
    objectLocator: {
      schemaName: RenovoLiveDBSchemas.ANY,
      tableName: '%',
    },
    ruleAction: DmsTransformationRuleActions.ADD_COLUMN,
    value: 'ingestion_operation',
    expression: '$AR_H_OPERATION',
    oldValue: 'I',
    dataType: {
      type: DmsDataTypes.STRING,
      length: 20,
      scale: '',
    },
  },
  {
    ruleTarget: DmsTransformationRuleRuleTargets.COLUMN,
    objectLocator: {
      schemaName: RenovoLiveDBSchemas.ANY,
      tableName: '%',
    },
    ruleAction: DmsTransformationRuleActions.ADD_COLUMN,
    value: 'ingestion_timestamp',
    expression: '$AR_H_TIMESTAMP',
    dataType: {
      type: DmsDataTypes.DATETIME,
      precision: 7,
    },
  },
  {
    ruleTarget: DmsTransformationRuleRuleTargets.COLUMN,
    objectLocator: {
      schemaName: RenovoLiveDBSchemas.ANY,
      tableName: '%',
    },
    ruleAction: DmsTransformationRuleActions.ADD_COLUMN,
    value: 'ingestion_seq',
    expression: '$AR_H_CHANGE_SEQ',
    dataType: {
      type: DmsDataTypes.STRING,
      length: 50,
    },
  },
];

/**
 * DMS instance setup.
 */
export class DmsInstanceStack extends Stack {
  public readonly replicationInstanceArn: string;
  constructor(scope: Construct, id: string, props: DmsStackProps) {
    super(scope, id, props);

    const vpc = ec2.Vpc.fromLookup(this, 'vpc', {
      vpcId: props.vpcId,
    });

    const dmsInstanceSg = new ec2.SecurityGroup(this, 'dmsInstanceSg', {
      vpc,
      allowAllOutbound: true,
    });

    const subnetGroup = new dms.CfnReplicationSubnetGroup(this, 'dmsSubnetGroup', {
      replicationSubnetGroupDescription: 'DMS Subnet Group',
      replicationSubnetGroupIdentifier: Names.uniqueResourceName(this, {}),
      subnetIds: vpc.selectSubnets(
        {
          subnetGroupName: 'Private',
        },
      ).subnetIds,
    });

    const replicationInstance = new dms.CfnReplicationInstance(this, 'dmsInstance', {
      replicationInstanceClass: `dms.${props.instanceType.toString()}`,
      replicationInstanceIdentifier: Names.uniqueResourceName(this, {}),
      multiAz: true,
      replicationSubnetGroupIdentifier: subnetGroup.ref,
      vpcSecurityGroupIds: [dmsInstanceSg.securityGroupId],
      autoMinorVersionUpgrade: true,
      publiclyAccessible: false,
      engineVersion: '3.5.3',
    });
    this.replicationInstanceArn = replicationInstance.ref;
  }
};

export interface RLDbDmsStackProps extends StackProps {
  /**
   * The source database name
   */
  readonly databaseName: string;
  /**
   * The RenovoLive environment
   */
  readonly renovoLiveEnv: RenovoLiveEnv;
  /**
   * The raw bucket to store data
   */
  readonly rawBucket: s3.IBucket;
  /**
   * The DMS rules to apply to the tables
   */
  readonly tableMappingRules: any;
  /**
   * The replication instance ARN
   */
  readonly replicationInstanceArn: string;
  /**
   * The KMS key for the raw bucket encryption
   */
  readonly kmsKey: kms.IKey;
}

/**
 * A DMS source, destination, and replication task stack for an individual
 * source database (e.g. clRenovo).
 */
export class RLDbDmsStack extends Stack {
  constructor(scope: Construct, id: string, props: RLDbDmsStackProps) {
    super(scope, id, props);

    const envNameCapitalized = props.renovoLiveEnv.charAt(0).toUpperCase() + props.renovoLiveEnv.slice(1);

    const connectionSecret = secretsmanager.Secret.fromSecretNameV2(this, 'DBConnectionDetails', `RenovoLive/${envNameCapitalized}/QuickSight/DMS/DBConnectionDetails`);

    const secretAccessPolicy = new iam.ManagedPolicy(this, 'secretAccessPolicy', {
      statements: [
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: [
            'secretsmanager:GetSecretValue',
          ],
          resources: [
            connectionSecret.secretArn,
            `${connectionSecret.secretArn}-*`,
          ],
        }),
      ],
    });

    const secretAccessRole = new iam.Role(this, 'secretAccessRole', {
      assumedBy: new iam.ServicePrincipal('dms.us-east-1.amazonaws.com'),
      managedPolicies: [
        secretAccessPolicy,
      ],
    });

    NagSuppressions.addResourceSuppressions(secretAccessPolicy, [
      {
        id: 'AwsSolutions-IAM5',
        reason: 'This policy is required for DMS to access the secret and the wildcard only wildcards the secret version.',
      },
    ]);

    const sourceEndpoint = new RlSourceCfnEndpoint(this, 'dmsSourceEndpoint', {
      databaseName: props.databaseName,
      role: secretAccessRole,
      secret: connectionSecret,
      renovoLiveEnv: props.renovoLiveEnv,
    });

    const s3Policy = new iam.ManagedPolicy(this, 'dmsS3Policy', {
      statements: [
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: [
            's3:PutObject',
            's3:DeleteObject',
            's3:PutObjectTagging',
          ],
          resources: [
            `${props.rawBucket.arnForObjects(`renovolive/${props.renovoLiveEnv}/${props.databaseName}/*`)}`,
          ],
        }),
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: [
            's3:ListBucket',
          ],
          resources: [
            props.rawBucket.bucketArn,
          ],
        }),
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          sid: 'AllowS3EncryptionKey',
          actions: [
            'kms:GenerateDataKey',
            'kms:Encrypt',
            'kms:Decrypt',
          ],
          resources: [
            props.kmsKey.keyArn,
          ],
        }),
      ],
    });

    const s3Role = new iam.Role(this, 'dmsS3Role', {
      assumedBy: new iam.ServicePrincipal('dms.amazonaws.com'),
      managedPolicies: [
        s3Policy,
      ],
    });

    NagSuppressions.addResourceSuppressions(s3Policy, [
      {
        id: 'AwsSolutions-IAM5',
        reason: 'This policy is required for DMS to write to S3 and the wildcard is necessary because objects will be created using timestamped names or names like LOADXXXXX.parquet. \
        This policy is sufficiently scoped to only allow access to the specific prefix for this database.',
      },
    ]);

    const s3DestinationFullLoad = new RlS3DestinationCfnEndpoint(this, 'dmsTargetEndpointS3FullLoad', {
      sourceDatabaseName: props.databaseName,
      role: s3Role,
      bucket: props.rawBucket,
      renovoLiveEnv: props.renovoLiveEnv,
      kmsKey: props.kmsKey,
      taskType: DmsTaskType.FULL_LOAD,
    });

    new RlCfnReplicationTask(this, 'dmsReplicationFullLoad', {
      replicationInstanceArn: props.replicationInstanceArn,
      sourceEndpointArn: sourceEndpoint.ref,
      targetEndpointArn: s3DestinationFullLoad.ref,
      tableMappings: JSON.stringify(props.tableMappingRules),
      databaseName: props.databaseName,
      renovoLiveEnv: props.renovoLiveEnv,
      taskType: DmsTaskType.FULL_LOAD,
    });

    const s3DestinationCdc = new RlS3DestinationCfnEndpoint(this, 'dmsTargetEndpointS3Cdc', {
      sourceDatabaseName: props.databaseName,
      role: s3Role,
      bucket: props.rawBucket,
      renovoLiveEnv: props.renovoLiveEnv,
      kmsKey: props.kmsKey,
      taskType: DmsTaskType.CDC,
    });

    new RlCfnReplicationTask(this, 'dmsReplicationCdc', {
      replicationInstanceArn: props.replicationInstanceArn,
      sourceEndpointArn: sourceEndpoint.ref,
      targetEndpointArn: s3DestinationCdc.ref,
      tableMappings: JSON.stringify(props.tableMappingRules),
      databaseName: props.databaseName,
      renovoLiveEnv: props.renovoLiveEnv,
      taskType: DmsTaskType.CDC,
    });
  }
}

export class RLDbDmsNotificationStack extends Stack {
  constructor(scope: Construct, id: string, props: StackProps) {
    super(scope, id, props);

    const queue = new sqs.Queue(this, 'dmsReplicationFailureQueue', {
      queueName: 'dms-replication-failure',
      retentionPeriod: Duration.days(14),
    });

    const topic = sns.Topic.fromTopicArn(
      this,
      'orgAlerts',
      'arn:aws:sns:us-east-1:815802118132:org-alerts-topic',
    );

    new events.Rule(this, 'dmsReplicationFailure', {
      description: 'DMS Replication Failure',
      ruleName: 'dms-replication-failure',
      eventPattern: {
        source: ['aws.dms'],
        detailType: ['DMS Replication Task State Change'],
        detail: {
          type: ['REPLICATION_TASK'],
          eventType: ['REPLICATION_TASK_FAILED'],
        },
      },
      targets: [
        new targets.SqsQueue(queue),
      ],
    });

    const dmsLookupPolicy = new iam.ManagedPolicy(this, 'dmsLookupPolicy', {
      statements: [
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: [
            'dms:DescribeReplicationTasks', // allows viewing status
          ],
          resources: ['*'],
        }),
      ],
    });

    const snsPublishPolicy = new iam.ManagedPolicy(this, 'snsPublishPolicy', {
      statements: [
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: [
            'sns:Publish',
          ],
          resources: [
            topic.topicArn,
          ],
        }),
      ],
    });

    const queueReadPolicy = new iam.ManagedPolicy(this, 'queueReadPolicy', {
      statements: [
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: [
            'sqs:ReceiveMessage',
            'sqs:DeleteMessage',
          ],
          resources: [
            queue.queueArn,
          ],
        }),
      ],
    });

    const handlerRole = new iam.Role(this, 'dmsReplicationFailureHandlerRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
        dmsLookupPolicy,
        snsPublishPolicy,
        queueReadPolicy,
      ],
    });

    const handler = new lambda.Function(this, 'dmsReplicationFailureHandler', {
      functionName: 'dms-replication-failure-handler',
      runtime: lambda.Runtime.PYTHON_3_13,
      code: lambda.Code.fromAsset(path.join(__dirname, './handlers/replication_error_handler/')),
      handler: 'index.lambda_handler',
      timeout: Duration.seconds(10),
      memorySize: 256,
      environment: {
        SNS_TOPIC_ARN: topic.topicArn,
      },
      role: handlerRole,
    });

    handler.addEventSource(new lambdaEventSources.SqsEventSource(queue));
  }
}

export interface RLDmsInstanceAllocation {
  /**
   * The databases assigned to this instance
   */
  readonly databases: DatabaseDetails[];
  /**
   * The EC2 instance size for this DMS instance
   *
   * @default ec2.InstanceType.of(ec2.InstanceClass.BURSTABLE2, ec2.InstanceSize.MEDIUM)
   */
  readonly instanceType?: ec2.InstanceType;
  /**
   * A unique identifier for this instance allocation
   */
  readonly id: string;
}

export interface RLCreateDataExportStacksProps {
  /**
   * The RenovoLive environment that is being targeted
   */
  readonly renovoLiveEnv: RenovoLiveEnv;
  /**
   * The raw bucket to store data
   */
  readonly rawBucket: s3.IBucket;
  /**
   * The CDK app to which the stacks will be added
   */
  readonly app: Construct;
  /**
   * The instance allocations for DMS
   */
  readonly instanceAllocations: RLDmsInstanceAllocation[];
  /**
   * The KMS key for the raw bucket encryption
   */
  readonly kmsKey: kms.IKey;
}

export class RLDataExportStacks {
  static createStacks(props: RLCreateDataExportStacksProps): void {
    const envNameCapitalized = props.renovoLiveEnv.charAt(0).toUpperCase() + props.renovoLiveEnv.slice(1);

    for (const allocation of props.instanceAllocations) {
      const instance = new DmsInstanceStack(props.app, `${envNameCapitalized}RenovoLiveDmsInstance${allocation.id}`, {
        env: {
          account: Accounts.SERVICES,
          region: 'us-east-1',
        },
        vpcId: VpcIds.SERVICES,
        instanceType: allocation.instanceType || ec2.InstanceType.of(ec2.InstanceClass.MEMORY6_INTEL, ec2.InstanceSize.LARGE),
      });

      for (const db of allocation.databases) {
        new RLDbDmsStack(props.app, `${envNameCapitalized}RenovoLiveDbDms-${db.databaseName}`, {
          env: {
            account: Accounts.SERVICES,
            region: 'us-east-1',
          },
          databaseName: db.databaseName,
          renovoLiveEnv: props.renovoLiveEnv,
          rawBucket: props.rawBucket,
          tableMappingRules: convertDatabaseDetailsToDmsRules(db),
          replicationInstanceArn: instance.replicationInstanceArn,
          kmsKey: props.kmsKey,
        });
      }
    }
  }
}
