import {
  aws_quicksight as quicksight,
  aws_iam as iam,
  aws_ec2 as ec2,
  Stack,
  StackProps,
  Names,
} from 'aws-cdk-lib';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import { Construct } from 'constructs';
import { NagSuppressions } from 'cdk-nag';

export interface QuickSightConfigProps extends StackProps {
  /**
   * The ID of the VPC where the QuickSight VPC connection will be created
   */
  readonly vpcId: string;
  /**
   * Quicksight user ARN for dataset and datasource creation
   */
  readonly quicksightUserArn: string;
}

export class QuickSightConfig extends Stack {
  /**
   * The security group used by the QuickSight connection to the VPC
   */
  public readonly vpcConnectionSecurityGroup: ec2.ISecurityGroup;
  /**
   * The QuickSight VPC connection
   */
  public readonly quickSightVpcConnection: quicksight.CfnVPCConnection;

  constructor(scope: Construct, id: string, props: QuickSightConfigProps) {
    super(scope, id, props);

    const vpc = ec2.Vpc.fromLookup(this, 'vpc', {
      vpcId: props.vpcId,
    });

    const subnetIds = vpc.selectSubnets({
      subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
    }).subnetIds;

    this.vpcConnectionSecurityGroup = new ec2.SecurityGroup(this, 'security-group', {
      vpc,
    });

    const executionRole = new iam.Role(this, 'execution-role', {
      roleName: 'QuickSightVpcConnectionExecutionRole',
      assumedBy: new iam.ServicePrincipal('quicksight.amazonaws.com'),
    });

    /**
     * Reference: https://docs.aws.amazon.com/quicksight/latest/user/vpc-creating-a-connection-in-quicksight-console.html
     */
    const quickSightVpcConnectionPolicy = new iam.ManagedPolicy(this, 'quicksight-vpc-connection-policy', {
      statements: [
        new iam.PolicyStatement({
          actions: [
            'ec2:CreateNetworkInterface',
            'ec2:ModifyNetworkInterfaceAttribute',
            'ec2:DeleteNetworkInterface',
            'ec2:DescribeSubnets',
            'ec2:DescribeSecurityGroups',
          ],
          resources: ['*'],
        }),
      ],
    });

    NagSuppressions.addResourceSuppressions(quickSightVpcConnectionPolicy, [
      {
        id: 'AwsSolutions-IAM5',
        reason: 'QuickSight VPC connection policy requires these permissions to set up the VPC connection',
      }
    ]);

    executionRole.addManagedPolicy(quickSightVpcConnectionPolicy);

    this.quickSightVpcConnection = new quicksight.CfnVPCConnection(this, 'QuickSightVpcConnection', {
      name: 'ServicesVpcConnection',
      roleArn: executionRole.roleArn,
      securityGroupIds: [this.vpcConnectionSecurityGroup.securityGroupId],
      subnetIds: subnetIds,
      awsAccountId: Stack.of(this).account,
      vpcConnectionId: Names.uniqueId(this),
    });

    const secret = secretsmanager.Secret.fromSecretNameV2(this, 'RedshiftSecret', 'redshift!renovo-np-admin');

    const redshiftDataSource = new quicksight.CfnDataSource(this, 'RedshiftDataSource', {
      awsAccountId: this.account,
      dataSourceId: 'redshift-serverless-ds',
      name: 'RedshiftServerlessDataSource',
      type: 'REDSHIFT',
      dataSourceParameters: {
        redshiftParameters: {
          database: 'prod',
          host: 'vpcendpoint-endpoint-dwbnwd5gsq3akgubfbmr.719035735300.us-east-1.redshift-serverless.amazonaws.com',
          port: 5439,
        },
      },
      credentials: {
        credentialPair: {
          username: secret.secretValueFromJson('username').toString(),
          password: secret.secretValueFromJson('password').toString(),
        },
      },
      permissions: [
        {
          principal: props.quicksightUserArn,
          actions: [
            'quicksight:DescribeDataSource',
            'quicksight:DescribeDataSourcePermissions',
            'quicksight:PassDataSource',
          ],
        },
      ],
      vpcConnectionProperties: {
        vpcConnectionArn: this.quickSightVpcConnection.attrArn,
      },
    });

    //Create dm list QuickSight dataset
    const viDmList = new quicksight.CfnDataSet(this, 'vi_dm_list', {
      awsAccountId: this.account,
      dataSetId: 'vi_dm_list_cdk',
      name: 'vi_dm_list_cdk',
      importMode: 'SPICE',
      physicalTableMap: {
        vidmlistcdkphy: {
          relationalTable: {
            dataSourceArn: redshiftDataSource.attrArn,
            schema: 'analytics',
            name: 'vi_dm_list',
            inputColumns: [
              {
                name: 'db',
                type: 'STRING',
              },
              {
                name: 'system_id',
                type: 'INTEGER',
              },
              {
                name: 'facility_id',
                type: 'INTEGER',
              },
              {
                name: 'client_id',
                type: 'INTEGER',
              },
              {
                name: 'region_id',
                type: 'INTEGER',
              },
              {
                name: 'system_name',
                type: 'STRING',
              },
              {
                name: 'facility',
                type: 'STRING',
              },
              {
                name: 'costcenter',
                type: 'STRING',
              },
              {
                name: 'receiveddate',
                type: 'DATETIME',
              },
              {
                name: 'se_workcompletedutc',
                type: 'DATETIME',
              },
              {
                name: 'serviceeventid',
                type: 'INTEGER',
              },
              {
                name: 'se_no',
                type: 'STRING',
              },
              {
                name: 'status',
                type: 'STRING',
              },
              {
                name: 'cetagno',
                type: 'STRING',
              },
              {
                name: 'serialnumber',
                type: 'STRING',
              },
              {
                name: 'location',
                type: 'STRING',
              },
              {
                name: 'facassetnumber',
                type: 'STRING',
              },
              {
                name: 'ismissioncritical',
                type: 'BIT',
              },
              {
                name: 'device',
                type: 'STRING',
              },
              {
                name: 'manufacturer',
                type: 'STRING',
              },
              {
                name: 'model',
                type: 'STRING',
              },
              {
                name: 'smdm',
                type: 'STRING',
              },
              {
                name: 'is_incoming_inspection',
                type: 'BIT',
              },
              {
                name: 'smsched',
                type: 'STRING',
              },
              {
                name: 'technician',
                type: 'STRING',
              },
              {
                name: 'po_number',
                type: 'STRING',
              },
              {
                name: 'specialcondition',
                type: 'STRING',
              },
              {
                name: 'se_status',
                type: 'STRING',
              },
              {
                name: 'se_completedutc',
                type: 'DATETIME',
              },
              {
                name: 'se__sm',
                type: 'STRING',
              },
              {
                name: 'notes1',
                type: 'STRING',
              },
              {
                name: 'notes2',
                type: 'STRING',
              },
              {
                name: 'priority',
                type: 'STRING',
              },
              {
                name: 'problem',
                type: 'STRING',
              },
              {
                name: 'problem_text',
                type: 'STRING',
              },
              {
                name: 'resolution',
                type: 'STRING',
              },
              {
                name: 'resolutioncode',
                type: 'STRING',
              },
              {
                name: 'symptom',
                type: 'STRING',
              },
              {
                name: 'diagnosis',
                type: 'STRING',
              },
              {
                name: 'parts_used',
                type: 'INTEGER',
              },
              {
                name: 'sestat_name',
                type: 'STRING',
              },
              {
                name: 'daystoresolution',
                type: 'INTEGER',
              },
              {
                name: 'locationkey',
                type: 'STRING',
              },
              {
                name: 'l_sespecialconditionid',
                type: 'INTEGER',
              },
              {
                name: 'parts_qty_ordered',
                type: 'DECIMAL',
                subType: 'FIXED',
              },
              {
                name: 'parts_qty_received',
                type: 'DECIMAL',
                subType: 'FIXED',
              },
            ],
          },
        },
      },
      logicalTableMap: {
        vidmlistcdk: {
          alias: 'vi_dm_list',
          dataTransforms: [
            {
              castColumnTypeOperation: {
                columnName: 'system_id',
                newColumnType: 'STRING',
              },
            },
            {
              castColumnTypeOperation: {
                columnName: 'facility_id',
                newColumnType: 'STRING',
              },
            },
            {
              castColumnTypeOperation: {
                columnName: 'client_id',
                newColumnType: 'STRING',
              },
            },
            {
              castColumnTypeOperation: {
                columnName: 'region_id',
                newColumnType: 'STRING',
              },
            },
            {
              createColumnsOperation: {
                columns: [
                  {
                    columnName: 'Age',
                    columnId: 'f08ae171-18ec-4c51-a44a-5c2b44ef628e',
                    expression: 'dateDiff(receiveddate,now())',
                  },
                ],
              },
            },
            {
              createColumnsOperation: {
                columns: [
                  {
                    columnName: 'Days To Close',
                    columnId: '9eeab72c-40de-43e6-a1ba-fb8cee8ae148',
                    expression: 'dateDiff(receiveddate,{se_completedutc})',
                  },
                ],
              },
            },
            {
              createColumnsOperation: {
                columns: [
                  {
                    columnName: 'se_sm',
                    columnId: 'a0164dcd-82b5-4fb8-9d10-adc0d145ce3e',
                    expression: 'receiveddate',
                  },
                ],
              },
            },
            {
              createColumnsOperation: {
                columns: [
                  {
                    columnName: 'Accidental Damage KPI',
                    columnId: '0b6ec35d-f137-4370-8895-8bc08876c665',
                    expression: "countIf(serviceeventid,{sestat_name}='Accidental Damage') / count(serviceeventid)",
                  },
                ],
              },
            },
            {
              createColumnsOperation: {
                columns: [
                  {
                    columnName: '3% Target',
                    columnId: 'fc6f6c1f-581a-43c1-b337-ed2aca574b30',
                    expression: '0.03',
                  },
                ],
              },
            },
            {
              createColumnsOperation: {
                columns: [
                  {
                    columnName: 'User Errors KPI',
                    columnId: '8e5a12b8-fc44-4f83-aa54-e57138619045',
                    expression: "countIf(serviceeventid,{sestat_name}='Operator Error') / count(serviceeventid)",
                  },
                ],
              },
            },
            {
              createColumnsOperation: {
                columns: [
                  {
                    columnName: 'Cannot Duplicate KPI',
                    columnId: '6e67a4a5-4a8b-40c0-94cc-93d0afe74d2f',
                    expression: "countIf(serviceeventid,{sestat_name}='Cannot Duplicate') / count(serviceeventid)",
                  },
                ],
              },
            },
            {
              createColumnsOperation: {
                columns: [
                  {
                    columnName: '2% Target',
                    columnId: 'fe092785-717b-4458-a8bb-1c09a570c0a5',
                    expression: '0.02    ',
                  },
                ],
              },
            },
            {
              createColumnsOperation: {
                columns: [
                  {
                    columnName: '5% Target',
                    columnId: '81b6d61f-6cd7-42ab-b0cd-bd96ef5d6c9e',
                    expression: '0.05',
                  },
                ],
              },
            },
            {
              createColumnsOperation: {
                columns: [
                  {
                    columnName: '95% Target',
                    columnId: '8d2da324-a497-4e2b-b042-d43402b191f4',
                    expression: '0.95',
                  },
                ],
              },
            },
            {
              createColumnsOperation: {
                columns: [
                  {
                    columnName: 'Manufacter Recalls',
                    columnId: '6b836de1-dddf-4943-8dbb-2962f34b5868',
                    expression: "countIf(serviceeventid,{sestat_name}='Manufacturer Alert/Recall')",
                  },
                ],
              },
            },
            {
              createColumnsOperation: {
                columns: [
                  {
                    columnName: 'userid',
                    columnId: '6d62779b-6681-408d-9d51-6b884fc4e21d',
                    expression: '${UserID}',
                  },
                ],
              },
            },
            {
              createColumnsOperation: {
                columns: [
                  {
                    columnName: 'Equipment Repair KPI',
                    columnId: 'e152543e-f0f9-416d-9c33-db2d42380fa1',
                    expression: "countIf(serviceeventid,status='Completed' or status='Closed') / count(serviceeventid)",
                  },
                ],
              },
            },
            {
              projectOperation: {
                projectedColumns: [
                  'db',
                  'system_id',
                  'facility_id',
                  'client_id',
                  'region_id',
                  'system_name',
                  'facility',
                  'costcenter',
                  'receiveddate',
                  'se_workcompletedutc',
                  'serviceeventid',
                  'se_no',
                  'status',
                  'cetagno',
                  'serialnumber',
                  'location',
                  'facassetnumber',
                  'ismissioncritical',
                  'device',
                  'manufacturer',
                  'model',
                  'smdm',
                  'is_incoming_inspection',
                  'smsched',
                  'technician',
                  'po_number',
                  'specialcondition',
                  'se_status',
                  'se_completedutc',
                  'se__sm',
                  'notes1',
                  'notes2',
                  'priority',
                  'problem',
                  'problem_text',
                  'resolution',
                  'resolutioncode',
                  'symptom',
                  'diagnosis',
                  'parts_used',
                  'sestat_name',
                  'daystoresolution',
                  'locationkey',
                  'l_sespecialconditionid',
                  'parts_qty_ordered',
                  'parts_qty_received',
                  'Age',
                  'Days To Close',
                  'se_sm',
                  'Accidental Damage KPI',
                  '3% Target',
                  'User Errors KPI',
                  'Cannot Duplicate KPI',
                  '2% Target',
                  '5% Target',
                  '95% Target',
                  'Manufacter Recalls',
                  'userid',
                  'Equipment Repair KPI',
                ],
              },
            },
          ],
          source: {
            physicalTableId: 'vidmlistcdkphy',
          },
        },

      },

      fieldFolders: {
        Calculations: {
          columns: [
            'Age',
            'Days To Close',
            'Accidental Damage KPI',
            '95% Target',
            '3% Target',
            'User Errors KPI',
            '5% Target',
            '2% Target',
            'Cannot Duplicate KPI',
            'Equipment Repair KPI',
          ],
        },
      },
      rowLevelPermissionDataSet: {
        namespace: 'default',
        arn: 'arn:aws:quicksight:us-east-1:719035735300:dataset/rls_permissions_cdk',
        permissionPolicy: 'GRANT_ACCESS',
        formatVersion: 'VERSION_1',
        status: 'ENABLED',
      },
      dataSetUsageConfiguration: {
        disableUseAsDirectQuerySource: false,
        disableUseAsImportedSource: false,
      },

      datasetParameters: [
        {
          decimalDatasetParameter: {
            id: '368d9e65-197f-4a8a-8d04-7a65fd08d5f5',
            name: 'UserID',
            valueType: 'SINGLE_VALUED',
            defaultValues: {
              staticValues: [
                3191.0,
              ],
            },
          },
        },
        {
          stringDatasetParameter: {
            id: 'a4997117-2702-4637-9abe-2a57e8d37b58',
            name: 'locationkeyparameter',
            valueType: 'SINGLE_VALUED',
            defaultValues: {
              staticValues: [
                'a',
              ],
            },
          },
        },
      ],

      permissions: [
        {
          principal: props.quicksightUserArn,
          actions: [
            'quicksight:DescribeDataSet',
            'quicksight:DescribeDataSetPermissions',
            'quicksight:PassDataSet',
            'quicksight:DescribeIngestion',
            'quicksight:ListIngestions',
            'quicksight:UpdateDataSet',
            'quicksight:UpdateDataSetPermissions',
          ],
        },
      ],
    });

    //Create purchase order QuickSight dataset
    const viPurchaseOrder = new quicksight.CfnDataSet(this, 'vi_purchase_order', {
      awsAccountId: this.account,
      dataSetId: 'vi_purchase_order_cdk',
      name: 'vi_purchase_order_cdk',
      importMode: 'SPICE',
      physicalTableMap: {
        vipurchaseorderphy: {
          relationalTable: {
            dataSourceArn: redshiftDataSource.attrArn,
            schema: 'analytics',
            name: 'vi_purchase_order',
            inputColumns: [
              {
                name: 'db',
                type: 'STRING',
              },
              {
                name: 'op',
                type: 'STRING',
              },
              {
                name: 'purchaseorderid',
                type: 'INTEGER',
              },
              {
                name: 'serviceeventid',
                type: 'INTEGER',
              },
              {
                name: 'facility_costcenterid',
                type: 'INTEGER',
              },
              {
                name: 'vendorlocationid',
                type: 'INTEGER',
              },
              {
                name: 'l_pospecialconditionid',
                type: 'INTEGER',
              },
              {
                name: 'l_shippingid',
                type: 'INTEGER',
              },
              {
                name: 'po_orgwide',
                type: 'BIT',
              },
              {
                name: 'po_number',
                type: 'STRING',
              },
              {
                name: 'po_amount',
                type: 'DECIMAL',
              },
              {
                name: 'po_exchgtotal',
                type: 'DECIMAL',
              },
              {
                name: 'po_total',
                type: 'DECIMAL',
              },
              {
                name: 'po_shippingamt',
                type: 'DECIMAL',
              },
              {
                name: 'po_taxotheramt',
                type: 'DECIMAL',
              },
              {
                name: 'po_description',
                type: 'STRING',
              },
              {
                name: 'po_type',
                type: 'INTEGER',
              },
              {
                name: 'po_approvalstatus',
                type: 'INTEGER',
              },
              {
                name: 'po_notesint',
                type: 'STRING',
              },
              {
                name: 'po_notesext',
                type: 'STRING',
              },
              {
                name: 'po_billtoclient',
                type: 'DECIMAL',
              },
              {
                name: 'po_shipping',
                type: 'STRING',
              },
              {
                name: 'po_paidbycreditcard',
                type: 'BIT',
              },
              {
                name: 'po_cod',
                type: 'BIT',
              },
              {
                name: 'po_trackingno',
                type: 'STRING',
              },
              {
                name: 'po_shipexpected',
                type: 'DATETIME',
              },
              {
                name: 'po_vendcontact',
                type: 'STRING',
              },
              {
                name: 'po_vendphone',
                type: 'STRING',
              },
              {
                name: 'po_vendemail',
                type: 'STRING',
              },
              {
                name: 'po_received',
                type: 'DATETIME',
              },
              {
                name: 'po_assigneduserid',
                type: 'INTEGER',
              },
              {
                name: 'po_orderplaced',
                type: 'DATETIME',
              },
              {
                name: 'po_orderplaceduserid',
                type: 'INTEGER',
              },
              {
                name: 'po_created',
                type: 'DATETIME',
              },
              {
                name: 'po_createduserid',
                type: 'INTEGER',
              },
              {
                name: 'po_approved',
                type: 'DATETIME',
              },
              {
                name: 'po_approveduserid',
                type: 'INTEGER',
              },
              {
                name: 'po_cancelled',
                type: 'DATETIME',
              },
              {
                name: 'po_cancelleduserid',
                type: 'INTEGER',
              },
              {
                name: 'po_closed',
                type: 'DATETIME',
              },
              {
                name: 'po_closeduserid',
                type: 'INTEGER',
              },
              {
                name: 'po_lastupdated',
                type: 'DATETIME',
              },
              {
                name: 'po_lastupdateduserid',
                type: 'INTEGER',
              },
              {
                name: 'posourceid',
                type: 'STRING',
              },
              {
                name: 'po_paymentterms',
                type: 'INTEGER',
              },
              {
                name: 'shippinglabelcreated',
                type: 'DATETIME',
              },
              {
                name: 'shippinglabelcreateduserid',
                type: 'INTEGER',
              },
              {
                name: 'l_vscinvoicefrequencyid',
                type: 'INTEGER',
              },
              {
                name: 'vendorid',
                type: 'INTEGER',
              },
              {
                name: 'partssourceorderid',
                type: 'INTEGER',
              },
              {
                name: 'ingestion_timestamp',
                type: 'DATETIME',
              },
              {
                name: 'ingestion_operation',
                type: 'STRING',
              },
              {
                name: 'ingestion_seq',
                type: 'STRING',
              },
              {
                name: 'ingestion_time_from_seq',
                type: 'STRING',
              },
              {
                name: 'ingestion_sequence',
                type: 'STRING',
              },
              {
                name: 'partition_date',
                type: 'DATETIME',
              },
              {
                name: 'potypelabel',
                type: 'STRING',
              },
              {
                name: 'postatuslabel',
                type: 'STRING',
              },
              {
                name: 'poapprovalstatuslabel',
                type: 'STRING',
              },
              {
                name: 'po_created_label',
                type: 'DATETIME',
              },
              {
                name: 'assigned_user_fullname',
                type: 'STRING',
              },
              {
                name: 'client_businessregion',
                type: 'STRING',
              },
              {
                name: 'vendorpaymentsunapproved',
                type: 'STRING',
              },
              {
                name: 'waitingvendorpayments',
                type: 'STRING',
              },
              {
                name: 'missingcredit',
                type: 'STRING',
              },
              {
                name: 'billabletimeamt',
                type: 'DECIMAL',
              },
              {
                name: 'billablepartsamt',
                type: 'DECIMAL',
              },
              {
                name: 'netdays',
                type: 'INTEGER',
              },
              {
                name: 'po_total_regular_value',
                type: 'DECIMAL',
              },
              {
                name: 'po_total_exchange_value',
                type: 'DECIMAL',
              },
              {
                name: 'po_total_shipping_value',
                type: 'DECIMAL',
              },
              {
                name: 'po_total_tax_value',
                type: 'DECIMAL',
              },
              {
                name: 'po_total_value',
                type: 'DECIMAL',
              },
              {
                name: 'po_vsc_total_value',
                type: 'DECIMAL',
              },
              {
                name: 'defaulterpregionid',
                type: 'STRING',
              },
              {
                name: 'iscostcenterinactive',
                type: 'BIT',
              },
              {
                name: 'isfacilityinactive',
                type: 'BIT',
              },
              {
                name: 'issysteminactive',
                type: 'BIT',
              },
              {
                name: 'iscientinactive',
                type: 'BIT',
              },
              {
                name: 'isinactive',
                type: 'INTEGER',
              },
              {
                name: 'sestatus',
                type: 'STRING',
              },
              {
                name: 'equipmentdescription',
                type: 'STRING',
              },
              {
                name: 'client_id',
                type: 'INTEGER',
              },
              {
                name: 'system_id',
                type: 'INTEGER',
              },
              {
                name: 'facility_id',
                type: 'INTEGER',
              },
              {
                name: 'region_id',
                type: 'INTEGER',
              },
              {
                name: 'costcenterid',
                type: 'INTEGER',
              },
              {
                name: 'hospitalsystemid',
                type: 'INTEGER',
              },
              {
                name: 'vendorlocation',
                type: 'STRING',
              },
              {
                name: 'system',
                type: 'STRING',
              },
              {
                name: 'facility',
                type: 'STRING',
              },
              {
                name: 'costcenter',
                type: 'STRING',
              },
              {
                name: 'deviceclass',
                type: 'STRING',
              },
              {
                name: 'facabbrev',
                type: 'STRING',
              },
              {
                name: 'poassigned',
                type: 'STRING',
              },
              {
                name: 'invoice_uploaded',
                type: 'DATETIME',
              },
              {
                name: 'billablestate',
                type: 'STRING',
              },
              {
                name: 'device',
                type: 'STRING',
              },
              {
                name: 'manufacturer',
                type: 'STRING',
              },
              {
                name: 'model',
                type: 'STRING',
              },
              {
                name: 'cetag',
                type: 'STRING',
              },
              {
                name: 'se_completedutc',
                type: 'DATETIME',
              },
              {
                name: 'facility_equipmentid',
                type: 'INTEGER',
              },
              {
                name: 'ismissingshipto',
                type: 'BIT',
              },
              {
                name: 'is_sedeleted',
                type: 'BIT',
              },
              {
                name: 'se_no',
                type: 'STRING',
              },
              {
                name: 'entityident',
                type: 'STRING',
              },
              {
                name: 'ismsystemid',
                type: 'INTEGER',
              },
              {
                name: 'system_siteid',
                type: 'STRING',
              },
              {
                name: 'completedapdate',
                type: 'DATETIME',
              },
              {
                name: 'equipbudgetcoverage',
                type: 'STRING',
              },
              {
                name: 'budgetamount',
                type: 'DECIMAL',
              },
              {
                name: 'exchgsopen',
                type: 'STRING',
              },
              {
                name: 'partsnotreceived',
                type: 'STRING',
              },
              {
                name: 'ready_to_close',
                type: 'STRING',
              },
            ],
          },
        },
      },
      logicalTableMap: {
        vipurchaseorderlog: {
          alias: 'vi_purchase_order',
          dataTransforms: [
            {
              renameColumnOperation: {
                columnName: 'po_total',
                newColumnName: 'PO Total',
              },
            },
            {
              renameColumnOperation: {
                columnName: 'po_total_regular_value',
                newColumnName: 'Regular',
              },
            },
            {
              renameColumnOperation: {
                columnName: 'po_total_exchange_value',
                newColumnName: 'Exchange',
              },
            },
            {
              renameColumnOperation: {
                columnName: 'po_total_shipping_value',
                newColumnName: 'Shipping',
              },
            },
            {
              renameColumnOperation: {
                columnName: 'po_total_tax_value',
                newColumnName: 'Tax',
              },
            },
            {
              renameColumnOperation: {
                columnName: 'po_total_value',
                newColumnName: 'Total',
              },
            },
            {
              renameColumnOperation: {
                columnName: 'po_vsc_total_value',
                newColumnName: 'VSC Total Value',
              },
            },
            {
              castColumnTypeOperation: {
                columnName: 'client_id',
                newColumnType: 'STRING',
              },
            },
            {
              castColumnTypeOperation: {
                columnName: 'system_id',
                newColumnType: 'STRING',
              },
            },
            {
              castColumnTypeOperation: {
                columnName: 'facility_id',
                newColumnType: 'STRING',
              },
            },
            {
              castColumnTypeOperation: {
                columnName: 'region_id',
                newColumnType: 'STRING',
              },
            },
            {
              createColumnsOperation: {
                columns: [
                  {
                    columnName: 'Billable State',
                    columnId: '6e243cc7-821b-40f6-b039-272b33aa75ff',
                    expression: "ifelse(\n    {billablestate}='NB','Non-Billable',\n    {billablestate}='B','Billable',\n    {billablestate}='Mixed','Mixed',\n    isNull({billablestate}),'None',\n    {billablestate}\n)",
                  },
                ],
              },
            },
            {
              tagColumnOperation: {
                columnName: 'PO Total',
                tags: [
                  {
                    columnDescription: {
                      text: 'po_total',
                    },
                  },
                ],
              },
            },
            {
              tagColumnOperation: {
                columnName: 'Regular',
                tags: [
                  {
                    columnDescription: {
                      text: 'po_total_regular_value',
                    },
                  },
                ],
              },
            },
            {
              tagColumnOperation: {
                columnName: 'Exchange',
                tags: [
                  {
                    columnDescription: {
                      text: 'po_total_exchange_value',
                    },
                  },
                ],
              },
            },
            {
              tagColumnOperation: {
                columnName: 'Shipping',
                tags: [
                  {
                    columnDescription: {
                      text: 'po_total_shipping_value',
                    },
                  },
                ],
              },
            },
            {
              tagColumnOperation: {
                columnName: 'Tax',
                tags: [
                  {
                    columnDescription: {
                      text: 'po_total_tax_value',
                    },
                  },
                ],
              },
            },
            {
              tagColumnOperation: {
                columnName: 'Total',
                tags: [
                  {
                    columnDescription: {
                      text: 'po_total_value',
                    },
                  },
                ],
              },
            },
            {
              tagColumnOperation: {
                columnName: 'VSC Total Value',
                tags: [
                  {
                    columnDescription: {
                      text: 'po_vsc_total_value',
                    },
                  },
                ],
              },
            },
            {
              projectOperation: {
                projectedColumns: [
                  'db',
                  'op',
                  'purchaseorderid',
                  'serviceeventid',
                  'facility_costcenterid',
                  'vendorlocationid',
                  'l_pospecialconditionid',
                  'l_shippingid',
                  'po_orgwide',
                  'po_number',
                  'po_amount',
                  'po_exchgtotal',
                  'PO Total',
                  'po_shippingamt',
                  'po_taxotheramt',
                  'po_description',
                  'po_type',
                  'po_approvalstatus',
                  'po_notesint',
                  'po_notesext',
                  'po_billtoclient',
                  'po_shipping',
                  'po_paidbycreditcard',
                  'po_cod',
                  'po_trackingno',
                  'po_shipexpected',
                  'po_vendcontact',
                  'po_vendphone',
                  'po_vendemail',
                  'po_received',
                  'po_assigneduserid',
                  'po_orderplaced',
                  'po_orderplaceduserid',
                  'po_created',
                  'po_createduserid',
                  'po_approved',
                  'po_approveduserid',
                  'po_cancelled',
                  'po_cancelleduserid',
                  'po_closed',
                  'po_closeduserid',
                  'po_lastupdated',
                  'po_lastupdateduserid',
                  'posourceid',
                  'po_paymentterms',
                  'shippinglabelcreated',
                  'shippinglabelcreateduserid',
                  'l_vscinvoicefrequencyid',
                  'vendorid',
                  'partssourceorderid',
                  'ingestion_timestamp',
                  'ingestion_operation',
                  'ingestion_seq',
                  'ingestion_time_from_seq',
                  'ingestion_sequence',
                  'partition_date',
                  'potypelabel',
                  'postatuslabel',
                  'poapprovalstatuslabel',
                  'po_created_label',
                  'assigned_user_fullname',
                  'client_businessregion',
                  'vendorpaymentsunapproved',
                  'waitingvendorpayments',
                  'missingcredit',
                  'billabletimeamt',
                  'billablepartsamt',
                  'netdays',
                  'Regular',
                  'Exchange',
                  'Shipping',
                  'Tax',
                  'Total',
                  'VSC Total Value',
                  'defaulterpregionid',
                  'iscostcenterinactive',
                  'isfacilityinactive',
                  'issysteminactive',
                  'iscientinactive',
                  'isinactive',
                  'sestatus',
                  'equipmentdescription',
                  'client_id',
                  'system_id',
                  'facility_id',
                  'region_id',
                  'costcenterid',
                  'hospitalsystemid',
                  'vendorlocation',
                  'system',
                  'facility',
                  'costcenter',
                  'deviceclass',
                  'facabbrev',
                  'poassigned',
                  'invoice_uploaded',
                  'billablestate',
                  'device',
                  'manufacturer',
                  'model',
                  'cetag',
                  'se_completedutc',
                  'facility_equipmentid',
                  'ismissingshipto',
                  'is_sedeleted',
                  'se_no',
                  'entityident',
                  'ismsystemid',
                  'system_siteid',
                  'completedapdate',
                  'equipbudgetcoverage',
                  'budgetamount',
                  'exchgsopen',
                  'partsnotreceived',
                  'ready_to_close',
                  'Billable State',
                ],
              },
            },
          ],
          source: {
            physicalTableId: 'vipurchaseorderphy',
          },
        },
      },
      rowLevelPermissionDataSet: {
        namespace: 'default',
        arn: 'arn:aws:quicksight:us-east-1:719035735300:dataset/rls_permissions_cdk',
        permissionPolicy: 'GRANT_ACCESS',
        formatVersion: 'VERSION_1',
        status: 'ENABLED',
      },
      dataSetUsageConfiguration: {
        disableUseAsDirectQuerySource: false,
        disableUseAsImportedSource: false,
      },
      permissions: [
        {
          principal: props.quicksightUserArn,
          actions: [
            'quicksight:DescribeDataSet',
            'quicksight:DescribeDataSetPermissions',
            'quicksight:PassDataSet',
            'quicksight:DescribeIngestion',
            'quicksight:ListIngestions',
            'quicksight:UpdateDataSet',
            'quicksight:UpdateDataSetPermissions',
          ],
        },
      ],
    });

    //Create sm detail QuickSight dataset
    const viSmStatus = new quicksight.CfnDataSet(this, 'vi_sm_status_dashboard_detail', {
      awsAccountId: this.account,
      dataSetId: 'vi_sm_status_dashboard_detail_cdk',
      name: 'vi_sm_status_dashboard_detail_cdk',
      importMode: 'SPICE',
      physicalTableMap: {
        vismstatusdashboardphy: {
          relationalTable: {
            dataSourceArn: redshiftDataSource.attrArn,
            schema: 'analytics',
            name: 'vi_sm_status_dashboard_detail',
            inputColumns: [
              {
                name: 'db',
                type: 'STRING',
              },
              {
                name: 'client_id',
                type: 'STRING',
              },
              {
                name: 'system_id',
                type: 'STRING',
              },
              {
                name: 'region_id',
                type: 'STRING',
              },
              {
                name: 'system_name',
                type: 'STRING',
              },
              {
                name: 'facility_id',
                type: 'INTEGER',
              },
              {
                name: 'facility_name',
                type: 'STRING',
              },
              {
                name: 'facility_costcenterid',
                type: 'INTEGER',
              },
              {
                name: 'costcenter_name',
                type: 'STRING',
              },
              {
                name: 'cetag',
                type: 'STRING',
              },
              {
                name: 'modelname',
                type: 'STRING',
              },
              {
                name: 'manufacturer',
                type: 'STRING',
              },
              {
                name: 'devicename',
                type: 'STRING',
              },
              {
                name: 'noserial',
                type: 'STRING',
              },
              {
                name: 'se_sm',
                type: 'DATETIME',
              },
              {
                name: 'technician',
                type: 'STRING',
              },
              {
                name: 'serviceeventid',
                type: 'INTEGER',
              },
              {
                name: 'se_number',
                type: 'STRING',
              },
              {
                name: 'se_initiatedutc',
                type: 'DATETIME',
              },
              {
                name: 'se_workcompletedutc',
                type: 'DATETIME',
              },
              {
                name: 'se_completedutc',
                type: 'DATETIME',
              },
              {
                name: 'lifesupport',
                type: 'INTEGER',
              },
              {
                name: 'se_smcomplcode',
                type: 'INTEGER',
              },
              {
                name: 'total',
                type: 'DECIMAL',
              },
              {
                name: 'se_state_name',
                type: 'STRING',
              },
              {
                name: 'managedsctype',
                type: 'STRING',
              },
              {
                name: 'state_name',
                type: 'STRING',
              },
              {
                name: 'sestat_name',
                type: 'STRING',
              },
              {
                name: 'resolution',
                type: 'STRING',
              },
              {
                name: 'l_seinitiatedbyid',
                type: 'INTEGER',
              },
              {
                name: 'l_equipmentstatusid',
                type: 'INTEGER',
              },
              {
                name: 'equipment_status',
                type: 'STRING',
              },
            ],
          },
        },
      },
      logicalTableMap: {
        vismstatusdashboardlog: {
          alias: 'vi_sm_status_dashboard_detail',
          dataTransforms: [
            {
              castColumnTypeOperation: {
                columnName: 'facility_id',
                newColumnType: 'STRING',
              },
            },
            {
              createColumnsOperation: {
                columns: [
                  {
                    columnName: 'SM Scheduled',
                    columnId: '6e41cec5-da13-4f63-b7b9-1318c30a7c9a',
                    expression: 'count(serviceeventid)',
                  },
                ],
              },
            },
            {
              createColumnsOperation: {
                columns: [
                  {
                    columnName: 'SM RFS',
                    columnId: '8d72d7cc-5143-47ce-bd9c-ccc52f32a6d3',
                    expression: 'countIf({serviceeventid},{se_smcomplcode}=1)',
                  },
                ],
              },
            },
            {
              createColumnsOperation: {
                columns: [
                  {
                    columnName: 'SM CIU',
                    columnId: '7d72f5a7-5061-43c4-a8c0-81e1c78a04cd',
                    expression: 'countIf({serviceeventid},{se_smcomplcode}=2)',
                  },
                ],
              },
            },
            {
              createColumnsOperation: {
                columns: [
                  {
                    columnName: 'SM Adjusted',
                    columnId: '3c066f3d-aac6-4b5f-b6c9-8db7cbb54399',
                    expression: 'countIf({serviceeventid},{se_smcomplcode}<>1 AND {se_smcomplcode}<>10)',
                  },
                ],
              },
            },
            {
              createColumnsOperation: {
                columns: [
                  {
                    columnName: 'SM UTL',
                    columnId: 'f936b001-59ef-408d-8f51-080ef6e5a87c',
                    expression: 'countIf({serviceeventid},{se_smcomplcode}=3)',
                  },
                ],
              },
            },
            {
              createColumnsOperation: {
                columns: [
                  {
                    columnName: 'SM COT No',
                    columnId: '6963a92a-dcbf-4c67-a3ad-937c7d80a78c',
                    expression: 'countIf({serviceeventid},{se_smcomplcode}=4)',
                  },
                ],
              },
            },
            {
              createColumnsOperation: {
                columns: [
                  {
                    columnName: 'SM COT Yes',
                    columnId: 'c989477d-05ee-4486-8147-783976dff981',
                    expression: 'countIf({serviceeventid},{se_smcomplcode}=5)',
                  },
                ],
              },
            },
            {
              createColumnsOperation: {
                columns: [
                  {
                    columnName: 'SM INCL',
                    columnId: 'd2bdd92d-062a-4abe-a053-b139aabc72bb',
                    expression: 'countIf({serviceeventid},{se_smcomplcode}=6)',
                  },
                ],
              },
            },
            {
              createColumnsOperation: {
                columns: [
                  {
                    columnName: 'SM ASM Completed',
                    columnId: 'eb59dbf7-6d5e-437a-bef0-b55d48b989da',
                    expression: 'countIf({serviceeventid},{se_smcomplcode}=7)',
                  },
                ],
              },
            },
            {
              createColumnsOperation: {
                columns: [
                  {
                    columnName: 'SM UFM',
                    columnId: '32930d7c-5173-4818-b718-92d355732e45',
                    expression: 'countIf({serviceeventid},{se_smcomplcode}=8)',
                  },
                ],
              },
            },
            {
              createColumnsOperation: {
                columns: [
                  {
                    columnName: 'SM RSCD',
                    columnId: '66d2aedf-2c70-4cbc-88f5-989600dfbf89',
                    expression: 'countIf({serviceeventid},{se_smcomplcode}=10)',
                  },
                ],
              },
            },
            {
              createColumnsOperation: {
                columns: [
                  {
                    columnName: 'SM Completed Std',
                    columnId: '562dc043-b4dc-47c7-8135-21b8f3f8774b',
                    expression: 'countIf({serviceeventid},{se_smcomplcode}=9)',
                  },
                ],
              },
            },
            {
              createColumnsOperation: {
                columns: [
                  {
                    columnName: 'SM Adjusted Completed',
                    columnId: '9fbea252-38a5-44f9-a3a4-24782e400548',
                    expression: 'countIf({serviceeventid},{se_smcomplcode}<>1 and {se_smcomplcode}<>10 and isNotNull({se_workcompletedutc}))',
                  },
                ],
              },
            },
            {
              createColumnsOperation: {
                columns: [
                  {
                    columnName: 'SM Adjusted Closed',
                    columnId: 'b6f8dbf3-7c78-4cdf-8882-b6bf7aea809c',
                    expression: 'countIf({serviceeventid},{se_smcomplcode}<>1 and {se_smcomplcode}<>10 and isNotNull({se_completedutc}))',
                  },
                ],
              },
            },
            {
              createColumnsOperation: {
                columns: [
                  {
                    columnName: 'SM Adjusted Open',
                    columnId: 'f1e174aa-03dc-47e8-b486-257ed27d1f4e',
                    expression: 'countIf({serviceeventid},{se_smcomplcode}<>1 and {se_smcomplcode}<>10 and isNull({se_workcompletedutc}))',
                  },
                ],
              },
            },
            {
              createColumnsOperation: {
                columns: [
                  {
                    columnName: 'SM Scheduled Completed',
                    columnId: '139d1e16-1eef-4ecb-ad08-956df91fd5b2',
                    expression: 'countIf({serviceeventid},isNotNull({se_workcompletedutc}))',
                  },
                ],
              },
            },
            {
              createColumnsOperation: {
                columns: [
                  {
                    columnName: 'SM Scheduled Closed',
                    columnId: '05d0fae8-71ea-4479-9d57-c26710b9838a',
                    expression: 'countIf({serviceeventid},isNotNull({se_completedutc}))',
                  },
                ],
              },
            },
            {
              createColumnsOperation: {
                columns: [
                  {
                    columnName: 'SM Scheduled Open',
                    columnId: '577017aa-479f-4a32-8e00-8535528f11ed',
                    expression: 'countIf({serviceeventid},isNull({se_workcompletedutc}) and isNull({se_completedutc}))',
                  },
                ],
              },
            },
            {
              createColumnsOperation: {
                columns: [
                  {
                    columnName: '% Completed',
                    columnId: 'bdbdb3f8-d383-4fdb-b87a-473fc627b639',
                    expression: 'ifelse(\n    ({SM Adjusted}) = 0, \n    1.0,\n    ({SM Adjusted Completed}) / (({SM Adjusted}) * 1.0)\n)\n',
                  },
                ],
              },
            },
            {
              createColumnsOperation: {
                columns: [
                  {
                    columnName: 'Compl Displ',
                    columnId: '7c723c78-3db7-42db-951f-115c6db23e4b',
                    expression: "concat( toString(coalesce( {SM Adjusted Completed} ,0)) ,' / ', toString(coalesce({SM Adjusted},0)) )",
                  },
                ],
              },
            },
            {
              createColumnsOperation: {
                columns: [
                  {
                    columnName: '30% Target',
                    columnId: '88753b0b-6c10-4265-882a-11d682401f45',
                    expression: 'round(({SM Adjusted}) * 0.3)',
                  },
                ],
              },
            },
            {
              createColumnsOperation: {
                columns: [
                  {
                    columnName: '60% Target',
                    columnId: 'e949bb0e-325c-433e-a8cc-fbd8d9c7134b',
                    expression: 'round({SM Adjusted} * 0.6,0)',
                  },
                ],
              },
            },
            {
              createColumnsOperation: {
                columns: [
                  {
                    columnName: '90% Target',
                    columnId: 'b31958f5-470d-45f9-9c4f-b315d299f2a1',
                    expression: 'round({SM Adjusted} * 0.9,0)',
                  },
                ],
              },
            },
            {
              createColumnsOperation: {
                columns: [
                  {
                    columnName: 'High Risk',
                    columnId: '68ee429d-37bc-48be-bfd6-e11a37c9a132',
                    expression: "ifelse(\n{lifesupport}=1, 'High Risk','No High Risk'\n)",
                  },
                ],
              },
            },
            {
              createColumnsOperation: {
                columns: [
                  {
                    columnName: 'SM Scheduled Closed - Completed',
                    columnId: '59f9a1d9-60a8-451c-b432-4016be95b855',
                    expression: 'countIf({serviceeventid},isNotNull({se_workcompletedutc}) and isNotNull({se_completedutc}))',
                  },
                ],
              },
            },
            {
              createColumnsOperation: {
                columns: [
                  {
                    columnName: 'SM Status',
                    columnId: '008004e5-acdb-4b6c-9a5e-c26bd78940bc',
                    expression: "ifelse(\n{se_smcomplcode}<>1 and {se_smcomplcode}<>10 and isNotNull({se_workcompletedutc}), 'Completed',\n{se_smcomplcode}<>1 and {se_smcomplcode}<>10 and isNull({se_workcompletedutc}),'Open'\n,'')",
                  },
                ],
              },
            },
            {
              createColumnsOperation: {
                columns: [
                  {
                    columnName: 'UTL KPI',
                    columnId: '0c4f18fd-58fc-4d3c-bebf-adcad042d3cf',
                    expression: '{SM UTL}/{SM Adjusted}',
                  },
                ],
              },
            },
            {
              tagColumnOperation: {
                columnName: 'SM RFS',
                tags: [
                  {
                    columnDescription: {
                      text: 'Removed From Service',
                    },
                  },
                ],
              },
            },
            {
              tagColumnOperation: {
                columnName: 'SM CIU',
                tags: [
                  {
                    columnDescription: {
                      text: 'Continuos in Use',
                    },
                  },
                ],
              },
            },
            {
              tagColumnOperation: {
                columnName: 'SM UTL',
                tags: [
                  {
                    columnDescription: {
                      text: 'Unable to Located',
                    },
                  },
                ],
              },
            },
            {
              tagColumnOperation: {
                columnName: 'SM COT No',
                tags: [
                  {
                    columnDescription: {
                      text: 'Completed On Time No',
                    },
                  },
                ],
              },
            },
            {
              tagColumnOperation: {
                columnName: 'SM COT Yes',
                tags: [
                  {
                    columnDescription: {
                      text: 'Completed On Time Yes',
                    },
                  },
                ],
              },
            },
            {
              tagColumnOperation: {
                columnName: 'SM INCL',
                tags: [
                  {
                    columnDescription: {
                      text: 'Open',
                    },
                  },
                ],
              },
            },
            {
              tagColumnOperation: {
                columnName: 'SM ASM Completed',
                tags: [
                  {
                    columnDescription: {
                      text: 'COMPLETE, On-Time (60-day window)',
                    },
                  },
                ],
              },
            },
            {
              tagColumnOperation: {
                columnName: 'SM UFM',
                tags: [
                  {
                    columnDescription: {
                      text: 'Unavailable For Maintenance',
                    },
                  },
                ],
              },
            },
            {
              tagColumnOperation: {
                columnName: 'SM RSCD',
                tags: [
                  {
                    columnDescription: {
                      text: 'Closed, Rescheduled',
                    },
                  },
                ],
              },
            },
            {
              projectOperation: {
                projectedColumns: [
                  'db',
                  'client_id',
                  'system_id',
                  'region_id',
                  'system_name',
                  'facility_id',
                  'facility_name',
                  'facility_costcenterid',
                  'costcenter_name',
                  'cetag',
                  'modelname',
                  'manufacturer',
                  'devicename',
                  'noserial',
                  'se_sm',
                  'technician',
                  'serviceeventid',
                  'se_number',
                  'se_initiatedutc',
                  'se_workcompletedutc',
                  'se_completedutc',
                  'lifesupport',
                  'se_smcomplcode',
                  'total',
                  'se_state_name',
                  'managedsctype',
                  'state_name',
                  'sestat_name',
                  'resolution',
                  'l_seinitiatedbyid',
                  'l_equipmentstatusid',
                  'equipment_status',
                  'SM Scheduled',
                  'SM RFS',
                  'SM CIU',
                  'SM Adjusted',
                  'SM UTL',
                  'SM COT No',
                  'SM COT Yes',
                  'SM INCL',
                  'SM ASM Completed',
                  'SM UFM',
                  'SM RSCD',
                  'SM Completed Std',
                  'SM Adjusted Completed',
                  'SM Adjusted Closed',
                  'SM Adjusted Open',
                  'SM Scheduled Completed',
                  'SM Scheduled Closed',
                  'SM Scheduled Open',
                  '% Completed',
                  'Compl Displ',
                  '30% Target',
                  '60% Target',
                  '90% Target',
                  'High Risk',
                  'SM Scheduled Closed - Completed',
                  'SM Status',
                  'UTL KPI',
                ],
              },
            },
          ],
          source: {
            physicalTableId: 'vismstatusdashboardphy',
          },
        },
      },

      fieldFolders: {
        calculations: {
          columns: [
            'SM Scheduled',
            'SM Scheduled Open',
            'SM RFS',
            'SM Scheduled Closed',
            'SM CIU',
            'SM Adjusted',
            'SM UTL',
            'SM COT No',
            'SM COT Yes',
            'SM INCL',
            'SM ASM Completed',
            'SM UFM',
            'SM Completed Std',
            'SM RSCD',
            'SM Adjusted Completed',
            'SM Adjusted Closed',
            'SM Adjusted Open',
            'SM Scheduled Completed',
            '90% Target',
            '60% Target',
            '30% Target',
            'Compl Displ',
            '% Completed',
          ],
        },
      },
      rowLevelPermissionDataSet: {
        namespace: 'default',
        arn: 'arn:aws:quicksight:us-east-1:719035735300:dataset/rls_permissions_cdk',
        permissionPolicy: 'GRANT_ACCESS',
        formatVersion: 'VERSION_1',
        status: 'ENABLED',
      },
      dataSetUsageConfiguration: {
        disableUseAsDirectQuerySource: false,
        disableUseAsImportedSource: false,
      },
      permissions: [
        {
          principal: props.quicksightUserArn,
          actions: [
            'quicksight:DescribeDataSet',
            'quicksight:DescribeDataSetPermissions',
            'quicksight:PassDataSet',
            'quicksight:DescribeIngestion',
            'quicksight:ListIngestions',
            'quicksight:UpdateDataSet',
            'quicksight:UpdateDataSetPermissions',
          ],
        },
      ],


    });

    //Create rls permissions QuickSight dataset
    const rlPermissions = new quicksight.CfnDataSet(this, 'rls_permissions', {
      awsAccountId: this.account,
      dataSetId: 'rls_permissions_cdk',
      name: 'rls_permissions_cdk',
      importMode: 'DIRECT_QUERY',
      physicalTableMap: {
        rlspermissionsphy: {
          relationalTable: {
            dataSourceArn: redshiftDataSource.attrArn,
            schema: 'analytics',
            name: 'rls_permissions_cdk',
            inputColumns: [
              {
                name: 'username',
                type: 'STRING',
              },
              {
                name: 'db',
                type: 'STRING',
              },
              {
                name: 'facility_id',
                type: 'STRING',
              },
              {
                name: 'system_id',
                type: 'STRING',
              },
              {
                name: 'client_id',
                type: 'STRING',
              },
              {
                name: 'region_id',
                type: 'STRING',
              },
            ],
          },
        },
      },
      logicalTableMap: {
        rlspermissionslog: {
          alias: 'rls_permissions_cdk',
          dataTransforms: [
            {
              tagColumnOperation: {
                columnName: 'region_id',
                tags: [
                  {
                    columnGeographicRole: 'STATE',
                  },
                ],
              },
            },
          ],
          source: {
            physicalTableId: 'rlspermissionsphy',
          },
        },
      },

      dataSetUsageConfiguration: {
        disableUseAsDirectQuerySource: false,
        disableUseAsImportedSource: false,
      },
      permissions: [
        {
          principal: props.quicksightUserArn,
          actions: [
            'quicksight:DescribeDataSet',
            'quicksight:DescribeDataSetPermissions',
            'quicksight:PassDataSet',
            'quicksight:DescribeIngestion',
            'quicksight:ListIngestions',
            'quicksight:UpdateDataSetPermissions',
          ],
        },
      ],


    });

    viDmList.node.addDependency(rlPermissions);
    viPurchaseOrder.node.addDependency(rlPermissions);
    viSmStatus.node.addDependency(rlPermissions);


  }
}