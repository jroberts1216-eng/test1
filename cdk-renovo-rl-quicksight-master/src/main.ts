import { App, Aspects } from 'aws-cdk-lib';
import { AwsSolutionsChecks, NIST80053R5Checks } from 'cdk-nag';
import { QuickSightCloudWatchDashboard } from './cloudwatch';
import { Accounts, RenovoLiveEnv, VpcIds, RLDBEndpoints, RLDBCredSecretArns } from './common';
import { RenovoLiveQuickSightCrossAccountAccessStack, QuickSightCrossAccountRoleAssumptionPolicy } from './crossaccountqsaccess';
import {
  dbClCarle,
  dbClCentro,
  dbClChrist,
  dbClDev,
  dbClEcmc,
  dbClRadon,
  dbClRadonVa,
  dbClRenovo,
  dbClVertexUk,
  dbClZoli,
  dbRenovoMaster,
} from './databases/renovolive';
import { RenovoLiveDataSources } from './datasources';
import { RLDataExportStacks, RLDbDmsNotificationStack } from './dms';
import { RLDbGlueStack } from './glue';
import { CloudWatchDashboardAccessStack } from './iam';
import { QuickSightConfig } from './quicksight';
import { RenovoLiveDmDataSet } from './quicksight-datasets/dm';
import { RenovoLiveSeRlsPermsDataSet } from './quicksight-datasets/se_rls_perms';
import { ReadOnlyPolicyStack } from './readonlypolicy';
import { RenovoLiveRedshiftStack } from './redshift-serverless';
import { DataBucketsStack } from './s3';
import { ApplyTags } from './tagsAspect';
import { QuickSightUserCostAllocation } from './usercostallocation';

const app = new App();

const appAspects = Aspects.of(app);

appAspects.add(
  new ApplyTags({
    'map-migrated': 'mig7S6T5G3TOV',
    'workloadId': 'renovolive-qs',
  }),
);

// appAspects.add(new AwsSolutionsChecks());
// appAspects.add(new NIST80053R5Checks());

const quickSightConfig = new QuickSightConfig(app, 'RlDevQuickSighConfig', {
  env: {
    account: Accounts.SERVICES,
    region: 'us-east-1',
  },
  vpcId: VpcIds.SERVICES,
  quicksightUserArn: 'arn:aws:quicksight:us-east-1:719035735300:group/default/test_group',
});

const renovoLiveCrossAccountRoleName = 'renovolive-quicksight-cross-account-role';

const renovoLiveQuickSightCrossAccountAccess = new RenovoLiveQuickSightCrossAccountAccessStack(app, 'QuickSightCrossAccountAccessRenovoLive', {
  env: {
    account: Accounts.SERVICES,
    region: 'us-east-1',
  },
  accountIds: [Accounts.RLDEV, Accounts.RLPROD],
  externalId: 'da5f713c-35b9-487b-bfc7-4b91e2e63e38',
  roleName: renovoLiveCrossAccountRoleName,
});

const renovoLiveCrossAccountQuickSightAccessStackAspects = Aspects.of(renovoLiveQuickSightCrossAccountAccess);

renovoLiveCrossAccountQuickSightAccessStackAspects.add(new AwsSolutionsChecks());
renovoLiveCrossAccountQuickSightAccessStackAspects.add(new NIST80053R5Checks());

new QuickSightCrossAccountRoleAssumptionPolicy(app, 'RlDevQuickSightCrossAccountRoleAssumptionPolicy', {
  env: {
    account: Accounts.RLDEV,
    region: 'us-east-1',
  },
  roleArn: `arn:aws:iam::${Accounts.SERVICES}:role/${renovoLiveCrossAccountRoleName}`,
  policyName: 'renovolive-quicksight-cross-account-policy',
});

new QuickSightCrossAccountRoleAssumptionPolicy(app, 'RlProdQuickSightCrossAccountRoleAssumptionPolicy', {
  env: {
    account: Accounts.RLPROD,
    region: 'us-east-1',
  },
  roleArn: `arn:aws:iam::${Accounts.SERVICES}:role/${renovoLiveCrossAccountRoleName}`,
  policyName: 'renovolive-quicksight-cross-account-policy',
});

new QuickSightUserCostAllocation(app, 'RlDevQuickSightUserCostAllocation', {
  env: {
    account: Accounts.SERVICES,
    region: 'us-east-1',
  },
  tags: {
    'map-migrated': 'mig7S6T5G3TOV',
    'workloadId': 'renovolive-qs',
  },
});

const dataBuckets = new DataBucketsStack(app, 'RLDataBuckets', {
  env: {
    account: Accounts.SERVICES,
    region: 'us-east-1',
  },
  rawDataBucketName: 'renovolive-rawdata',
  glueScriptsBucketName: 'renovolive-gluescripts',
  glueScratchBucketName: 'renovolive-gluescratch',
  kmsKeysNamespace: '/renovolive/dataexport',
});

RLDataExportStacks.createStacks({
  renovoLiveEnv: RenovoLiveEnv.DEV,
  instanceAllocations: [
    {
      databases: [dbRenovoMaster, dbClRenovo, dbClDev],
      id: 'default',
    },
  ],
  rawBucket: dataBuckets.rawDataBucket,
  kmsKey: dataBuckets.dataKmsKey,
  app,
});

const prodDbsDefault = [
  dbRenovoMaster,
  dbClRenovo,
  dbClDev,
];

const prodDbsClients = [
  dbClCarle,
  dbClCentro,
  dbClChrist,
  dbClEcmc,
  dbClRadon,
  dbClRadonVa,
  dbClVertexUk,
  dbClZoli,
];

const prodDbs = [
  ...prodDbsDefault,
  ...prodDbsClients,
];

RLDataExportStacks.createStacks({
  renovoLiveEnv: RenovoLiveEnv.PROD,
  instanceAllocations: [
    {
      databases: prodDbsDefault,
      id: 'default',
    },
    {
      databases: prodDbsClients,
      id: 'clients',
    },
  ],
  rawBucket: dataBuckets.rawDataBucket,
  kmsKey: dataBuckets.dataKmsKey,
  app,
});

const devRLRedshift = new RenovoLiveRedshiftStack(app, 'DevRenovoLiveRedshift', {
  env: {
    account: Accounts.SERVICES,
    region: 'us-east-1',
  },
  vpcId: VpcIds.SERVICES,
  rawBucket: dataBuckets.rawDataBucket,
  renovoLiveEnv: RenovoLiveEnv.DEV,
  rawBucketKmsKey: dataBuckets.dataKmsKey,
  quickSightSecurityGroup: quickSightConfig.vpcConnectionSecurityGroup,
  databases: [dbRenovoMaster, dbClRenovo, dbClDev],
});

const prodRLRedshift = new RenovoLiveRedshiftStack(app, 'ProdRenovoLiveRedshift', {
  env: {
    account: Accounts.SERVICES,
    region: 'us-east-1',
  },
  vpcId: VpcIds.SERVICES,
  rawBucket: dataBuckets.rawDataBucket,
  renovoLiveEnv: RenovoLiveEnv.PROD,
  rawBucketKmsKey: dataBuckets.dataKmsKey,
  quickSightSecurityGroup: quickSightConfig.vpcConnectionSecurityGroup,
  databases: prodDbs,
});

new RLDbGlueStack(app, 'DevRLDbGlueStack', {
  env: {
    account: Accounts.SERVICES,
    region: 'us-east-1',
  },
  rawDataBucket: dataBuckets.rawDataBucket,
  dataKmsKey: dataBuckets.dataKmsKey,
  renovoLiveEnv: RenovoLiveEnv.DEV,
  redshiftRole: devRLRedshift.role,
  databases: [dbRenovoMaster, dbClRenovo, dbClDev],
  redshiftWorkgroup: devRLRedshift.workgroup,
  rawIngestionTopic: dataBuckets.rawIngestionTopic,
});

new RLDbGlueStack(app, 'ProdRLDbGlueStack', {
  env: {
    account: Accounts.SERVICES,
    region: 'us-east-1',
  },
  rawDataBucket: dataBuckets.rawDataBucket,
  dataKmsKey: dataBuckets.dataKmsKey,
  renovoLiveEnv: RenovoLiveEnv.PROD,
  redshiftRole: prodRLRedshift.role,
  databases: prodDbs,
  redshiftWorkgroup: prodRLRedshift.workgroup,
  rawIngestionTopic: dataBuckets.rawIngestionTopic,
});

const devRlDatasource = new RenovoLiveDataSources(app, 'DevRenovoLiveDataSources', {
  env: {
    account: Accounts.SERVICES,
    region: 'us-east-1',
  },
  renovoLiveEnv: RenovoLiveEnv.DEV,
  redshiftWorkgroup: devRLRedshift.workgroup,
  quickSightVpcConnection: quickSightConfig.quickSightVpcConnection,
  folderArn: `arn:aws:quicksight:us-east-1:${Accounts.SERVICES}:folder/9c03505a-fb6c-4fbe-94ad-5ea69af34a69`, // manually created folder
  redshiftSecretArn: 'arn:aws:secretsmanager:us-east-1:719035735300:secret:redshift!dev-renovolive-admin-guRcHA', // Requires the FULL arn with suffix which means we cant use a lookup in CDK
  sqlServerEndpoint: RLDBEndpoints.DEV,
  sqlServerSecretArn: RLDBCredSecretArns.DEV,
});

const prodRlDatasource = new RenovoLiveDataSources(app, 'ProdRenovoLiveDataSources', {
  env: {
    account: Accounts.SERVICES,
    region: 'us-east-1',
  },
  renovoLiveEnv: RenovoLiveEnv.PROD,
  redshiftWorkgroup: prodRLRedshift.workgroup,
  quickSightVpcConnection: quickSightConfig.quickSightVpcConnection,
  folderArn: `arn:aws:quicksight:us-east-1:${Accounts.SERVICES}:folder/6e1bb85c-da6d-407f-b5d8-1e0c5e5dd06b`, // manually created folder
  redshiftSecretArn: 'arn:aws:secretsmanager:us-east-1:719035735300:secret:redshift!prod-renovolive-admin-6nSwGh', // Requires the FULL arn with suffix which means we cant use a lookup in CDK
  sqlServerEndpoint: RLDBEndpoints.PROD,
  sqlServerSecretArn: RLDBCredSecretArns.PROD,
});

const devClDevSePerms = new RenovoLiveSeRlsPermsDataSet(app, 'DevClDevRenovoLiveSeRlsPermsDataSet', {
  env: {
    account: Accounts.SERVICES,
    region: 'us-east-1',
  },
  renovoLiveEnv: RenovoLiveEnv.DEV,
  folderArn: `arn:aws:quicksight:us-east-1:${Accounts.SERVICES}:folder/64fb79ac-ee7d-43f6-be31-160a7eb25604`, // manually created folder
  dataSource: devRlDatasource.sqlServerDataSource,
  database: 'cldev',
});

const devClRenovoSePerms = new RenovoLiveSeRlsPermsDataSet(app, 'DevClRenovoRenovoLiveSeRlsPermsDataSet', {
  env: {
    account: Accounts.SERVICES,
    region: 'us-east-1',
  },
  renovoLiveEnv: RenovoLiveEnv.DEV,
  folderArn: `arn:aws:quicksight:us-east-1:${Accounts.SERVICES}:folder/64fb79ac-ee7d-43f6-be31-160a7eb25604`, // manually created folder
  dataSource: devRlDatasource.sqlServerDataSource,
  database: 'clrenovo',
});

const prodClDevSePerms = new RenovoLiveSeRlsPermsDataSet(app, 'ProdClDevRenovoLiveSeRlsPermsDataSet', {
  env: {
    account: Accounts.SERVICES,
    region: 'us-east-1',
  },
  renovoLiveEnv: RenovoLiveEnv.PROD,
  folderArn: `arn:aws:quicksight:us-east-1:${Accounts.SERVICES}:folder/64fb79ac-ee7d-43f6-be31-160a7eb25604`, // manually created folder
  dataSource: prodRlDatasource.sqlServerDataSource,
  database: 'cldev',
});

const prodClRenovoSePerms = new RenovoLiveSeRlsPermsDataSet(app, 'ProdClRenovoRenovoLiveSeRlsPermsDataSet', {
  env: {
    account: Accounts.SERVICES,
    region: 'us-east-1',
  },
  renovoLiveEnv: RenovoLiveEnv.PROD,
  folderArn: `arn:aws:quicksight:us-east-1:${Accounts.SERVICES}:folder/64fb79ac-ee7d-43f6-be31-160a7eb25604`, // manually created folder
  dataSource: prodRlDatasource.sqlServerDataSource,
  database: 'clrenovo',
});

const devClDevDm = new RenovoLiveDmDataSet(app, 'DevClDevRenovoLiveDmDataSet', {
  env: {
    account: Accounts.SERVICES,
    region: 'us-east-1',
  },
  renovoLiveEnv: RenovoLiveEnv.DEV,
  folderArn: `arn:aws:quicksight:us-east-1:${Accounts.SERVICES}:folder/64fb79ac-ee7d-43f6-be31-160a7eb25604`, // manually created folder
  dataSource: devRlDatasource.redshiftDataSource,
  database: 'cldev',
  createRefreshSchedules: true,
  rowLevelPermissionsDataSetArn: `arn:aws:quicksight:us-east-1:719035735300:dataset/${RenovoLiveEnv.DEV}_cldev_vRlsServiceEvents`,
});

devClDevDm.addDependency(devClDevSePerms);

const devClRenovoDm = new RenovoLiveDmDataSet(app, 'DevClRenovoRenovoLiveDmDataSet', {
  env: {
    account: Accounts.SERVICES,
    region: 'us-east-1',
  },
  renovoLiveEnv: RenovoLiveEnv.DEV,
  folderArn: `arn:aws:quicksight:us-east-1:${Accounts.SERVICES}:folder/64fb79ac-ee7d-43f6-be31-160a7eb25604`, // manually created folder
  dataSource: devRlDatasource.redshiftDataSource,
  database: 'clrenovo',
  createRefreshSchedules: true,
  rowLevelPermissionsDataSetArn: `arn:aws:quicksight:us-east-1:719035735300:dataset/${RenovoLiveEnv.DEV}_clrenovo_vRlsServiceEvents`,
});

devClRenovoDm.addDependency(devClRenovoSePerms);

const prodClDevDm = new RenovoLiveDmDataSet(app, 'ProdClDevRenovoLiveDmDataSet', {
  env: {
    account: Accounts.SERVICES,
    region: 'us-east-1',
  },
  renovoLiveEnv: RenovoLiveEnv.PROD,
  folderArn: `arn:aws:quicksight:us-east-1:${Accounts.SERVICES}:folder/0539a7af-0e51-4ac9-a6df-46e37fe702ce`, // manually created folder
  dataSource: prodRlDatasource.redshiftDataSource,
  database: 'cldev',
  createRefreshSchedules: true,
  rowLevelPermissionsDataSetArn: `arn:aws:quicksight:us-east-1:719035735300:dataset/${RenovoLiveEnv.PROD}_cldev_vRlsServiceEvents`,
});

prodClDevDm.addDependency(prodClDevSePerms);

const prodClRenovoDm = new RenovoLiveDmDataSet(app, 'ProdClRenovoRenovoLiveDmDataSet', {
  env: {
    account: Accounts.SERVICES,
    region: 'us-east-1',
  },
  renovoLiveEnv: RenovoLiveEnv.PROD,
  folderArn: `arn:aws:quicksight:us-east-1:${Accounts.SERVICES}:folder/0539a7af-0e51-4ac9-a6df-46e37fe702ce`, // manually created folder
  dataSource: prodRlDatasource.redshiftDataSource,
  database: 'clrenovo',
  createRefreshSchedules: true,
  rowLevelPermissionsDataSetArn: `arn:aws:quicksight:us-east-1:719035735300:dataset/${RenovoLiveEnv.PROD}_clrenovo_vRlsServiceEvents`,
});

prodClRenovoDm.addDependency(prodClRenovoSePerms);

new RLDbDmsNotificationStack(app, 'RenovoLiveDbDmsNotification', {
  env: {
    account: Accounts.SERVICES,
    region: 'us-east-1',
  },
});

new ReadOnlyPolicyStack(app, 'ReadOnlyPolicyStack', {
  env: {
    account: Accounts.SERVICES,
    region: 'us-east-1',
  },
});

new QuickSightCloudWatchDashboard(app, 'RLCloudWatchDashboard', {
  env: {
    account: Accounts.SERVICES,
    region: 'us-east-1',
  },
  dashboardName: 'prod-clrenovo-renovolive-dm-dashboard',
  datasetId: 'prod_clrenovo_v_qs_dm',
  quickSightDashboardId: 'prod-clrenovo-renovolive-dm-dashboard',
});

new CloudWatchDashboardAccessStack(app, 'CloudWatchDashboardAccessStack', {
  env: {
    account: Accounts.SERVICES,
    region: 'us-east-1',
  },
  policyName: 'CloudWatchDashboardAccessPolicy',
});

app.synth();