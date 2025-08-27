import { App, aws_ec2 as ec2 } from 'aws-cdk-lib';
import { Template } from 'aws-cdk-lib/assertions';
import { VpcIds } from '../src/common';
import { DmsInstanceStack, DmsRlStack } from '../src/dms';
import { GlueStack } from '../src/glue';
import { ReadOnlyPolicyStack } from '../src/readonlypolicy';
import { RedshiftStack } from '../src/redshift-serverless';
import { S3Stack } from '../src/s3';

test('Snapshot', () => {
  const app = new App();

  const s3Stack = new S3Stack(app, 'S3Stack', {
    env: {
      account: '123456789012',
      region: 'us-east-1',
    },
    bucketNamePrefix: 'rl-quicksight',
  });

  const dmsInstanceStack = new DmsInstanceStack(app, 'DmsInstanceStack', {
    env: {
      account: '123456789012',
      region: 'us-east-1',
    },
    vpcId: VpcIds.SERVICES,
    instanceType: ec2.InstanceType.of(ec2.InstanceClass.BURSTABLE3, ec2.InstanceSize.MEDIUM),
  });

  new DmsRlStack(app, 'DmsRlStack', {
    env: {
      account: '123456789012',
      region: 'us-east-1',
    },
    secretPath: 'RenovoLive/Dev/QuickSight/DMS/DBConnectionDetails',
    rawBucket: s3Stack.rawBucket,
    replicationInstanceArn: dmsInstanceStack.replicationInstanceArn,
  });

  const redshift = new RedshiftStack(app, 'instance', {
    env: {
      account: '123456789012',
      region: 'us-east-1',
    },
    vpcId: VpcIds.SERVICES,
    namespaceName: 'renovo-namespace',
    workgroupName: 'renovo-workgroup',
    baseCapacity: 32,
    enhancedVpcRouting: false,
    publiclyAccessible: true,
    rawBucket: s3Stack.rawBucket,
  });

  const glue = new GlueStack(app, 'source', {
    env: {
      account: '123456789012',
      region: 'us-east-1',
    },
    roleName: 'AWSGlueServiceRole',
    crawlerNameClRenovo: 's3-renovo-crawler',
    crawlerNameRenovoMaster: 's3-renovomaster-crawler',
    crawlerNameUnifiedDb: 's3-unifieddb-crawler',
    glueScriptPath: '../src/glue-scripts',
    rawBucket: s3Stack.rawBucket,
    glueScriptsBucket: s3Stack.glueScriptsBucket,
  });

  const readOnlyPolicy = new ReadOnlyPolicyStack(app, 'ReadOnlyPolicyStack', {
    env: {
      account: '123456789012',
      region: 'us-east-1',
    },
  });

  const instanceTemplate = Template.fromStack(redshift);
  const sourceTemplate = Template.fromStack(glue);
  const readOnlyPolicyTemplate = Template.fromStack(readOnlyPolicy);
  expect(instanceTemplate.toJSON()).toMatchSnapshot();
  expect(sourceTemplate.toJSON()).toMatchSnapshot();
  expect(readOnlyPolicyTemplate.toJSON()).toMatchSnapshot();
});