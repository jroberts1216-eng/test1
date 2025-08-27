import { App, aws_ec2 as ec2 } from 'aws-cdk-lib';
import { Template } from 'aws-cdk-lib/assertions';
import { VpcIds } from '../src/common';
import { DmsInstanceStack, DmsRlStack, DmsCsmSourceStack } from '../src/dms';
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

  const instance = new DmsInstanceStack(app, 'instance', {
    env: {
      account: '123456789012',
      region: 'us-east-1',
    },
    vpcId: VpcIds.SERVICES,
    instanceType: ec2.InstanceType.of(ec2.InstanceClass.BURSTABLE3, ec2.InstanceSize.MEDIUM),
  });

  const source = new DmsRlStack(app, 'source', {
    env: {
      account: '123456789012',
      region: 'us-east-1',
    },
    secretPath: 'RenovoLive/Dev/QuickSight/DMS/DBConnectionDetails',
    rawBucket: s3Stack.rawBucket,
    replicationInstanceArn: instance.replicationInstanceArn,
  });

  const csmSource = new DmsCsmSourceStack(app, 'csmSource', {
    env: {
      account: '123456789012',
      region: 'us-east-1',
    },
    secretPath: 'CSM/Test/QuickSight/DMS/DBConnectionDetails',
    envPrefix: 'test',
  });

  const instanceTemplate = Template.fromStack(instance);
  const sourceTemplate = Template.fromStack(source);
  const csmSourceTemplate = Template.fromStack(csmSource);
  expect(instanceTemplate.toJSON()).toMatchSnapshot();
  expect(sourceTemplate.toJSON()).toMatchSnapshot();
  expect(csmSourceTemplate.toJSON()).toMatchSnapshot();
});