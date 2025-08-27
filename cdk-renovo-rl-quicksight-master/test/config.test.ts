import { App } from 'aws-cdk-lib';
import { Template } from 'aws-cdk-lib/assertions';
import { QuickSightConfig } from '../src/quicksight';

test('Snapshot', () => {
  const app = new App();

  const quickSightConfig = new QuickSightConfig(app, 'RlDevQuickSightConfig', {
    env: {
      account: '123456789012',
      region: 'us-east-1',
    },
    vpcId: 'vpc-123abc456789d10e11f',
    quicksightUserArn: 'arn:aws:quicksight:us-east-1:1234567890:user/default',
  });

  const quickSightTemplate = Template.fromStack(quickSightConfig);
  expect(quickSightTemplate.toJSON()).toMatchSnapshot();
});