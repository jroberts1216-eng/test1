import { App } from 'aws-cdk-lib';
import { Template } from 'aws-cdk-lib/assertions';
import { RenovoLiveQuickSightCrossAccountAccessStack } from '../src/crossaccountqsaccess';

test('Snapshot', () => {
  const app = new App();

  const renovoLiveQuickSightCrossAccountAccess = new RenovoLiveQuickSightCrossAccountAccessStack(app, 'QuickSightCrossAccountAccessRenovoLive', {
    env: {
      account: '123456789012',
      region: 'us-east-1',
    },
    accountIds: ['123456789013', '123456789014'],
    externalId: 'ab1c723d-45e6-789f-ghi1-4j23k4l56m78',
    roleName: 'renovolive-quicksight-cross-account-role',
  });

  const renovoLiveQuickSightCrossAccountAccessTemplate = Template.fromStack(renovoLiveQuickSightCrossAccountAccess);
  expect(renovoLiveQuickSightCrossAccountAccessTemplate.toJSON()).toMatchSnapshot();
});