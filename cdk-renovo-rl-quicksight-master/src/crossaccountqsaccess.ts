import {
  aws_iam as iam,
  Stack,
  StackProps,
} from 'aws-cdk-lib';
import { NagSuppressions } from 'cdk-nag';
import { Construct } from 'constructs';

export interface RenovoLiveQuickSightCrossAccountAccessStackProps extends StackProps {
  /**
   * The account IDs that will be granted access to actions
   * in the QuickSight account
   */
  readonly accountIds: string[];
  /**
   * The external ID that will be used to assume the role
   */
  readonly externalId: string;
  /**
   * The name of the role that will be created
   */
  readonly roleName: string;
}

export class RenovoLiveQuickSightCrossAccountAccessStack extends Stack {
  constructor(scope: Construct, id: string, props: RenovoLiveQuickSightCrossAccountAccessStackProps) {
    super(scope, id, props);

    const role = new iam.Role(this, 'QuickSightCrossAccountRole', {
      roleName: props.roleName,
      assumedBy: new iam.PrincipalWithConditions(
        new iam.CompositePrincipal(
          ...props.accountIds.map(accountId => new iam.AccountPrincipal(accountId)),
        ),
        {
          StringEquals: {
            'sts:ExternalId': props.externalId,
          },
        },
      ),
    });

    /**
     * TODO: Pending guidance on how we can limit RenovoLive to generating embed URLs
     * for specific dashboards only based on their usage being intended for RenovoLive.
     * As this currently stands applications or users with access to assume a role with
     * this policy can generate embed URLs for any user in the QuickSight account for any
     * dashboard they have access to.
     */
    const quickSightPolicy = new iam.ManagedPolicy(this, 'QuickSightCrossAccountPolicy', {
      description: 'Policy to be used with cross-account access to QuickSight',
      statements: [
        new iam.PolicyStatement({
          sid: 'Default',
          effect: iam.Effect.ALLOW,
          actions: [
            'quicksight:ListUsers',
            'quicksight:DescribeUser',
            'quicksight:RegisterUser',
            'quicksight:ListDashboards',
            'quicksight:DescribeDashboard',
            'quicksight:ListDashboardVersions',
            'quicksight:GetSessionEmbedUrl',
            'quicksight:CreateGroupMembership',
            'quicksight:DeleteGroupMembership',
            'quicksight:DeleteUser',
          ],
          resources: ['*'],
        }),
        new iam.PolicyStatement({
          sid: 'AllowEmbedForRegisteredUser',
          effect: iam.Effect.ALLOW,
          actions: [
            'quicksight:GenerateEmbedUrlForRegisteredUser',
            'quicksight:GetAuthCode',
            'quicksight:SearchDashboards',
          ],
          resources: [
            `arn:aws:quicksight:${Stack.of(this).region}:${Stack.of(this).account}:user/*`,
          ],
        }),
        new iam.PolicyStatement({
          sid: 'AllowSearchDashboards',
          effect: iam.Effect.ALLOW,
          actions: [
            'quicksight:SearchDashboards',
          ],
          resources: [
            `arn:aws:quicksight:${Stack.of(this).region}:${Stack.of(this).account}:dashboard/*`,
          ],
        }),
      ],
    });

    NagSuppressions.addResourceSuppressions(quickSightPolicy, [
      {
        id: 'AwsSolutions-IAM5',
        reason: 'This policy is intended to be used with cross-account access to QuickSight and the wildcarded permissions are necessary for the use case. \
        See role policy guidance here: https://aws.amazon.com/blogs/modernizing-with-aws/embedding-amazon-quicksight-analytics-in-net-applications/',
      },
    ]);

    role.addManagedPolicy(quickSightPolicy);
  }
}

export interface QuickSightCrossAccountRoleAssumptionPolicyProps extends StackProps {
  /**
   * The role arn that will be assumable
   */
  readonly roleArn: string;
  /**
   * The name of the policy
   */
  readonly policyName: string;
};

export class QuickSightCrossAccountRoleAssumptionPolicy extends Stack {
  constructor(scope: Construct, id: string, props: QuickSightCrossAccountRoleAssumptionPolicyProps) {
    super(scope, id, props);

    new iam.ManagedPolicy(this, 'QuickSightCrossAccountRoleAssumptionPolicy', {
      managedPolicyName: props.policyName,
      description: 'Policy to be used to assume the QuickSight cross-account role',
      statements: [
        new iam.PolicyStatement({
          sid: 'AssumeRole',
          effect: iam.Effect.ALLOW,
          actions: [
            'sts:AssumeRole',
          ],
          resources: [
            props.roleArn,
          ],
        }),
      ],
    });
  }
}
