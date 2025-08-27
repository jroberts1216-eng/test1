import {
  aws_iam as iam,
  CfnOutput,
  Stack,
  StackProps,
} from 'aws-cdk-lib';
import { Construct } from 'constructs';

export interface CloudWatchDashboardAccessStackProps extends StackProps {
  /**
   * Optional policy name. If not provided, a name will be generated.
   */
  policyName?: string;
}

/**
 * A stack that creates a managed policy granting access to CloudWatch dashboards and their widgets
 */
export class CloudWatchDashboardAccessStack extends Stack {
  /**
   * The managed policy created by this stack
   */
  public readonly dashboardAccessPolicy: iam.ManagedPolicy;

  constructor(scope: Construct, id: string, props: CloudWatchDashboardAccessStackProps) {
    super(scope, id, props);

    // Create the managed policy
    this.dashboardAccessPolicy = new iam.ManagedPolicy(this, 'CloudWatchDashboardAccess', {
      managedPolicyName: props.policyName,
      description: 'Grants access to CloudWatch dashboards and the widgets they contain',
      statements: [
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: [
            'cloudwatch:GetDashboard',
            'cloudwatch:ListDashboards',
            'cloudwatch:GetMetricData',
            'cloudwatch:GetMetricStatistics',
            'cloudwatch:DescribeAlarms',
            'cloudwatch:GetMetricWidgetImage',
          ],
          resources: ['*'],
        }),
      ],
    });

    // Output the ARN of the created policy
    new CfnOutput(this, 'DashboardAccessPolicyArn', {
      value: this.dashboardAccessPolicy.managedPolicyArn,
      description: 'ARN of the CloudWatch Dashboard Access Policy',
      exportName: `${this.stackName}-DashboardAccessPolicyArn`,
    });
  }
}
