import {
  aws_iam as iam,
  Stack,
  StackProps,
} from 'aws-cdk-lib';
import { Construct } from 'constructs';

export class ReadOnlyPolicyStack extends Stack {
  constructor(scope: Construct, id: string, props: StackProps) {
    super(scope, id, props);

    new iam.ManagedPolicy(this, 'ReadOnlyGluePolicy', {
      managedPolicyName: 'glue-read-only-policy',
      description: 'Read only policy for Glue',
      statements: [
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: [
            'glue:Get*',
            'glue:List*',
            'glue:Query*',
            'glue:BatchGet*',
            'glue:Check*',
            'glue:Search*',
            'logs:DescribeLogGroups',
          ],
          resources: ['*'],
        }),
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: [
            's3:GetObject',
          ],
          resources: ['arn:aws:s3:::aws-glue-assets-*/*'],
        }),
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: [
            'logs:DescribeLogStreams',
          ],
          resources: [`arn:aws:logs:us-east-1:${Stack.of(this).account}:log-group:/aws-glue/*`],
        }),
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: [
            'logs:GetLogEvents',
          ],
          resources: [`arn:aws:logs:us-east-1:${Stack.of(this).account}:log-group:/aws-glue/*:log-stream:*`],
        }),
      ],
    });

    new iam.ManagedPolicy(this, 'ReadOnlyRedshiftPolicy', {
      managedPolicyName: 'redshift-read-only-policy',
      description: 'Read only policy for Redshift',
      statements: [
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: [
            'redshift:Describe*',
            'redshift:List*',
            'redshift:View*',
            'redshift:Get*',
            'redshift:Fetch*',
            'redshift-serverless:List*',
            'redshift-serverless:Describe*',
            'redshift-serverless:Get*',
            'redshift-data:List*',
            'redshift-data:Describe*',
            'redshift-data:Get*',
          ],
          resources: ['*'],
        }),
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: [
            'logs:DescribeLogStreams',
          ],
          resources: [`arn:aws:logs:us-east-1:${Stack.of(this).account}:log-group:/aws/redshift/*`],
        }),
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: [
            'logs:GetLogEvents',
          ],
          resources: [`arn:aws:logs:us-east-1:${Stack.of(this).account}:log-group:/aws/redshift/*:log-stream:*`],
        }),
      ],
    });

    new iam.ManagedPolicy(this, 'ReadOnlyQuickSightPolicy', {
      managedPolicyName: 'quicksight-read-only-policy',
      description: 'Read only policy for QuickSight',
      statements: [
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: [
            'quicksight:List*',
            'quicksight:Search*',
            'quicksight:Describe*',
            'quicksight:Get*',
            'quicksight:Pass*',
          ],
          resources: ['*'],
        }),
      ],
    });
  }
}