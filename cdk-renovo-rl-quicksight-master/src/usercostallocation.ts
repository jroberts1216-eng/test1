import {
  aws_lambda as lambda,
  aws_sqs as sqs,
  aws_events as events,
  aws_events_targets as targets,
  aws_iam as iam,
  aws_lambda_event_sources as lambda_event_sources,
  Stack,
  StackProps,
  Duration,
} from 'aws-cdk-lib';
import { Construct } from 'constructs';

export interface QuickSightUserCostAllocationProps extends StackProps {
  /**
   * The cost allocation tags and values to set on the QuickSight users
   */
  readonly tags: { [key: string]: string };
}

export class QuickSightUserCostAllocation extends Stack {
  constructor(scope: Construct, id: string, props: QuickSightUserCostAllocationProps) {
    super(scope, id, props);

    const dlg = new sqs.Queue(this, 'dlq', {
      visibilityTimeout: Duration.seconds(30),
    });

    const sqsQueue = new sqs.Queue(this, 'queue', {
      visibilityTimeout: Duration.seconds(30),
      deadLetterQueue: {
        queue: dlg,
        maxReceiveCount: 3,
      },
    });

    const producerRole = new iam.Role(this, 'producer-role', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole')],
    });

    const producerPolicy = new iam.ManagedPolicy(this, 'producer-policy', {
      statements: [
        new iam.PolicyStatement({
          actions: ['sqs:SendMessage'],
          resources: [sqsQueue.queueArn],
        }),
        new iam.PolicyStatement({
          actions: [
            'quicksight:ListNamespaces',
            'quicksight:ListUsers',
            'quicksight:ListTagsForResource',
          ],
          resources: ['*'],
        }),
      ],
    });

    producerRole.addManagedPolicy(producerPolicy);

    const producer = new lambda.Function(this, 'producer', {
      runtime: lambda.Runtime.PYTHON_3_10,
      handler: 'index.handler',
      code: lambda.Code.fromAsset(`${__dirname}/handlers/costallocation/producer`),
      environment: {
        TAGS_JSON: JSON.stringify(props.tags),
        SQS_QUEUE_URL: sqsQueue.queueUrl,
      },
      role: producerRole,
      timeout: Duration.seconds(180),
    });

    new events.Rule(this, 'rule', {
      schedule: events.Schedule.cron({ minute: '0', hour: '0' }),
      targets: [new targets.LambdaFunction(producer)],
    });

    const consumerRole = new iam.Role(this, 'consumer-role', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole')],
    });

    const consumerPolicy = new iam.ManagedPolicy(this, 'consumer-policy', {
      statements: [
        new iam.PolicyStatement({
          actions: [
            'quicksight:TagResource',
          ],
          resources: ['*'],
        }),
        new iam.PolicyStatement({
          actions: [
            'sqs:DeleteMessage',
          ],
          resources: ['*'],
        }),
      ],
    });

    consumerRole.addManagedPolicy(consumerPolicy);

    const consumer = new lambda.Function(this, 'consumer', {
      runtime: lambda.Runtime.PYTHON_3_10,
      handler: 'index.handler',
      code: lambda.Code.fromAsset(`${__dirname}/handlers/costallocation/consumer`),
      role: consumerRole,
      timeout: Duration.seconds(30),
    });

    consumer.addEventSource(new lambda_event_sources.SqsEventSource(sqsQueue, {
      batchSize: 10,
    }));
  }
}
