import { Duration } from 'aws-cdk-lib';
import { Alarm, ComparisonOperator, Metric, TreatMissingData } from 'aws-cdk-lib/aws-cloudwatch';
import { Effect, PolicyStatement } from 'aws-cdk-lib/aws-iam';
import { CfnStreamConsumer, IStream } from 'aws-cdk-lib/aws-kinesis';
import { EventSourceMapping, IFunction, StartingPosition } from 'aws-cdk-lib/aws-lambda';
import { SqsDlq } from 'aws-cdk-lib/aws-lambda-event-sources';
import { Queue, QueueEncryption } from 'aws-cdk-lib/aws-sqs';
import { Construct } from 'constructs';

export interface EnahncedFanOutEventProps {
  lambdaFunction: IFunction;
  kinesisStream: IStream;
  batchSize?: number | undefined;
  parallelizationFactor?: number | undefined;
  startingPosition?: StartingPosition | undefined;
  maxBatchingWindow?: Duration | undefined;
}

export class EnhancedFanOutEvent extends Construct {
  public readonly dlq: Queue;

  constructor(scope: Construct, id: string, props: EnahncedFanOutEventProps) {
    super(scope, id);

    const consumer = new CfnStreamConsumer(this, id + 'Consumer', {
      consumerName: id + 'Consumer',
      streamArn: props.kinesisStream.streamArn,
    });

    this.dlq = new Queue(this, 'DLQ', {
      encryption: QueueEncryption.KMS_MANAGED,
      dataKeyReuse: Duration.minutes(30)
    });

    new Alarm(this, 'DLQDepthAlarm', {
      metric: new Metric({
        namespace: 'AWS/SQS',
        metricName: 'ApproximateNumberOfMessagesVisible',
        dimensionsMap: {
          QueueName: this.dlq.queueName,
        },
        statistic: 'Sum',
        period: Duration.minutes(5)
      }),
      threshold: 1,
      comparisonOperator: ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
      evaluationPeriods: 1,
      treatMissingData: TreatMissingData.NOT_BREACHING
    });

    new EventSourceMapping(this, 'EventSourceMapping', {
      eventSourceArn: consumer.attrConsumerArn,
      target: props.lambdaFunction,
      startingPosition: props.startingPosition,
      batchSize: props.batchSize,
      maxBatchingWindow: props.maxBatchingWindow,
      parallelizationFactor: props.parallelizationFactor,
      onFailure: new SqsDlq(this.dlq),
      retryAttempts: 0,
    });

    props.lambdaFunction.addToRolePolicy(
      new PolicyStatement({
        actions: ['kinesis:SubscribeToShard'],
        effect: Effect.ALLOW,
        resources: [consumer.attrConsumerArn],
      })
    );
  }
}
