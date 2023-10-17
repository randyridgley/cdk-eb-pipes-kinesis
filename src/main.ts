import { App, Duration, Stack, StackProps } from 'aws-cdk-lib';
import { Role, ServicePrincipal } from 'aws-cdk-lib/aws-iam';
import { Stream } from 'aws-cdk-lib/aws-kinesis';
import { Code, Function, Runtime, StartingPosition } from 'aws-cdk-lib/aws-lambda';
import { CfnPipe } from 'aws-cdk-lib/aws-pipes';
import { Construct } from 'constructs';
import { EnhancedFanOutEvent } from './constructs/enhanced-fanout';
import { S3DeliveryPipeline } from './constructs/s3-delivery-stream';
import { Bucket } from 'aws-cdk-lib/aws-s3';
import { Database } from '@aws-cdk/aws-glue-alpha';
import { Key } from 'aws-cdk-lib/aws-kms';

export class PipesAndStreamsStack extends Stack {
  constructor(scope: Construct, id: string, props: StackProps = {}) {
    super(scope, id, props);

    const sourceStream = new Stream(this, 'sourceStream', {
      shardCount: 1,
      streamName: 'eb-source-stream',
    });

    const targetStream = new Stream(this, 'targetStream', {
      shardCount: 1,
      streamName: 'eb-target-stream',
    });

    const pipeRole = new Role(this, 'pipeRole', {
      assumedBy: new ServicePrincipal('pipes.amazonaws.com'),
    });
    sourceStream.grantRead(pipeRole);
    targetStream.grantWrite(pipeRole);

    new CfnPipe(this, 'pipe', {
      roleArn: pipeRole.roleArn,
      source: sourceStream.streamArn,
      target: targetStream.streamArn,
      sourceParameters: {
        kinesisStreamParameters: {
          maximumBatchingWindowInSeconds: 1,
          onPartialBatchItemFailure: 'AUTOMATIC_BISECT',
          startingPosition: 'TRIM_HORIZON',          
        },
      },
      targetParameters: {
        inputTemplate: `{"name": <$.data.name>, "age": "<$.data.age>", "m_time": <$.data.m_time>}\n`,
        kinesisStreamParameters: {
          partitionKey: '$.partitionKey',
        },
      },
    });
    
    const lambdaFunction = new Function(this, 'Function', {
      code: Code.fromAsset('src/lambda/consumer'),
      handler: 'index.handler',
      functionName: 'EBTargetKinesisMessageHandler',
      runtime: Runtime.NODEJS_18_X,
    });
    targetStream.grantRead(lambdaFunction);

    new EnhancedFanOutEvent(this, 'EnhancedFanOutEvent', {
      kinesisStream: targetStream,
      lambdaFunction: lambdaFunction,
      batchSize: 100,
      maxBatchingWindow: Duration.seconds(1),      
      startingPosition: StartingPosition.LATEST, 
    });

    const bucket = new Bucket(this, 'S3Bucket');

    const database = new Database(this, 'PipesDatabase', {
      databaseName: 'pipes'
    });

    const key = new Key(this, 'PipeKey');

    new S3DeliveryPipeline(this, 'S3DeliveryStream', {
      baseTableName: 'peoples',
      bucket: bucket,
      database: database,
      deliveryStreamName: 'peoples-on-fire',
      kmsKey: key,
      processedColumns: [
        {
          name: 'name',
          type: 'string',
          comment: 'it\'s the name silly',
        },
        {
          name: 'age',
          type: 'int',
          comment: 'it\'s the age silly',        
        },
        {
          name: 'm_time',
          type: 'timestamp',
          comment: 'it\'s the time silly',
        }
      ],
      rawColumns: [
        {
          name: 'name',
          type: 'string',
          comment: 'it\'s the name silly',
        },
        {
          name: 'age',
          type: 'int',
          comment: 'it\'s the age silly',        
        },
        {
          name: 'm_time',
          type: 'timestamp',
          comment: 'it\'s the time silly',
        }
      ],
      timestampColumn: 'm_time',
      stream: targetStream
    });
  }
}

// for development, use account/region from cdk cli
const devEnv = {
  account: process.env.CDK_DEFAULT_ACCOUNT,
  region: process.env.CDK_DEFAULT_REGION,
};

const app = new App();

new PipesAndStreamsStack(app, 'cdk-eb-pipes-kinesis-dev', { env: devEnv });

app.synth();