import { CloudWatchEncryptionMode, Database, S3EncryptionMode, SecurityConfiguration } from '@aws-cdk/aws-glue-alpha';
import { Aws, CfnOutput, CfnResource, RemovalPolicy } from 'aws-cdk-lib';
import { CfnCrawler, CfnTable } from 'aws-cdk-lib/aws-glue';
import { Effect, ManagedPolicy, PolicyDocument, PolicyStatement, Role, ServicePrincipal } from 'aws-cdk-lib/aws-iam';
import { Stream } from 'aws-cdk-lib/aws-kinesis';
import { CfnDeliveryStream } from 'aws-cdk-lib/aws-kinesisfirehose';
import { Key } from 'aws-cdk-lib/aws-kms';
import { RetentionDays, LogGroup, LogStream } from 'aws-cdk-lib/aws-logs';
import { Bucket } from 'aws-cdk-lib/aws-s3';
import { Construct } from 'constructs';

export interface DeliveryPipelineProps {
  readonly deliveryStreamName: string;
  readonly kmsKey: Key;
  readonly bucket: Bucket;
  readonly database: Database;
  readonly baseTableName: string;
  readonly processedColumns: Array<CfnTable.ColumnProperty>;
  readonly processedPartitions?: Array<CfnTable.ColumnProperty>;
  readonly rawColumns: Array<CfnTable.ColumnProperty>;
  readonly rawPartitions?: Array<CfnTable.ColumnProperty>;
  readonly timestampColumn: string;
  readonly stream?: Stream;
}

export class S3DeliveryPipeline extends Construct {
  public readonly deliveryStream: CfnDeliveryStream;
  public readonly processedTable: CfnTable;
  public readonly rawTable: CfnTable;
  public readonly firehoseLogGroup: LogGroup;

  constructor(scope: Construct, id: string, props: DeliveryPipelineProps) {
    super(scope, id);

    const processedCols = props.processedColumns
      .map((col) => col.name)
      .join(',')
      .toString();
    const processedPartionKeys = props.processedPartitions ? props.processedPartitions : [];

    const processedTableName = `p_${props.baseTableName}`;

    this.processedTable = new CfnTable(this, 'p_' + props.baseTableName, {
      databaseName: props.database.databaseName,
      catalogId: Aws.ACCOUNT_ID,
      tableInput: {
        description: `processed ${props.baseTableName}`,
        name: processedTableName,
        parameters: {
          has_encrypted_data: false,
          classification: 'parquet',
          typeOfData: 'file',
        },
        partitionKeys: processedPartionKeys,
        storageDescriptor: {
          columns: props.processedColumns,
          compressed: false,
          inputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
          location: `s3://${props.bucket.bucketName}/${props.baseTableName}/processed/`,
          outputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
          serdeInfo: {
            parameters: {
              paths: processedCols,
            },
            serializationLibrary: 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
          },
          storedAsSubDirectories: false,
        },
        tableType: 'EXTERNAL_TABLE',
      },
    });

    const rawCols = props.rawColumns
      .map((col) => col.name)
      .join(',')
      .toString();
    const rawPartionKeys = props.rawPartitions ? props.rawPartitions : [];
    const rawTableName = `r_${props.baseTableName}`;

    this.rawTable = new CfnTable(this, 'r_' + props.baseTableName, {
      databaseName: props.database.databaseName,
      catalogId: Aws.ACCOUNT_ID,
      tableInput: {
        description: `raw ${props.baseTableName}`,
        name: rawTableName,
        parameters: {
          has_encrypted_data: false,
          classification: 'json',
          typeOfData: 'file',
        },
        partitionKeys: rawPartionKeys,
        storageDescriptor: {
          columns: props.rawColumns,
          compressed: false,
          inputFormat: 'org.apache.hadoop.mapred.TextInputFormat',
          location: `s3://${props.bucket.bucketName}/${props.baseTableName}/raw/`,
          outputFormat: 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
          serdeInfo: {
            parameters: {
              paths: rawCols,
            },
            serializationLibrary: 'org.openx.data.jsonserde.JsonSerDe',
          },
          storedAsSubDirectories: false,
        },
        tableType: 'EXTERNAL_TABLE',
      },
    });

    const crawlerRole = new Role(this, 'GluePartitionCrawlerRole', {
      assumedBy: new ServicePrincipal('glue.amazonaws.com'),
      managedPolicies: [ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole')],
    });
    props.bucket.grantRead(crawlerRole);
    // props.kmsKey.grantEncryptDecrypt(crawlerRole);

    crawlerRole.addToPolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: ['logs:CreateLogGroup', 'logs:CreateLogStream', 'logs:PutLogEvents', 'logs:AssociateKmsKey'],
        resources: ['arn:aws:logs:*:*:/aws-glue/*'],
      }),
    );

    const securityConfName = `${props.baseTableName}-security`;
    const glueSecurityOptions = new SecurityConfiguration(this, 'EventsPipelineGlueSecurityConf', {
      securityConfigurationName: securityConfName,
      s3Encryption: {
        mode: S3EncryptionMode.KMS,
        kmsKey: props.kmsKey,
      },
      cloudWatchEncryption: {
        mode: CloudWatchEncryptionMode.KMS,
        kmsKey: props.kmsKey,
      },
    });

    glueSecurityOptions.cloudWatchEncryptionKey?.addToResourcePolicy(
      new PolicyStatement({
        principals: [new ServicePrincipal('logs.amazonaws.com')],
        actions: ['kms:Encrypt*', 'kms:Decrypt*', 'kms:ReEncrypt*', 'kms:GenerateDataKey*', 'kms:Describe*'],
        resources: ['*'],
      }),
    );

    const crawler = new CfnCrawler(this, 'PartitionUpdateCralwer', {
      role: crawlerRole.roleName,
      targets: {
        catalogTargets: [
          {
            databaseName: props.database.databaseName,
            tables: [processedTableName],
          },
          {
            databaseName: props.database.databaseName,
            tables: [rawTableName],
          },
        ],
      },
      configuration: '{"Version":1.0,"CrawlerOutput":{"Partitions":{"AddOrUpdateBehavior":"InheritFromTable"}},"Grouping":{"TableGroupingPolicy":"CombineCompatibleSchemas"}}',
      databaseName: props.database.databaseName,
      name: 'EventsUpdatePartitionCrawler',
      description: 'Crawler to update partitions of raw and processed tables every 5 minutes',
      recrawlPolicy: {
        recrawlBehavior: 'CRAWL_EVERYTHING',
      },
      schedule: {
        scheduleExpression: 'cron(0/5 * * * ? *)',
      },
      schemaChangePolicy: {
        deleteBehavior: 'LOG',
        updateBehavior: 'UPDATE_IN_DATABASE',
      },
      crawlerSecurityConfiguration: glueSecurityOptions.securityConfigurationName,
    });
    crawler.addDependency(this.processedTable);
    crawler.addDependency(this.rawTable);

    this.firehoseLogGroup = new LogGroup(this, 'FirehoseLogGroup', {
      retention: RetentionDays.ONE_WEEK,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    const firehoseLogStream = new LogStream(this, 'FirehoseLogStream', {
      logGroup: this.firehoseLogGroup,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    // had to hack the inline kinesis:DescribeStream as creation of resources was out of order.
    const firehoseKinesisReaderRole = new Role(this, 'FirehoseReader', {
      assumedBy: new ServicePrincipal('firehose.amazonaws.com'),
      description: 'Role for Firehose to read from Kinesis Stream  on storage layer',
      inlinePolicies: {
        'allow-s3-kinesis-logs': new PolicyDocument({
          statements: [
            new PolicyStatement({
              effect: Effect.ALLOW,
              actions: [
                'kinesis:DescribeStream',
                'kinesis:DescribeStreamSummary',
                'kinesis:GetRecords',
                'kinesis:GetShardIterator',
                'kinesis:ListShards',
                'kinesis:SubscribeToShard',
              ],
              resources: [props.stream!.streamArn],
            }),
          ],
        }),
      },
    });

    const firehoseS3WriterRole = new Role(this, 'FirehoseWriter', {
      assumedBy: new ServicePrincipal('firehose.amazonaws.com'),
      description: 'Role for Firehose to write to S3 storage layer',
      inlinePolicies: {
        GlueTableAccess: new PolicyDocument({
          statements: [
            new PolicyStatement({
              actions: ['glue:GetTableVersions', 'glue:GetTable', 'glue:GetTableVersion'],
              resources: [
                props.database.catalogArn,
                props.database.databaseArn,
                `arn:aws:glue:${Aws.REGION}:${Aws.ACCOUNT_ID}:table/${props.database.databaseName}/${this.processedTable.ref}`,
                `arn:aws:glue:${Aws.REGION}:${Aws.ACCOUNT_ID}:table/${props.database.databaseName}/${this.rawTable.ref}`,
              ],
            }),
          ],
        }),
      },
    });
    props.bucket.grantWrite(firehoseS3WriterRole);
    // props.kmsKey.grantEncrypt(firehoseS3WriterRole);

    firehoseS3WriterRole.node.addDependency(this.processedTable);
    firehoseS3WriterRole.node.addDependency(this.rawTable);


    this.deliveryStream = new CfnDeliveryStream(this, 'EventsDeliveryStream', {
      deliveryStreamName: props.deliveryStreamName,
      deliveryStreamType: props.stream ? 'KinesisStreamAsSource' : 'DirectPut',
      kinesisStreamSourceConfiguration: props.stream
        ? {
          kinesisStreamArn: props.stream.streamArn,
          roleArn: firehoseKinesisReaderRole.roleArn,
        }
        : undefined,
      deliveryStreamEncryptionConfigurationInput: props.stream
        ? undefined
        : {
          keyType: 'CUSTOMER_MANAGED_CMK',
          keyArn: props.kmsKey.keyArn,
        },
      extendedS3DestinationConfiguration: {
        bucketArn: props.bucket.bucketArn,
        roleArn: firehoseS3WriterRole.roleArn,
        encryptionConfiguration:
          props.stream === undefined && props.kmsKey !== undefined
            ? {
              kmsEncryptionConfig: {
                awskmsKeyArn: props.kmsKey.keyArn,
              },
            }
            : undefined,
        bufferingHints: {
          intervalInSeconds: 60,
          sizeInMBs: 128,
        },
        compressionFormat: 'UNCOMPRESSED',
        cloudWatchLoggingOptions: {
          enabled: true,
          logGroupName: this.firehoseLogGroup.logGroupName,
          logStreamName: firehoseLogStream.logStreamName,
        },
        prefix: `${props.baseTableName}/processed/year=!{partitionKeyFromQuery:year}/month=!{partitionKeyFromQuery:month}/day=!{partitionKeyFromQuery:day}/`,
        errorOutputPrefix: 'error/',
        s3BackupMode: 'Enabled',
        s3BackupConfiguration: {
          roleArn: firehoseS3WriterRole.roleArn,
          bucketArn: props.bucket.bucketArn,
          errorOutputPrefix: `${props.baseTableName}/failed/!{firehose:error-output-type}/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/`,
          prefix: `${props.baseTableName}/raw/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/`, // this is lame cause s3 backup doesn't support dynamic partitioning
          bufferingHints: {
            sizeInMBs: 128,
            intervalInSeconds: 60,
          },
          compressionFormat: 'UNCOMPRESSED',
          encryptionConfiguration: {
            noEncryptionConfig: 'NoEncryption',
          },
          cloudWatchLoggingOptions: {
            enabled: true,
            logGroupName: this.firehoseLogGroup.logGroupName,
            logStreamName: firehoseLogStream.logStreamName,
          },
        },
        dataFormatConversionConfiguration: {
          schemaConfiguration: {
            roleArn: firehoseS3WriterRole.roleArn,
            catalogId: Aws.ACCOUNT_ID,
            databaseName: props.database.databaseName,
            tableName: this.processedTable.ref,
            region: Aws.REGION,
            versionId: 'LATEST',
          },
          inputFormatConfiguration: {
            deserializer: {
              openXJsonSerDe: {},
            },
          },
          outputFormatConfiguration: {
            serializer: {
              parquetSerDe: {},
            },
          },
          enabled: true,
        },
        dynamicPartitioningConfiguration: {
          enabled: true,
          retryOptions: {
            durationInSeconds: 10,
          },
        },
        processingConfiguration: {
          enabled: true,
          processors: [
            {
              type: 'AppendDelimiterToRecord',
              parameters: [],
            },
            {
              type: 'MetadataExtraction',
              parameters: [
                {
                  parameterName: 'MetadataExtractionQuery',
                  parameterValue: `{year:.${props.timestampColumn} | strftime(\"%Y\"),month:.${props.timestampColumn} | strftime(\"%m\"),day:.${props.timestampColumn} | strftime(\"%d\")}`,
                },
                {
                  parameterName: 'JsonParsingEngine',
                  parameterValue: 'JQ-1.6',
                },
              ],
            },
          ],
        },
      },
    });

    this.deliveryStream.node.addDependency(firehoseKinesisReaderRole.node.defaultChild as CfnResource);
    this.deliveryStream.node.addDependency(firehoseS3WriterRole.node.defaultChild as CfnResource);

    if (props.stream) {
      props.stream.grantRead(firehoseKinesisReaderRole);
      props.stream.encryptionKey?.grantDecrypt(firehoseKinesisReaderRole);
      // Work around this: https://github.com/aws/aws-cdk/issues/10783
      const grant = props.stream.grant(firehoseKinesisReaderRole, 'kinesis:DescribeStream');
      grant.applyBefore(this.deliveryStream);
    }

    this.firehoseLogGroup.grantWrite(firehoseS3WriterRole);

    new CfnOutput(this, 'CloudwatchLogsInsights', {
      value: `https://console.aws.amazon.com/cloudwatch/home#logs-insights:queryDetail=~(end~0~source~'${this.firehoseLogGroup.logGroupName}~start~-3600~timeType~'RELATIVE~unit~'seconds)`,
    });
  }
}
