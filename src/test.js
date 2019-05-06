const AWS = require('aws-sdk');
AWS.config.update({
		  region: 'eu-west-1',
		  signatureVersion: 'v4',
});

const kinesis = new AWS.Kinesis();
const FakeKinesisDataGeneratorStream = require('../Streams/FakeKinesisDataGeneratorStream.js');
const AggregationStream = require('../Streams/AggregateStream.js');
const KinesisWriteStream = require('../Streams/KinesisWriteStream.js');
 const KINESIS_STREAM_NAME='test-stream';
const  generator= new FakeKinesisDataGeneratorStream();
const aggregationStream = new AggregationStream({batchDuration:'1000 ms'});
const writeStream = new KinesisWriteStream({streamName:KINESIS_STREAM_NAME,client:kinesis,batchDuration:'1000 ms'})

generator.pipe(aggregationStream)
				 .pipe(writeStream);
					




