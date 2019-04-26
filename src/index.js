const stream = require('stream');
const _aggregate = require('aws-kinesis-agg').aggregate;

 
function aggregate(records){
	return new Promise((resolve,reject)=>{
		_aggregate(records, resolve,reject,reject);
	})
}
const AWS = require('aws-sdk');
const util = require('util');
AWS.config.update({
		  region: 'eu-west-1',
		  signatureVersion: 'v4',
});
const kinesis = new AWS.Kinesis();
const crypto = require('crypto')
const listShards = util.promisify(kinesis.listShards).bind(kinesis);
const bb = require('bigint-buffer');
async function getShardsInfo(StreamName){
	return listShards({StreamName});
}
function detirmineUserRecordShard(ShrardsInfo, record){
	if(!record || (!record.PartitionKey && ! record.ExplicitHashKey)){
		throw new Error("Record doesn't have a partitionKey or a explicitHashKey");
	}
	let bigInteger;
	if(record.ExplicitHashKey){
		bigInteger = record.ExplicitHashKey;
	}
	else{
		bigInteger =bb.toBigIntBE(crypto.createHash('md5').update(record.PartitionKey).digest("buffer"),32);
	}
	for(const shard  of ShrardsInfo.Shards.filter(e=>e.SequenceNumberRange.EndingSequenceNumber===undefined)){
	const startingHashKey=BigInt(shard.HashKeyRange.StartingHashKey);
	const endingHashKey=BigInt(shard.HashKeyRange.EndingHashKey);
	if(bigInteger>=startingHashKey && bigInteger<=endingHashKey){
		return shard.ShardId;
	}
	
}
}

class KinesisStream extends stream.Writable{
	constructor({client,streamName}){
		super({objectMode:true});

		if(!client){
			throw new Error("Valid Kinesis client shoul be provided");
		}
		if(!streamName){
			throw new Error("Please specify a valid kinesis stream Name");
		}
		this.client=client;
		this.streamName=streamName;
		this.initialized=false;
		this._initialize();
		this.initializationBuffer=[];
	}
	async _initialize(){
		this.shardsInfo=await getShardsInfo(this.streamName);
		this.shardStreams={};
		for(const shard of this.shardsInfo.Shards){
			this.shardStreams[shard.ShardId]=new ShardStream({client:this.client,shardId:shard.ShardId,bufferDuration:1000});
		}
		this.initializationBuffer.map(e=>this.handleChunck(e));
		this.initialized=true;

	}
	handleChunck(chunck){
		const shard=detirmineUserRecordShard(this.shardsInfo,chunck);
		if(this.shardStreams[shard]){
			this.shardStreams[shard].write(chunck);
		}
	}
	_write(chunk,encoding,cb){
		if(!this.initialized){
			this.initializationBuffer.push(chunk);
			return cb();
		}
		this.handleChunck(chunck);
		cb();
	}
}

class ShardStream extends stream.Writable{
		constructor({aggregationLimit,client, bufferDuration, shardId}){
			super({objectMode:true});
			this.shardId=shardId;
			this.client=client;
			this.bufferDuration=bufferDuration
			this.internalBuffer=[];
			this.aggregationLimit=aggregationLimit || 200;
			this.limitDuration=500;
			this.triggerer=null;
			this.procesing=false;
		}
		scheduleTriggerer(){
			if(this.triggerer){
					this.stopTriggerer();
			}
			this.triggerer=setTimeout(this.putRecordsToKinesis.bind(this),this.limitDuration);
		}
		stopTriggerer(){
							if(this.triggerer){
					clearTimeout(this.triggerer)
					this.triggerer=null;
		}
	}

		async putRecordsToKinesis(){
			this.processing=true;
			this.stopTriggerer();
			const totalData=this.internalBuffer.slice(0,this.aggregationLimit*500);
			const records=[];
			for(let i=0;i<totalData.length;i+=this.aggregationLimit){
				const readyToAggregateRecords=totalData.slice(i,i+this.aggregationLimit).map(e=>{
				e.data=e.Data;
				delete e.Data;
				e.partitionKey=e.PartitionKey;
				delete e.PartitionKey;
				e.explicitHashKey=e.ExplicitHashKey;
				delete e.ExplicitHashKey;
				return e;
			})
		 	records.push(await aggregate(readyToAggregateRecords));
			}

			console.log(this.shardId,records);
		}
		_write(chunck,encoding,cb){
		if(!this.triggerer && !this.processing){
			this.scheduleTriggerer();
		}
		this.internalBuffer.push(chunck);
		cb();
		}



	}
const producer= new KinesisStream({aggregationLimit:200,maxBufferDuration:150, client:kinesis, streamName:'test-stream'});

for(let i=0;i<1000;i++){

	producer.write({PartitionKey:"1"+i,Data:"hey"});
	producer.write({PartitionKey:"1"+i+"2",Data:"hey"});
  producer.write({PartitionKey:"1"+i+"2"+"3",Data:"hey"});
}