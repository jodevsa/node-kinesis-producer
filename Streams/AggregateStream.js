const debug= require('debug')('AggregationStream');
const RecordAggregator = require('aws-kinesis-agg-temp').RecordAggregator;
const stream = require('stream');
const process = require('process');
const parseDurationString = require('parse-duration')
const sleep = function(ms){
	return new Promise((resolve,reject)=>{
	setTimeout(()=>{
		resolve()
	},ms)
	})
}

const nextTick = function(ms){
	return new Promise((resolve,reject)=>{
	process.nextTick(()=>{
		resolve()
	})
	})
}
class AggregationStream extends  stream.Transform{
	constructor({batchDuration}){
		super({objectMode:true});
		this._timeoutReference=null;
		this.readEnabled=false;
		this.batchDuration=typeof(batchDuration)==='string'?parseDurationString(batchDuration):batchDuration
		this.recordAggregator= new RecordAggregator();
		this.recv=0;
		this.sent=0;
		this.totalReocrdsSent=0;
	}
	async _build(record,cb){
		this._stopInternalClock();
		debug(`Batched ${this.recordAggregator.length()}`)
		this.totalReocrdsSent+=this.recordAggregator.length()
		const totalBytes=this.recordAggregator.totalBytes;
		const encodedData=this.recordAggregator.build();
		this.push({totalBytes,encodedData});
		this.sent+=1;
		debug('Sent ',this.sent,'Batches', this.totalReocrdsSent,'User Records');
		if(record){
			this._addRecord(record);
		}
		if(cb){

			cb();
		}
	}
	_addRecord(record){
		this.recordAggregator.addUserRecord(record);
		if(this.recordAggregator.length()===1){
			this._startInternalClock();
		}
	}
	_startInternalClock(){
		if(this._timeoutReference){
			this._stopInternalClock();
		}
		this._timeoutReference=setTimeout(this._build.bind(this),this.batchDuration)
	}
	_stopInternalClock(){
		if(this._timeoutReference){
			clearTimeout(this._timeoutReference);
			this._timeoutReference=null;
		}
	}
	_transform(record,encoding,cb){
			this.recv+=1;
			if(!this.recordAggregator.checkIfUserRecordFits(record)){
				debug(`RecordAggregator is full, got ${this.recordAggregator.length()} in flight`)
				return this._build(record,cb);
			}
			this._addRecord(record);
			cb();
	}
}

module.exports=AggregationStream;
/*
const FakeKinesisDataStream= require('./FakeKinesisDataGeneratorStream.js')
const FakeWritableStream = require('./KinesisWriteStream.js');
	console.log(FakeKinesisDataStream);
(new FakeKinesisDataStream())
		.pipe(new AggregationStream({batchDuration:'200 ms'}))
		.pipe(new FakeWritableStream())


*/