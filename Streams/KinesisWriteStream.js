const stream = require('stream');
const parseDurationString = require('parse-duration')
const RECORD_LIMIT=500;
const filesizeParser = require('filesize-parser');
const putRecords = require('../src/putRecords.js')
const debug= require('debug')('WriteStream');
const RateLimiter = require('../src/helpers/RateLimit.js');
class WritableStream extends stream.Writable{
	constructor({streamName, batchDuration, client}){
		super({objectMode:true})
		this.streamName=streamName;
		this.internalBuffer=[];
		this.rateLimiter= new RateLimiter({limitPerInterval:500,sizeLimitPerInterval:"1000 mb",interval:"1000 ms"});
		this.inflightRecords;
		this.totalSize=0;
		if(!client){
			throw new Error("Must have  a client");
		}
		this.client= client;
		this.batchDuration= typeof(batchDuration)==='string'?parseDurationString(batchDuration):batchDuration;
		
	}
	async _putRecords(cb,{record,totalBytes}){
		this._stopInternalClock();
		await this.rateLimiter.aquireInnovocations({count:this.internalBuffer.length,size:this.totalBytes},true);
		console.log('here',this.internalBuffer.length)

		const recordsParams = {
		      Records: this.internalBuffer,
		      StreamName: this.streamName
		    };
		const result= await putRecords({client:this.client,recordsParams})
		console.log('total size', this.totalSize)
		debug(`Successfully put ${this.internalBuffer.length}/${this.total} Kinesis Records`)
		this.internalBuffer=[];
		this.totalSize=0;
		if(record){
			console.log('34')
		}
		if(cb){
			cb();
		}
	}
	_startInternalClock(){
		this._startTime=Date.now();
		if(this._timeoutReference){
			this._stopInternalClock();
		}
		this._timeoutReference=setTimeout(this._putRecords.bind(this),this.batchDuration)
	}
	_stopInternalClock(){
		if(this._timeoutReference){
			clearTimeout(this._timeoutReference);
			this._timeoutReference=null;
		}
	}
	async _write({totalBytes,encodedData},encoding,cb){
		let record={};
		record.Data=encodedData.data;
		record.PartitionKey=encodedData.partitionKey;
		if(this.totalSize + totalBytes > filesizeParser('1 mb') && totalBytes!=0){
			return this._putRecords(cb,record);
		}
		this.internalBuffer.push(record);
		this.totalSize+=totalBytes;
		if(this.internalBuffer.length===RECORD_LIMIT){
			return this._putRecords(cb);
		}
		if(this.internalBuffer.length===1){
			this._startInternalClock();
		}
		cb();
	}
}


module.exports= WritableStream;