const stream = require('stream');
const parseDurationString = require('parse-duration')
const RECORD_LIMIT=500;
const filesizeParser = require('filesize-parser');
const putRecords = require('../src/putRecords.js')
const debug= require('debug')('WriteStream');
const RateLimiter = require('../src/helpers/RateLimit.js');

const KINESIS_ITEMS_PER_SECOND = 1000
const KINESIS_BYTES_PER_SECOND=filesizeParser('1 mb');
const KINESIS_INETRVAL_LENGTH = parseDurationString("1000 ms");
class WritableStream extends stream.Writable{
	constructor({streamName, batchDuration, client}){
		super({objectMode:true})
		this.streamName=streamName;
		this.internalBuffer=[];
		// these are kinesis limits, not mine!
		this.rateLimiter= new RateLimiter({limitPerInterval:KINESIS_ITEMS_PER_SECOND,
																			 sizeLimitPerInterval:KINESIS_BYTES_PER_SECOND,
																			 interval:KINESIS_INETRVAL_LENGTH});
		this.inflightRecords;
		this.continueOpen=()=>{};
		this.totalSize=0;
		if(!client){
			throw new Error("Must have  a client");
		}
		this.client= client;
		this.state='open';
		this.batchDuration= typeof(batchDuration)==='string'?parseDurationString(batchDuration):batchDuration;
		
	}
	async _putRecords(cb,record,totalBytes){
		if(this.state==='processing'){
			exit();
		}
		this.state='processing';
		console.log(this.state);
		this._stopInternalClock();
		console.log(Date.now(),this.internalBuffer.length,this.totalSize,record,totalBytes);
		await this.rateLimiter.aquireInnovocations({count:this.internalBuffer.length,size:this.totalSize},true);
		console.log(Date.now(),this.internalBuffer.length,this.totalSize);
		console.log('here',this.internalBuffer.length)

		const recordsParams = {
		      Records: this.internalBuffer,
		      StreamName: this.streamName
		    };
		const result= await putRecords({client:this.client,recordsParams})
		console.log('total size', this.totalSize)
		debug(`Successfully put ${this.internalBuffer.length} Kinesis Records`)
		this.internalBuffer=[];
		this.totalSize=0;
		if(record && totalBytes){
			this.internalBuffer=[record];
			this.totalSize=totalBytes;
			if(this.internalBuffer.length===1){
				this._startInternalClock();
			}
		}
	
	  	this.state='open';
		this.continueOpen();
		this.continueOpen=()=>{};
			if(cb){
			cb();
		}
	}
	_startInternalClock(){
		console.log('start clockS')
		this._startTime=Date.now();
		if(this._timeoutReference){
			this._stopInternalClock();
		}
		this._timeoutReference=setTimeout(this._putRecords.bind(this),this.batchDuration)
	}
	_stopInternalClock(){
		console.log('stop clock')
		if(this._timeoutReference){
			clearTimeout(this._timeoutReference);
			this._timeoutReference=null;
		}
	}
	async _write({totalBytes,encodedData},encoding,cb){
		let record={};
		record.Data=encodedData.data;
		record.PartitionKey=encodedData.partitionKey;
		if(this.state==='processing'){
			console.log('process.')
			this.continueOpen=function(){
				this.state='open';
				this.internalBuffer.push(record);
				this.totalSize+=totalBytes;
				console.log(this.internalBuffer.length,'84')
				if(this.internalBuffer.length===1){
					this._startInternalClock();
				}
				this.state='open';
				console.log('here......')
				cb();

			}
			return ;
		}
		if(this.internalBuffer.length+1 === KINESIS_ITEMS_PER_SECOND || this.totalSize + totalBytes > KINESIS_BYTES_PER_SECOND && totalBytes!=0){
			return this._putRecords(cb,record,totalBytes);
		}
		this.internalBuffer.push(record);
		this.totalSize+=totalBytes;
		if(this.internalBuffer.length===1){
			this._startInternalClock();
		}
		cb();
	}
}


module.exports= WritableStream;