const debug= require('debug')('AggregationStream');
const RecordAggregator = require('aws-kinesis-agg-temp').RecordAggregator;
const stream = require('stream');
const process = require('process');
const x=JSON.stringify({"_id":{"_data":"825CB474B4000000012B022C0100296E5A1004471D0870442648AE964ED20220AE6F3646645F696400645CB474819EB6F318DC496C950004"},"clusterTime":{"$timestamp":{"i":1,"t":1555330228}},"documentKey":{"_id":"5cb474819eb6f318dc496c95"},"ns":{"coll":"task","db":"FieldRadar-STG"},"operationType":"update","updateDescription":{"removedFields":[],"updatedFields":{"assetId":null,"assignedUser":null,"description":"\\u003cp\\u003e3\\u003cbr/\\u003e\\u003c/p\\u003e","location":null,"organization":null,"roleId":null,"updatedAt":"2019-04-15T12:10:28.974Z"}}});
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

class FakeKinesisDataStream extends  stream.Readable{
	constructor(){
			super({objectMode:true});
			this.i=0;
			this.reading=false;

	}
	 async startReading(){
	 	this.reading=true;
	let noBackPressure=true;
		while(this.i<100000){
			if(this.i % 1000 ===0){

			await sleep(50);
			}
			this.i+=1;


			if(!this.push({data:x,partitionKey:"partk"+this.i})){
				break;
			}
		}
		this.reading=false;
	 }
	 async _read(){
	 	if(this.reading===true){
	 		return;
	 	}
	 	this.startReading();
	}

}

module.exports=FakeKinesisDataStream;