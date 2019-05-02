const MAX_RETRIES=10
const MIN_WAIT_TIME=150;
const MULTIPLY_FACTOR=2;
const util = require('util');
const AWS = require('aws-sdk');
AWS.config.update({
		  region: 'eu-west-1',
		  signatureVersion: 'v4',
});
const sleep=function(ms){
	return new Promise((resolve)=>setInterval(resolve,ms));
}
const kinesis = new AWS.Kinesis();
const putRecordsAsync = util.promisify(kinesis.putRecords).bind(kinesis);
const listShards = util.promisify(kinesis.listShards).bind(kinesis);
async function putRecords(recordsParams){
	let waitTime=MIN_WAIT_TIME;
	for(let i=0;i<MAX_RETRIES;i++){
		if(i!=0){
			await sleep(waitTime);
			waitTime*=MULTIPLY_FACTOR;
		}
		const result = await putRecordsAsync(recordsParams);
  	if (result.FailedRecordCount != 0) {
		 continue;
		}
		return result;
	}
	throw new Error('could not put into kinesis');
}


module.exports=putRecords;