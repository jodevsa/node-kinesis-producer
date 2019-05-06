const MAX_RETRIES=10
const MIN_WAIT_TIME=150;
const MULTIPLY_FACTOR=2;
const util = require('util');
const sleep=function(ms){
	return new Promise((resolve)=>setInterval(resolve,ms));
}

async function putRecords({client,recordsParams}){
	const putRecordsAsync = util.promisify(client.putRecords).bind(client);
	const listShards = util.promisify(client.listShards).bind(client);
	let waitTime=MIN_WAIT_TIME;
	for(let i=0;i<MAX_RETRIES;i++){
		if(i!=0){
			await sleep(waitTime);
			waitTime*=MULTIPLY_FACTOR;
		}
		const result = await putRecordsAsync(recordsParams);
  	if (result.FailedRecordCount != 0) {
  		console.log(result)
		 continue;
		}
		return result;
	}
	throw new Error('could not put into kinesis');
}


module.exports=putRecords;