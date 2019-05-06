const util = require('util');
const parseDurationString = require('parse-duration')
const filesizeParser = require('filesize-parser');
const setTimeoutAsync=  (ms)=>new Promise(resolve=>setTimeout(resolve,ms))
// implements the fixed window algorithim
class RateLimiter{
	_aquire({count,size}){
		this.currentIntervalCount-=count;
		this.currentIntervalBits-=typeof(size) ==='string'?
															filesizeParser(size):size;								
	}
	_reset(){
		this.currentIntervalTime = new Date().getTime();
		this.currentIntervalCount=this.limit;
		this.currentIntervalBits=this.sizeLimitPerInterval;
	}
	constructor({limitPerInterval,sizeLimitPerInterval,interval}){
		if(typeof(interval ) ==='string'){
			interval=parseDurationString(interval);
		}

		this.sizeLimitPerInterval=typeof(sizeLimitPerInterval) ==='string'?
															filesizeParser(sizeLimitPerInterval):sizeLimitPerInterval;									
		this.limit=limitPerInterval;
		this._reset();
		this.intervalInMS=interval;
	}
	 async aquireInnovocations({minSize, minCount, count, size}, wait, cb){
	 	size=typeof(size)==='string'?filesizeParser(size):size;
	 	minSize=typeof(minSize)==='string'?filesizeParser(minSize):minSize;
	 	let {sizeLimitPerInterval, limit, intervalInMS,currentIntervalTime,currentIntervalCount, currentIntervalBits} = this;
	 	if(Date.now() >= this.currentIntervalTime+ this.intervalInMS){
	 		this._reset();
	 		currentIntervalTime=this.currentIntervalTime;
	 		currentIntervalCount = this.currentIntervalCount;
	 		currentIntervalBits= this.currentIntervalBits;
	 	}
		
		if(count<0){
			throw new Error('trying t aquire a negative value');
		}
		if(size>sizeLimitPerInterval){
			throw new Error(`trying to aqquire size higher than the global limit ${size} bits in ${intervalInMS}`)
		}
		if(count>limit){
			throw new Error(`Count needed higher than the global limit ${limit} innovocation in ${intervalInMS} ms`);
		}

		if(count>currentIntervalCount  || size >currentIntervalBits){
			if(minSize && minCount && minCount<=currentIntervalCount && minSize<=currentIntervalBits){
				this._aquire({count:Math.min(count,currentIntervalCount),size:Math.min(size,currentIntervalBits)});
				if(cb){
				cb({count:Math.min(count,currentIntervalCount),size:Math.min(size,currentIntervalBits)});
			}
			return  {count:Math.min(count,currentIntervalCount),size:Math.min(size,currentIntervalBits)};

			}

			if(wait){
			await setTimeoutAsync(Math.ceil((Date.now()+intervalInMS)- currentIntervalTime));
			this._reset();
			this._aquire({count,size});
				return {count,size};
			}
			else{
				return false;
			}
		}
		else{
			this._aquire({count, size});
			if(cb){
				cb(count);
			}
			return {count,size};

		}
	}
}

module.exports=RateLimiter;;

