function getPotentialIndex(lookup, key, count) {
	const it = lookup[key]
	return (it) ? it : count
}

/*   
  0001 0000 10000
*/




 	function calculateVarIntSize(value) {
		if (value < 0) {
			throw new Error("Size values should not be negative.");
		} else if (value == 0) {
			return 1;
		}

		let numBitsNeeded = 0;

		// shift the value right one bit at a time until
		// there are no more '1' bits left...this should count
		// how many bits we need to represent the number
		while (value > 0) {
			numBitsNeeded++;

			console.log('value',value,'number of bits needed',numBitsNeeded);
			value = value >> 1;
		}

		// varints only use 7 bits of the byte for the actual value
		let numVarintBytes = Math.trunc(numBitsNeeded / 7);
		if (numBitsNeeded % 7 > 0) {
			console.log('hey.')
			numVarintBytes += 1;
		}

		return numVarintBytes;
	}

console.log(calculateVarIntSize(128));

function calculateRecordSize(self, record) {
	let messageSize = 0;

	// calculate the total new message size when aggregated into protobuf
	if (!self.partitionKeyTable.hasOwnProperty(record.partitionKey)) {
		// add the size of the partition key when encoded
		const pkLength = record.partitionKey.length;
		messageSize += 1; // (message index + wire type for PK table)
		messageSize += calculateVarIntSize(pkLength); // size of pk lengthvalue
		messageSize += pkLength; // actual pk length
	}

	if (record.explicitHashKey && !self.explicitHashKeyTable.hasOwnProperty(record.explicitHashKey)) {
		// add the size of the explicit hash key when encoded
		const ehkLength = record.explicitHashKey.length;
		messageSize += 1; // (message index + wire type for EHK table)
		messageSize += calculateVarIntSize(ehkLength); /* size of ehk length value */
		messageSize += ehkLength; // actual ehk length
	}

	/* compute the data record length */

	// add the sizes of the partition and hash key indexes
	let innerRecordSize = 1;
	innerRecordSize += calculateVarIntSize(getPotentialIndex(self.partitionKeyTable, record.partitionKey, self.partitionKeyCount));
	// explicit hash key field (this is optional)
	if (record.explicitHashKey) {
		innerRecordSize += 1;
		innerRecordSize += calculateVarIntSize(getPotentialIndex(self.explicitHashKeyTable, record.explicitHashKey, self.explicitHashKeyCount));
	}

	if (typeof (record.data) === 'string') {
		record.data = Buffer.from(record.data); // default utf8
	}
	var dataLength = Buffer.byteLength(record.data, 'binary');

	// message index + wire type for record data
	innerRecordSize += 1;
	// size of data length value
	innerRecordSize += calculateVarIntSize(dataLength);
	// actual data length
	innerRecordSize += dataLength;

	// data field
	messageSize += 1; // message index + wire type for record
	messageSize += calculateVarIntSize(innerRecordSize); // size of entire record length value

	messageSize += innerRecordSize; // actual entire record length
	return messageSize
}
