in progress..
# Node Kinesis Producer

A library that provides a port of AWS Kinesis Producer (KPL) functionality

# Features
* Aggregates records only based on ExplicitKey or PartitionKey
* Exponential backoff strategy, no more handeling of ProvisionedThroughputExceededException
* Auto detects  stream merge/split and recalculates the shard strategy
## Diagram
![Image description](https://i.imgur.com/Qzo19LY.png)