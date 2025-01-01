# Kafka producer load balancer 

A custom producer implementation in go, which will keep lag in balance for a consumer group.

Solves a sepecific case where multiple consumers are processing msgs from a topic at their own pace, overtime 
if kafka lag is not balanced among them, its best use case to use.

## Example - 

There are 10m tasks to be done every-day, we have a task scheduler which pushes tasks to a kafka topic 
which has 10 partitions, and we have 10 EC2 instances to process.

so   
 - 1m tasks per partition -> per EC2 instance per day  
 - 10m tasks for a topic -> 10 EC2 instances per day   

each task will take sometime to do. Overtime we see a pattern that some partitions have unfinished tasks after a day
meaning some EC2 machines are not able to complete 1m tasks and some of them are able to finish in less than 24h
it can be because of nature of tasks or other factors.  

Next day - if task-scheduler pushes 1m to each partition, the lag will keep growing for some instances.


## Solution - 
load balancing each time we publish msg to kafka.

## GAP - Group Aware Producer (as I call it)


How ? 

 - Monitor -> a kafka admin client which gets lag per partition from kafka when called.
 - Cache -> keeps lag per partition in memory, refresh using monitor after TTL in a separate go-routine call. Sort the lag in increasing order.
 - Producer -> for a batch of msgs, calls cache to get min lag partition for all the msgs and sends batch to kafka.

