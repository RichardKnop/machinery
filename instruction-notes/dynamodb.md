# Using DynamoDB as a result backend
## What is DynamoDB
Amazon DynamoDB is a fast and flexible NoSQL database service.
Check this [official website](https://aws.amazon.com/dynamodb/
) for details.

## How to use DynamoDB as a result backend in Machinery
### Create two tables first
There will be two tables required now(2018-01-12):
* group_metas: A table which saves group tasks' meta data. The primary key for this table is ```GroupUUID```, and it should be set properly when creating this table.
* task_states: A table which saves every task's states. The primary key for this table is ```TaskUUID```, and it should be set properly when creating this table.


### Add DynamoDB config to the config file
#### example config
```yaml
broker: 'https://sqs.us-west-1.amazonaws.com/123456789012'
default_queue: machinery-queue
result_backend: 'https://dynamodb.us-west-1.amazonaws.com/123456789012'
results_expire_in: 3600
dynamodb:
  task_states_table: 'task_states'
  group_metas_table: 'group_metas'
```
Then DynamoDB will be used as a result backend.