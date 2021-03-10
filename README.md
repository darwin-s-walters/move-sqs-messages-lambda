# move-sqs-messages-lambda

A lambda implementation of [alexwlchan's redrive_sqs_queue script](https://alexwlchan.net/2020/05/moving-messages-between-sqs-queues/). 


Sample event:

```
{
  "SRC_QUEUE_URL": "<src_queue_url>",
  "DEST_QUEUE_URL": "<dest_queue_url>"
}
```