import json
import boto3
import sys
import sys

import boto3


def get_messages_from_queue(sqs_client, queue_url):
    """Generates messages from an SQS queue.

    Note: this continues to generate messages until the queue is empty.
    Every message on the queue will be deleted.

    :param queue_url: URL of the SQS queue to read.

    See https://alexwlchan.net/2018/01/downloading-sqs-queues/

    """
    while True:
        resp = sqs_client.receive_message(
            QueueUrl=queue_url, AttributeNames=["All"], MaxNumberOfMessages=10
        )

        try:
            yield from resp["Messages"]
        except KeyError:
            return

        entries = [
            {"Id": msg["MessageId"], "ReceiptHandle": msg["ReceiptHandle"]}
            for msg in resp["Messages"]
        ]

        resp = sqs_client.delete_message_batch(QueueUrl=queue_url, Entries=entries)

        if len(resp["Successful"]) != len(entries):
            raise RuntimeError(
                f"Failed to delete messages: entries={entries!r} resp={resp!r}"
            )


def lambda_handler(event, context):
    
    src_queue_url = event["SRC_QUEUE_URL"]
    dst_queue_url = event["DEST_QUEUE_URL"]

    if src_queue_url == dst_queue_url:
        sys.exit("Source and destination queues cannot be the same.")

    sqs_client = boto3.client("sqs")

    for message in get_messages_from_queue(sqs_client, queue_url=src_queue_url):
        sqs_client.send_message(QueueUrl=dst_queue_url, MessageBody=message["Body"])
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
