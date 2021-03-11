import json
import boto3
import sys
import sys

def get_messages_from_queue(sqs_client, queue_url, max_message_count):
    """Generates messages from an SQS queue.

    Note: this continues to generate messages until the queue is empty.
    Every message on the queue will be deleted.

    :param queue_url: URL of the SQS queue to read.

    See https://alexwlchan.net/2018/01/downloading-sqs-queues/

    """
    processed_message_count = 0

    while processed_message_count < max_message_count:
        #print("Max Mesage Count: " + str(max_message_count))
        remaining_message_count = max_message_count - processed_message_count
        #print("Remaining messages: " + str(remaining_message_count))

        receive_message_count = min(10, remaining_message_count)
        get_resp = sqs_client.receive_message(
            QueueUrl=queue_url, AttributeNames=["All"], MaxNumberOfMessages=receive_message_count
        )

        #print("Actual response:")
        #print(get_resp)

        try:
            #print("Number of messages receieved: " + str(len(get_resp["Messages"])))
            yield from get_resp["Messages"]
        except KeyError:
            return

        entries = [
            {"Id": msg["MessageId"], "ReceiptHandle": msg["ReceiptHandle"]}
            for msg in get_resp["Messages"]
        ]

        resp = sqs_client.delete_message_batch(QueueUrl=queue_url, Entries=entries)

        if len(resp["Successful"]) != len(entries):
            raise RuntimeError(
                f"Failed to delete messages: entries={entries!r} resp={resp!r}"
            )
        
        processed_message_count += len(get_resp["Messages"])
        #print("After deleting, number of processed messages are: " + str(processed_message_count))


def lambda_handler(event, context):
    max_message_count = event['MSG_TRANSFER_LIMIT']
    src_queue_url = event["SRC_QUEUE_URL"]
    dst_queue_url = event["DEST_QUEUE_URL"]

    if src_queue_url == dst_queue_url:
        sys.exit("Source and destination queues cannot be the same.")

    sqs_client = boto3.client("sqs")

    # while processed_message_count < max_message_count:
        

    for message in get_messages_from_queue(sqs_client, src_queue_url, max_message_count):
        sqs_client.send_message(QueueUrl=dst_queue_url, MessageBody=message["Body"])

    return {
        'ProcessedMessageCount': max_message_count
    }
