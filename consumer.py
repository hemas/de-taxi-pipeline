import boto3
import subprocess

QUEUE_URL = "https://queue.amazonaws.com/629255175421/de-taxi-pipeline-events"
sqs = boto3.client("sqs", region_name="us-east-1")

print("Starting clean.py script...")

while True:
    resp = sqs.receive_message(
        QueueUrl=QUEUE_URL,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=20
    )

    messages = resp.get("Messages", [])
    if not messages:
        #print("No messages received. Exiting.")
        continue

    for msg in messages:
        body = msg["Body"]
        print(f"Received message: {body}")

        if body == "clean stage complete":
            print("Triggering aggregate.py script...")
            subprocess.run(["python3", "aggregate.py"])
            print("aggregate.py script completed.")

        #delete the message from the queue after processing
        sqs.delete_message(
            QueueUrl=QUEUE_URL,
            ReceiptHandle=msg["ReceiptHandle"]
        )
        print("Message deleted from SQS queue.")
