from confluent_kafka import Producer
import json
import yaml
import time
import weaviate
from src.data_ingestion.email_api import auth, fetch_emails
from src.data_ingestion.import_data import get_last_email_date
from google.oauth2.credentials import Credentials
from pathlib import Path


def init_producer(config_path: str) -> Producer:
    # Load config
    with open(config_path, "r") as config_file:
        config = yaml.safe_load(config_file)
    producer_config = {**config['kafka'], **config['producer']}
    return Producer(producer_config)


def delivery_report(err, msg):
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] successfully!")


def send_batch_periodically(producer: Producer, gmail_creds: Credentials, emails_collection, polling_time: int=60) -> None:
    # Fetch a batch of emails each `polling_time` and send it to consumer. 
    while True:
        print("Checking for new emails...")
        email_id, from_date_time = get_last_email_date(emails_collection)
        for email in fetch_emails(gmail_creds, from_date_time):
            # Check if fetched email is the same as last added email
            # Gmail api query return emails with date >= from_date_time, so we need to filter the already added emails
            # This needs to be done by comparing ids, not dates, to avoid the problem of ignoring emails that arrived at the exact same time.
            if email_id == email['email_id']:
                continue
            # Convert datetime to iso format as it's not json serialiazable
            print(f'Sending to consumer -- email: id: {email['email_id']}, date: {email['date_time']}, subject: {email['subject']}')
            email['date_time'] = email['date_time'].isoformat()
            producer.produce(
                topic="emails_dev",
                value=json.dumps(email).encode("utf-8"),
                callback=delivery_report
            )

        producer.flush()
        time.sleep(polling_time)


if __name__ == "__main__":
    # Gmail api auth
    gmail_creds = auth()

    # Get email collection from db
    client = weaviate.connect_to_local()
    emails_collection = client.collections.get("Emails")

    config_path = Path(__file__).parent / "../kafka_config.yaml"
    producer = init_producer(config_path)

    # Run 
    send_batch_periodically(producer, gmail_creds, emails_collection, 30)

    client.close()