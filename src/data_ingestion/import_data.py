import weaviate
from weaviate.util import generate_uuid5
from src.data_ingestion.email_api import auth, fetch_emails
from weaviate.classes.query import Filter, Sort
from datetime import datetime, timezone
from google.oauth2.credentials import Credentials


def get_last_email_date(emails_collection) -> datetime:
    result = emails_collection.query.fetch_objects(
        sort=Sort.by_property("date_time", ascending=False),
        limit=1
    )
    if len(result.objects) == 0: # Empty database
        return None, datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc) # First date of 2025
    return result.objects[0].properties['email_id'], result.objects[0].properties['date_time'] 


def batch_import_data(client, gmail_creds: Credentials):
    # Get collection
    emails = client.collections.get("Emails")

    email_id, from_date_time = get_last_email_date(emails)
    with emails.batch.dynamic() as batch:
        for email in fetch_emails(gmail_creds, from_date_time):
            # Check if fetched email is the same as last added email
            # Gmail api query return emails with date >= from_date_time, so we need to filter the already added emails
            # This needs to be done by comparing ids, not dates, to avoid the problem of ignoring emails that arrived at the exact same time.
            if email_id == email['email_id']:
                continue
            
            print(f'Adding email: id: {email['email_id']}, date: {email['date_time']}, subject: {email['subject']}')
            batch.add_object(
                properties=email,
                uuid=generate_uuid5(email['email_id'])
            )

    # Check for failed objects
    if len(emails.batch.failed_objects) > 0:
        print(f"Failed to import {len(emails.batch.failed_objects)} objects.")


if __name__ == "__main__":
    # Gmail api auth
    gmail_creds = auth()

    client = weaviate.connect_to_local()
    # Parse the datetime string
    batch_import_data(client, gmail_creds)

    client.close()