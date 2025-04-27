import weaviate
from weaviate.util import generate_uuid5
from email_api import auth, fetch_emails


def batch_import_data(client, gmail_creds, n_emails=100):
    # Get collection
    emails = client.collections.get("Emails")

    with emails.batch.dynamic() as batch:
        for email in fetch_emails(gmail_creds, n_emails):
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

    batch_import_data(client, gmail_creds)

    client.close()