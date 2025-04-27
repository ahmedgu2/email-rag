import weaviate
import weaviate.classes.query as wq


def sementic_query(client, search_text: str) -> None:
    emails = client.collections.get('Emails')

    response = emails.query.near_text(
        query=search_text,
        limit=10
    )

    for email in response.objects:
        print(f"Subject: {email.properties['subject']}")

if __name__ == "__main__":
    client = weaviate.connect_to_local()

    sementic_query(client, "Newsletters")

    client.close()