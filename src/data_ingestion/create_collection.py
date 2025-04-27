import weaviate
import weaviate.classes.config as wc

client = weaviate.connect_to_local()

emails = client.collections.create(
    name="Emails",
    properties=[
        wc.Property(name='email_id', data_type=wc.DataType.TEXT, skip_vectorization=True),
        wc.Property(name='thread_id', data_type=wc.DataType.TEXT, skip_vectorization=True),
        wc.Property(name='sender_name', data_type=wc.DataType.TEXT),
        wc.Property(name='sender_email', data_type=wc.DataType.TEXT),
        wc.Property(name='receiver_name', data_type=wc.DataType.TEXT),
        wc.Property(name='receiver_email', data_type=wc.DataType.TEXT),
        wc.Property(name='subject', data_type=wc.DataType.TEXT),
        wc.Property(name='body', data_type=wc.DataType.TEXT)
    ],
    vectorizer_config=wc.Configure.Vectorizer.text2vec_ollama(
        api_endpoint="http://host.docker.internal:11434",
        model='nomic-embed-text'
    ),
    generative_config=wc.Configure.Generative.ollama(
        api_endpoint="http://host.docker.internal:11434",
        model='llama3.2'
    )
)

print(emails)

client.close()