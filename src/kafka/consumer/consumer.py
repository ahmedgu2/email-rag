from confluent_kafka import Consumer
from pathlib import Path
import json 
import yaml


def init_consumer(config_path: str) -> Consumer:
    # Load config
    with open(config_path, "r") as config_file:
        config = yaml.safe_load(config_file)
    consumer_config = {**config['kafka'], **config['consumer']}

    consumer = Consumer(consumer_config)
    return consumer


def run_consumer(consumer: Consumer):
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None: # No new messages
                continue
            if message.error():
                print(f"Error: {message.error()}")
                continue
            # We got a new message
            print(f"Consumed message from {message.topic()}, key={message.key()}, message contains: {message.value().decode('utf-8')}")
    except KeyboardInterrupt:
        print("Exited!")
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    config_path = Path(__file__).parent / "../kafka_config.yaml"
    consumer = init_consumer(config_path)
    consumer.subscribe(["emails_dev"])
    run_consumer(consumer)