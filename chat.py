import json
import threading
from datetime import datetime
from hashlib import md5
from typing import List

import click
from kafka import KafkaConsumer, KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic

BOOTSTRAP_SERVERS = "localhost:57232"


def parse_message(message) -> str:
    key = message.key.decode("utf-8")
    value = json.loads(message.value.decode("utf-8"))
    message_text = value["message"]
    message_timestamp = value["timestamp"]
    incoming_message = click.style(
        f"{key} ({message_timestamp}): {message_text}", fg="green"
    )
    return incoming_message


def read_messages(topic: str, message_history: List[str]) -> None:
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP_SERVERS,
    )

    for message in consumer:
        click.clear()
        message_history.append(parse_message(message))
        for _msg in message_history:
            click.echo(_msg)


def get_message_history(topic):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        consumer_timeout_ms=1000,  # Exit if no messages after 1 second
    )
    messages = []

    for message in consumer:
        parsed_message = parse_message(message)
        messages.append(parsed_message)

    return messages


def create_chat_room(current_username: str, target_username: str) -> str:
    # Sort the user pair alphabetically to ensure that the same topic is used for both users
    chat_topic_name = md5(
        f"{'-'.join([u for u in sorted([current_username, target_username])])}".encode()
    ).hexdigest()

    admin_client = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS, client_id="chat-app"
    )

    # Check if topic already exists first, if not, create it!
    existing_topics = admin_client.list_topics()
    if chat_topic_name not in existing_topics:
        admin_client.create_topics(
            [NewTopic(chat_topic_name, num_partitions=1, replication_factor=1)]
        )
    return chat_topic_name


def send_message(
    producer: KafkaProducer,
    message_text: str,
    chat_topic_name: str,
    current_username: str,
) -> None:
    payload = json.dumps(
        {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "message": message_text,
        }
    )
    producer.send(
        topic=chat_topic_name,
        key=current_username.encode("utf-8"),
        value=payload.encode("utf-8"),
    )


@click.command()
@click.option("--current-username", prompt="Your username")
@click.option(
    "--target-username", prompt="Target username", help="The person to chat with."
)
def chat(current_username: str, target_username: str) -> None:
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)

    chat_topic_name = create_chat_room(current_username, target_username)

    # Get message history
    message_history = get_message_history(chat_topic_name)

    # Start a background thread to read messages from the topic
    thread = threading.Thread(
        target=read_messages,
        args=(
            chat_topic_name,
            message_history,
        ),
    )
    thread.start()

    while True:
        message_input = click.prompt("")
        send_message(producer, message_input, chat_topic_name, current_username)


if __name__ == "__main__":
    click.clear()
    chat()
