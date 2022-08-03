"""SNS helper functions."""

import json
import logging
from functools import lru_cache

import boto3

from toshi_hazard_post.local_config import IS_OFFLINE  # , IS_TESTING

log = logging.getLogger(__name__)


def get_sns_client():
    """Get an SNS client."""
    AWS_REGION = 'ap-southeast-2'
    if IS_OFFLINE:  # and not IS_TESTING:
        log.debug("**OFFLINE SNS SETUP**")
        return boto3.client('sns', endpoint_url="http://127.0.0.1:4002", region_name=AWS_REGION)
    else:
        return boto3.client('sns', region_name=AWS_REGION)


@lru_cache(maxsize=2)
def get_sns_topic_arn(topic_name):
    """Get the ARN for the given topic_name."""
    log.debug(f"get_sns_topic_arn for {topic_name}")

    conn = get_sns_client()
    response = conn.list_topics()

    topic_arn = None
    for topic in response.get('Topics'):
        if topic_name in topic['TopicArn']:
            return topic['TopicArn']

    # need to create the topic
    conn.create_topic(Name=topic_name)
    response = conn.list_topics()
    topic_arn = response["Topics"][0]["TopicArn"]
    return topic_arn


def publish_message(message, topic):
    """Publish AWS SNS message."""
    log.debug(f"publish_message {message}")
    client = get_sns_client()
    topic_arn = get_sns_topic_arn(topic)
    log.debug(f'TOPIC ARN {topic_arn}')
    try:
        response = client.publish(
            TargetArn=topic_arn, Message=json.dumps({'default': json.dumps(message)}), MessageStructure='json'
        )
        log.debug(f"SNS reponse {response}")
    except Exception as err:
        log.error(err)
        raise
    log.info("publish_message OK")
