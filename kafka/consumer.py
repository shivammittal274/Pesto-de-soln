import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), os.pardir))

from confluent_kafka import Consumer
import json
import time
import avro
import io
from fastavro import writer, reader, parse_schema
import fastavro
from datetime import datetime
from random import randint
from uuid import uuid4
import csv
from cassandra.cluster import Cluster
from cassandra.cqlengine.connection import set_session
from dataset.conn import connect_to_keyspace, close_connection
from dataset.execute import create_tables, drop_tables
from dataset.models import User, Website, Ads, Clicks, Impression, Bid
from config.constants import topics, click_csv_schema, bid_schema
from config.config import config_dict_consumer, config_keyspace


def consume_user(msg):
    # This callback is called when a message is received on the 'user' topic
    try:
        data = json.loads(msg.value())
        if data['user_id'] in [user.user_id for user in User.objects().all()]:
            print(f"User already exists: {data}")
        else:
            user = User.create(user_id=data['user_id'], username=data['username'])
            user.save()
        print(f"User data consumed: {data}")
    except Exception as e:
        print(f"Error consuming user data: {e}")

def consume_website(msg):
    # Handle consumption of 'website' topic
    try:
        data = json.loads(msg.value())
        website = Website.create(website_id=data['website_id'], url=data['url'])
        website.save()
        print(f"Website data consumed: {data}")
    except Exception as e:
        print(f"Error consuming website data: {e}")
        
def consume_ads(msg):
    # Handle consumption of 'ads' topic
    try:
        data = json.loads(msg.value())
        if data['creative_id'] in [ads.creative_id for ads in Ads.objects().all()]:
            print(f"Ads already exists: {data}")
        else:
            ads = Ads.create(creative_id=data['creative_id'], ad_name=data['ad_name'])
            ads.save()
        print(f"Ads data consumed: {data}")
    except Exception as e:
        print(f"Error consuming ads data: {e}")
        
def consume_click(msg):
    # Handle consumption of 'click' topic
    try:
        data = next(csv.reader(msg.value().decode('utf-8').splitlines()))
        data = {click_csv_schema[i]: data[i] for i in range(len(click_csv_schema))}
                
        if data['click_id'] in [click.click_id for click in Clicks.objects().all()]:
            print(f"Click already exists: {data}")
        else:
            click = Clicks.create(
                click_id=data['click_id'],
                user_id=data['user_id'],
                creative_id=data['creative_id'],
                click_time=datetime.strptime(data['click_time'], "%Y-%m-%d %H:%M:%S.%f"),
                conversion_type=data['conversion_type']
            )
            click.save()
        print(f"Click data consumed: {data}")
    except Exception as e:
        print(f"Error consuming click data: {e}")
        
def consume_impression(msg):
    # Handle consumption of 'impression' topic
    try:
        data = json.loads(msg.value())
        data['user_id'] = str(data['user_id'])  # Ensure user_id is a string
        data['website_id'] = str(data['website_id'])  # Ensure website_id is a string
        data['creative_id'] = str(data['creative_id'])  # Ensure creative_id is a string
        data['impression_time'] = datetime.strptime(data['impression_time'], "%Y-%m-%d %H:%M:%S.%f")  # Convert impression_time to datetime
        if data['impression_id'] in [impression.impression_id for impression in Impression.objects().all()]:
            print(f"Impression already exists: {data}")
        else:
            impression = Impression.create(**data)
            impression.save()
        print(f"Impression data consumed: {data}")
    except Exception as e:
        print(f"Error consuming impression data: {e}")
        
def consume_bid(msg):
    # Handle consumption of 'bid' topic
    try:
        data = next(fastavro.reader(io.BytesIO(msg.value()), reader_schema=bid_schema))
        if data['bid_id'] in [bid.bid_id for bid in Bid.objects().all()]:
            print(f"Bid already exists: {data}")
        else:
            bid = Bid.create(
                bid_id=data['bid_id'],
                user_id=data['user_id'],
                website_id=data['website_id'],
                creative_id=data['creative_id'],
                bid_time=datetime.strptime(data['bid_time'], "%Y-%m-%d %H:%M:%S.%f"),
                bid_amount=data['bid_amount']
            )
            bid.save()
        print(f"Bid data consumed: {data}")
    except Exception as e:
        print(f"Error consuming bid data: {e}")

# Define similar consume functions for other topics (ads, impression, click, bid)

def consume_messages():
    consumer = Consumer(config_dict_consumer)
    consume_topic = {"user": consume_user, "website": consume_website, "ads": consume_ads, "impression": consume_impression, "click": consume_click, "bid": consume_bid}

    create_tables()
    session = connect_to_keyspace(config_keyspace)
    set_session(session)    

    if consumer is not None:
        consumer.subscribe(topics)

        try:
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue

                if msg.error():
                    print(f"Error: {msg.error()}")
                    continue

                # Call the appropriate consume function based on the topic
                if msg.topic() in consume_topic:
                    consume_topic[msg.topic()](msg)
                    
                # Add similar conditions for other topics (ads, impression, click, bid)

        except KeyboardInterrupt:
            pass
        finally:
            close_connection(session)
            consumer.close()

if __name__ == "__main__":
    consume_messages()