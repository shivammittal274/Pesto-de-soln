import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), os.pardir))

from confluent_kafka import Producer
import json
import time
from datetime import datetime
from random import randint
from uuid import uuid4
import csv
from dataset.conn import connect_to_keyspace, close_connection
from config.config import config_dict_producer
import avro
import io
from fastavro import write, parse_schema
from argparse import ArgumentParser

from config.constants import conversion_types, bid_schema, click_csv_schema

def produce(n):
    try:
        p = Producer(config_dict_producer)
    except Exception as e:
        print(f"Error connecting to kafka: {e}")
        return
    topics = ["user", "website", "ads", "impression", "click", "bid"]

    for i in range(n):
        user_data, website_data, ads_data, click_data, impression_data, bid_data = None, None, None, None, None, None
        
        # create a user data in json format
        user_id = uuid4()
        username = f"user_{i}"
        user_data = {
            "user_id": str(user_id),
            "username": str(username)
        }
        
        # create a website data in json format
        website_id = uuid4()
        url = f"http://www.example{i}.com"
        website_data = {
            "website_id": str(website_id),
            "url": url
        }
        
        # create an ad data in json format
        creative_id = uuid4()
        ad_name = f"ad_{i}"
        ads_data = {
            "creative_id": str(creative_id),
            "ad_name": str(ad_name)
        }
        
        # create impression data in json format
        impression_id = uuid4()
        impression_time = datetime.now()
        impression_data = {
            "impression_id": str(impression_id),
            "user_id": str(user_id),
            "website_id": str(website_id),
            "creative_id": str(creative_id),
            "impression_time": str(impression_time)
        }
        
        if randint(1, 2)==1:        
            # create clicks data in csv format
            click_id = uuid4()
            click_time = datetime.now() 
            conversion_type = conversion_types[randint(0, len(conversion_types)-1)]
            click_data = io.StringIO()
            click_data_writer = csv.DictWriter(click_data, fieldnames=click_csv_schema)
            click_data_writer.writerow({
                "click_id": str(click_id),
                "user_id": str(user_id),
                "creative_id": str(creative_id),
                "click_time": str(click_time),
                "conversion_type": conversion_type
            })
            
        if randint(1, 2)==1:
            # create bid data in avro format
            bid_id = uuid4()
            bid_time = datetime.now()
            bid_amount = float(randint(1, 1000))
                    
            bid_data = {
                "bid_id": str(bid_id),
                "user_id": str(user_id),
                "website_id": str(website_id),
                "creative_id": str(creative_id),
                "bid_time": str(bid_time),
                "bid_amount": bid_amount
            }
            
            bid_avro_data = io.BytesIO()
            write.writer(bid_avro_data, parse_schema(bid_schema), [bid_data])
                
        # produce data to the topic
        p.produce(topics[0], key=str(user_id), value=json.dumps(user_data))
        p.poll(1000)
        p.produce(topics[1], key=str(website_id), value=json.dumps(website_data))
        p.poll(1000)
        p.produce(topics[2], key=str(creative_id), value=json.dumps(ads_data))
        p.poll(1000)
        p.produce(topics[3], key=str(impression_id), value=json.dumps(impression_data))
        p.poll(1000)
        if click_data:
            p.produce(topics[4], key=str(click_id), value=click_data.getvalue().encode('utf-8'))
        if bid_data:
            p.produce(topics[5], key=str(bid_id), value=bid_avro_data.getvalue())
            p.poll(1000)
            
        print(f"Produced message: {i}")
        # time.sleep(1)
        p.flush()
        
if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-n", "--number", help="Number of messages to produce", type=int, default=10)
    args = parser.parse_args()
    
    produce(args.number)

            
        