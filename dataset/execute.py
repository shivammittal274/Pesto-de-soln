import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), os.pardir))

from cassandra.cluster import Cluster
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.management import sync_table, drop_table
from cassandra.cqlengine.connection import set_session
from cassandra.query import dict_factory

from argparse import ArgumentParser

from dataset.conn import connect_to_keyspace, close_connection
from dataset.models import User, Website, Ads, Clicks, Impression, Bid
from config.config import config_dict_cluster, config_keyspace

def create_tables():
    session = connect_to_keyspace(config_keyspace)
    set_session(session)
    
    try:
        sync_table(User)
    except Exception as e:
        print(e)
    try:
        sync_table(Website)
    except Exception as e:
        print(e)
    try:
        sync_table(Ads)
    except Exception as e:
        print(e)
    try:
        sync_table(Clicks)
    except Exception as e:
        print(e)
    try:
        sync_table(Impression)
    except Exception as e:
        print(e)
    try:
        sync_table(Bid)
    except Exception as e:
        print(e)

    print("Tables created.")
    close_connection(session)

def drop_tables():
    session = connect_to_keyspace(config_keyspace)
    set_session(session)

    try:
        drop_table(User)
    except Exception as e:
        print(e)
    try:
        drop_table(Website)
    except Exception as e:
        print(e)
    try:
        drop_table(Ads)
    except Exception as e:
        print(e)
    try:
        drop_table(Clicks)
    except Exception as e:
        print(e)
    try:
        drop_table(Impression)
    except Exception as e:
        print(e)
    try:
        drop_table(Bid)
    except Exception as e:
        print(e)

    print("Tables dropped.")
    close_connection(session)
    
def check_tables():
    session = connect_to_keyspace(config_keyspace)
    set_session(session)
    print("Checking tables...")

    print("\nUser Table:", len(User.objects().all()))
    for user in User.objects().all():
        print(user.user_id, user.username)

    print("\nWebsite Table:", len(Website.objects().all()))
    for website in Website.objects().all():
        print(website.website_id, website.url)

    print("\nAds Table:", len(Ads.objects().all()))
    for ad in Ads.objects().all():
        print(ad.creative_id, ad.ad_name)
        
    print("\nClicks Table:", len(Clicks.objects().all()))
    for click in Clicks.objects().all():
        print(click.click_id, click.user_id, click.creative_id, click.click_time, click.conversion_type)
        
    print("\nImpression Table:", len(Impression.objects().all()))
    for impression in Impression.objects().all():
        print(impression.impression_id, impression.user_id, impression.website_id, impression.creative_id, impression.impression_time)
        
    print("\nBid Table:", len(Bid.objects().all()))
    for bid in Bid.objects().all():
        print(bid.bid_id, bid.user_id, bid.website_id, bid.creative_id, bid.bid_time, bid.bid_amount)

    close_connection(session)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--create", help="Create tables", action="store_true")
    parser.add_argument("--drop", help="Drop tables", action="store_true")
    parser.add_argument("--check", help="Check tables", action="store_true")
    args = parser.parse_args()
    if args.create:
        create_tables()
    if args.drop:
        drop_tables()
    if args.check:
        check_tables()