import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), os.pardir))

from cassandra.cluster import Cluster
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
from dataset.conn import connect_to_keyspace, close_connection

class User(Model):
    user_id = columns.UUID(primary_key=True)
    username = columns.Text()
    
class Website(Model):
    website_id = columns.UUID(primary_key=True)
    url = columns.Text()
    
class Ads(Model):
    creative_id = columns.UUID(primary_key=True)
    ad_name = columns.Text()
    
class Clicks(Model):
    click_id = columns.UUID(primary_key=True)
    user_id = columns.UUID()
    creative_id = columns.UUID()
    click_time = columns.DateTime()
    conversion_type = columns.Text()
    
class Impression(Model):
    impression_id = columns.UUID(primary_key=True)
    user_id = columns.UUID()
    website_id = columns.UUID()
    creative_id = columns.UUID()
    impression_time = columns.DateTime()
    
class Bid(Model):
    bid_id = columns.UUID(primary_key=True)
    user_id = columns.UUID()
    website_id = columns.UUID()
    creative_id = columns.UUID()
    bid_time = columns.DateTime()
    bid_amount = columns.Float()
    
if __name__ == "__main__":
    print(User)
    print(Website)
    print(Ads)
    print(Clicks)
    print(Impression)
    