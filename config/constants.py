# topic names
topics = ["user", "website", "ads", "impression", "click", "bid"]

# click csv headers
click_csv_schema = ["click_id", "user_id", "creative_id", "click_time", "conversion_type"]

# ad impression conversion types
conversion_types = ['visit', 'purchase', 'report', 'install']

# bid avro schema
bid_schema = {
    "type": "record",
    "name": "Bid",
    "fields": [
        {"name": "bid_id", "type": "string"},
        {"name": "user_id", "type": "string"},
        {"name": "website_id", "type": "string"},
        {"name": "creative_id", "type": "string"},
        {"name": "bid_time", "type": "string"},
        {"name": "bid_amount", "type": "float"}
    ]
}