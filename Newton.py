import json
import time

import findspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
import tweepy
import textblob


findspark.init()
sc = SparkContext()
spark = SparkSession(sc)

bearer_token = 'token'
keyword = 'keyword'
time_limit_seconds = 10

buffer = []

# SQLContext(sc).read.parquet("./tweets.parquet").show(20)


class TweetsListener(tweepy.StreamingClient):
    def __init__(self, bearer_token, time_limit):
        self.start_time = time.time()
        self.limit = time_limit
        super(TweetsListener, self).__init__(bearer_token)

    def on_data(self, data):
        if (time.time() - self.start_time) < self.limit:
            try:
                message = json.loads(data).get('data').get('text')
                print(message)
                blobSenti = textblob.TextBlob(message).sentiment
                print(blobSenti.polarity)
                print(blobSenti.subjectivity)
                buffer.append((message, blobSenti.polarity, blobSenti.subjectivity))
                return True
            except BaseException as e:
                print("Error on_data: %s" % str(e))
            return True
        else:
            return self.disconnect()

    def if_error(self, status):
        print(status)
        return True


twitter_stream = TweetsListener(bearer_token, time_limit_seconds)
twitter_stream.add_rules(tweepy.StreamRule(keyword))
twitter_stream.filter()

while len(buffer) > 100 or not twitter_stream.running:
    columns = ["text", "polarity", "subjectivity"]
    DF = sc.parallelize(buffer).toDF(columns)
    DF.write.mode('overwrite').parquet("./tweets.parquet")
    buffer = []

quit()
