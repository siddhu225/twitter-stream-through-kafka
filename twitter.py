import mysql.connector
from tweepy.streaming import StreamListener
from tweepy import OAuthHandle
from mysql.connector import Error
from tweepy import Stream
import json
from dateutil import parser
from kafka import SimpleProducer, KafkaClient

access_token = "1052874772915212290-V3QoRs4yFPtS2SveqXxxfXoSVRX62x"
access_token_secret =  "D3lcxSX8WL95qZr8GIekrgaACUf2CT0SkrDscZYmT3oRD"
consumer_key =  "TeI68mR2ss2siq2PLdMZaziM5"
consumer_secret =  "ivlLZRYHb9HTdMPzdJhLZuvwJwofsv0xRa3CLzQJFg0jZ24zSK"

def connect(username, created_at, tweet, retweet_count, place , location):
	try:
	    con = mysql.connector.connect(host = 'localhost',database='twitterdb', user='root', password = 'siddhu225', charset = 'utf8')
	    if con.is_connected():
		cursor = con.cursor
		query = "INSERT INTO tweets-table(username, created_at, tweet, retweet_count,place, location) VALUES (%s, %s, %s, %s, %s, %s)"
		cursor.execute(query, (username, created_at, tweet, retweet_count, place, location))
		con.commit()
	except Error as e:
		print(e)

	cursor.close()
	con.close()

	return

class StdOutListener(StreamListener):
    def on_data(self, data):
        producer.send_messages("trump", data.encode('utf-8'))
        raw_data = json.loads(data)
        username = raw_data['user']['screen_name']
	created_at = parser.parse(raw_data['created_at'])
	tweet = raw_data['text']
	retweet_count = raw_data['retweet_count']

	if raw_data['place'] is not None:
		place = raw_data['place']['country']
		print(place)
	else:
		place = None
	location = raw_data['user']['location']
	connect(username, created_at, tweet, retweet_count, place, location)
	print("Tweet colleted at: {} ".format(str(created_at)))
				

				
    def on_error(self, status):
        print (status)

kafka = KafkaClient("localhost:9092")
producer = SimpleProducer(kafka)
l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)
stream.filter(track="trump")


#I only know the basic sql commands.But i have done my projects realated to bigdata in my engineering in sparka and storm.I also have good knowledge related to fullstack vue j.s in frontend and node js as backend.
#querys------------------------------------------------
#1).total count of tweets
SELECT COUNT(*) FROM tweets-table
#2).UNIQUE USERS
SELECT DISTINCT username  
FROM tweets-table
#3).Tweets with most retweets
SELECT tweet
FROM tweets-table
WHERE (count(retweet_count)>40)
#4).Tweets with the most favourite count
SELECT COUNT(tweet)
FROM tweets-table    
GROUP BY tweet
ORDER BY COUNT(*) DESC
LIMIT    1;	
