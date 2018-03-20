import tweepy
import pymongo
from textblob import TextBlob
from sqlalchemy.exc import ProgrammingError
import json
import requests
from urllib3.exceptions import ProtocolError

client = pymongo.MongoClient()
db = client.cryptoDB
TRX = db.TRX
BTC = db.BTC
ETH = db.ETH
LTC = db.LTC
SUMO = db.SUMO

def currentPrice():
        r = requests.get('https://api.coindesk.com/v1/bpi/currentprice.json')
        price = r.json()['bpi']['USD']['rate']
        print(price)
currentPrice()

class StreamListener(tweepy.StreamListener):
 
    def on_connect(self):
        print("STREAM CONNECTED:")
        return True

    def on_status(self, status):
        if status.retweeted:
            return

        description = status.user.description
        loc = status.user.location
        text = status.text
        coords = status.coordinates
        geo = status.geo
        name = status.user.screen_name
        user_created = status.user.created_at
        followers = status.user.followers_count
        id_str = status.id_str
        date_created = status.created_at.date()
        timestamp = status.created_at
        retweets = status.retweet_count
        bg_color = status.user.profile_background_color
        blob = TextBlob(text)
        sent = blob.sentiment

        if geo is not None:
            geo = json.dumps(geo)

        if coords is not None:
            coords = json.dumps(coords)

        processedtweet = dict(
                user_description=description,
                user_location=loc,
                coordinates=coords,
                text=text,
                geo=geo,
                user_name=name,
                user_created=user_created,
                user_followers=followers,
                id_str=id_str,
                #date_created=date_created,
                timestamp = timestamp,
                retweet_count=retweets,
                user_bg_color=bg_color,
                polarity=sent.polarity,
                subjectivity=sent.subjectivity,
                #price=currentPrice()
                )

        if "$TRX" in text or "Tron" in text:
            try:
                TRX.insert_one(processedtweet)
                #print("New Status: TRX")
            
            except ProgrammingError as err:
                print(err)

        if "$LTC" in text or "Litecoin" in text:
            try:
                LTC.insert_one(processedtweet)
                #print("New Status: LTC")
            
            except ProgrammingError as err:
                print(err)

        if "$ETH" in text or "Ethereum" in text:
            try:
                ETH.insert_one(processedtweet)            
                #print("New Status: ETH")
            
            except ProgrammingError as err:
                print(err)

        if "$SUMO" in text or "Sumokoin" in text:
            try:
                SUMO.insert_one(processedtweet)            
                #print("New Status: SUMO")
            
            except ProgrammingError as err:
                print(err)

        if "$BTC" in text or "Bitcoin" in text:
            try:
                BTC.insert_one(processedtweet)            
                #print("New Status: BTC")
            
            except ProgrammingError as err:
                print(err)

    def on_exception(self, exception):
        # called when an unhandled exception occurs
        print("Unhandled exception occurred:")
        print(exception)
        return
    
    def on_limit(self, track):
        # called when a limitation notice arrives
        print("Twitter sent limitation notice:")
        print(track)
        return
    
    def on_disconnect(self, notice):
        # called when twitter sends a disconnect notice
        print("Twitter sent disconnect notice:")
        print(notice)
        return False
    
    def on_timeout(self):
        # called when the stream connection times out
        print("Stream connection timed out.")
        return False
    
    def on_warning(self, notice):
        # called when disconnect warning arrives
        # we get these warnings when we set stall_warning to TRUE
        print("Twitter sent us a disconnect warning:")
        print(notice)
        return

    def on_error(self, status_code):
        print(status_code)
        # 429 returned when app exceeds rate limit (Too Many Requests)
        if status_code == 429:
            # disconnect the stream
            return False
        # 420 returned when app is being rate limited for too many requests
        if status_code == 420:
            # disconnect the stream
            return False

consumer_key = "ol4tLqBzc78ruf9u3L3UgfYaZ"
consumer_secret = "5GRFSfIeHKPxQPsiboPIQe4iBCAZBzn0WOI9kt0sLaKLtgwg04"
access_token = "4002387988-GNrKiqiJrWkekRP1Rl4KEAu75wfkTvb9UcvqBmt"
access_secret ="UlHyHTuBAB3d28ibVBakF6MsDnFBpWO6MQoNPCBzMPnCA"
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)
api = tweepy.API(auth) 


# Twitter may force a disconnect if tweets pile up aka we fail to read them fast enough
# in Twitter dev docs it is called a Full Buffer disconnect
# better solution to implement is putting tweets to be saved into a queue (like Redis)
# Queue would have no problem waiting for us to consume the data
# For now, try just reconnecting stream (will lead to loss of some tweets)
# we can check to see if we are failing to process tweets fast enough
# using the `stall_warnings` param - Twitter will send back warning messages
#while True:
#    try:
        # Connect/Reconnect the stream
#        stream = tweepy.Stream(auth=api.auth, listener=stream_listener)
#        stream.filter(track=["$BTC","Bitcoin"]. stall_warnings=true)
#    except ProtocolError:
        # Shrug, reconnect and keep tracking
#        continue
#    except KeyboardInterupt:
        # something to break out of the loop and end the stream
#        stream.disconnect()
#        break
### lets not do this for now and try to have the stream print the error we encounter
### otherwise we will lose the "log" (messages printed to console) bc of too many messages


stream_listener = StreamListener()
stream = tweepy.Stream(auth=api.auth, listener=stream_listener)
stream.filter(track=["$TRX","$LTC","$ETH","$SUMO","Tron","Litecoin","Ethereum","Sumokoin", "$BTC", "Bitcoin"], stall_warnings=True, async = True)













