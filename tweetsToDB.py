#this script will save from stream tweets directly into database
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import time
import json
import sqlite3

#We use the sqlite so it's slightly differnet in syntax than mysql
conn = sqlite3.connect('starwarsX.sqlite') #Put your own DB name
conn.text_factory = str
cur = conn.cursor()


#consumer key, consumer secret, access token, access secret.
ckey="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
csecret="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
atoken="xxxxxxxxxxxx-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
asecret="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

# here we create the table or drop it if does exist, you can overwrite if you like
cur.execute('''DROP TABLE IF EXISTS Tweets ''')
cur.execute('''CREATE TABLE IF NOT EXISTS Tweets
    (id INTEGER PRIMARY KEY, tweet TEXT, username Text, language Text, location Text)''')

class listener(StreamListener):

    def on_data(self, data):
        # we parse the tags of interest from the json
        all_data = json.loads(data)
        tweet = all_data["text"]
        username = all_data["user"]["screen_name"]
        language = all_data["lang"]
        location = all_data["user"]["location"]
        # we write the parsed data into the table we have in the DB
        cur.execute('INSERT OR IGNORE INTO Tweets (tweet, username, language, location) VALUES (?, ?, ?, ?)', (tweet, username, language, location) )

        conn.commit()

        print((username,tweet, language, location))

        return True

    def on_error(self, status):
        print status

auth = OAuthHandler(ckey, csecret)
auth.set_access_token(atoken, asecret)

twitterStream = Stream(auth, listener())
#here we filter the tweets, so only tweets of interest are recieved
twitterStream.filter(track=["Rogue one", "Star wars"])
cur.close()
