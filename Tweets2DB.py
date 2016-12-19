#this script will save from stream tweets directly into database

from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import time
import json
import sqlite3

#        replace mysql.server with "localhost" if you are running via your own server!
#                        server       MySQL username	MySQL pass  Database name.
conn = sqlite3.connect('starwarsX.sqlite')
conn.text_factory = str
cur = conn.cursor()


#consumer key, consumer secret, access token, access secret.
ckey="6XaQG4zLLYofSW3sxYyX2xnPd"
csecret="WcyMD5WyWHe1wC9db3zq6sqM9g1MnwApTbAbuFQXqeyPJzL5Qb"
atoken="2345692375-dHATdOL5ZadGoIM0QNnPAqxyqLvaQ25fSsaEigG"
asecret="raSOZY8i0l1AxP7C7CDUjLARWswDgpWYjuB27oeaQlSzD"

cur.execute('''DROP TABLE IF EXISTS Tweets ''')
cur.execute('''CREATE TABLE IF NOT EXISTS Tweets
    (id INTEGER PRIMARY KEY, tweet TEXT, username Text, language Text, location Text)''')

class listener(StreamListener):

    def on_data(self, data):

        all_data = json.loads(data)
        tweet = all_data["text"]
        username = all_data["user"]["screen_name"]
        language = all_data["lang"]
        location = all_data["user"]["location"]

        cur.execute('INSERT OR IGNORE INTO Tweets (tweet, username, language, location) VALUES (?, ?, ?, ?)', (tweet, username, language, location) )

        conn.commit()

        print((username,tweet))

        return True

    def on_error(self, status):
        print status

auth = OAuthHandler(ckey, csecret)
auth.set_access_token(atoken, asecret)

twitterStream = Stream(auth, listener())
twitterStream.filter(track=["Rogue one", "Star wars"])
cur.close()
