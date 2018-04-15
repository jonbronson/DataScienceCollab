from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy import API

import re


CONSUMER_KEY = 'huLv8s01aKfdBlU2uKWSXZjXk'
CONSUMER_SECRET = 'H8pIbDxV89ncAb2gMCU5AnvxVjEB1OsvqQ2lkmq7YVn7Efmcmf'
ACCESS_KEY = '21141927-Wnr41tNWNN6P3MCwstGWqtYypnpv2pLmcLaD2Zxiz'
ACCESS_SECRET = '1Z86XSw72qwRlKErocDh3ZqkkZfIpg6k3yTq643kf8c2q'


class MyStreamListener(StreamListener):
    def strip_emojis(self, tweet_text: str):
        # TODO: this is only the first match in the emoji unicode table
        emoji_list1 = re.compile(u'[\U0001f300-\U0001f5fF]')
        stripped_tweet_text = emoji_list1.sub(' ', tweet_text)

        return stripped_tweet_text

    def on_status(self, status):
        tweet = self.strip_emojis(status.text)
        #print('tweet: [{}]'.format(tweet))

        # accumulate?

    def on_error(self, status_code):
        if status_code == 420:
            print("rate limited!")
            return False


if __name__ == '__main__':
    auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_KEY, ACCESS_SECRET)
    api = API(auth)

    my_stream_listener = MyStreamListener()
    my_stream = Stream(auth=api.auth, listener=my_stream_listener)

    # ???
    #my_stream.firehose(count=100000)
    #
    # filter - works!
    #my_stream.filter(track=['facebook'], languages=['en'])

    # basic - works!
    my_stream.sample(languages=['en'])


