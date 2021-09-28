import socket
import sys
import requests
import requests_oauthlib
import json

# Replace the values below with yours
ACCESS_TOKEN = '1438184742273306634-fOkBlIQQ3KeGqAjJbIPcS7lK2RUc1G'
ACCESS_SECRET = 'CnpnKfQgqEzsx7AYFiWOTWHNVaOAeYcchLlPvqZHrjn54'
CONSUMER_KEY = 'wD0QUTlbO0MapO9TRu6J7khBp'
CONSUMER_SECRET = 'FngYcVbIg7hGvNLHcOAl3htOBY3uUP4qdkVp4XfQIvBIk8i5Cu'
my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET)

def get_tweets():
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    query_data = [('language', 'en'), ('locations', '-130,-20,100,50'),('track','#')]
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    response = requests.get(query_url, auth=my_auth, stream=True)
    print(query_url, response)
    return response


def send_tweets_to_spark(http_resp, tcp_connection):
    for line in http_resp.iter_lines():
        try:
            full_tweet = json.loads(line)
            tweet_text = full_tweet['text']
            print("Tweet Text: " + tweet_text)
            print ("------------------------------------------")
            #tweet_text += '\n'
            tweet_text = bytes(tweet_text + "\n", 'utf-8')
            tcp_connection.send(tweet_text)#.encode())

        except:
            e = sys.exc_info()[1]
            print("Error: %s" % e)

def get_fake_tweets():
    return "hello world #tweet"

def send_fake_tweets(resp, tcp_connection):
    return tcp_connection.send(bytes(resp + " \n",'utf-8'))

TCP_IP = "localhost"
TCP_PORT = 9009
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
conn, addr = s.accept()
print("Connected... Starting getting tweets.")
resp = get_tweets()
send_tweets_to_spark(resp,conn)
#while(True):
    #resp = get_fake_tweets()
 #   send_fake_tweets(resp, conn)