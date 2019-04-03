import requests
import argparse
from strgen import StringGenerator
import json
import os
import json

from twitter import Api

# Either specify a set of keys here or use os.getenv('CONSUMER_KEY') style
# assignment:

CONSUMER_KEY = ''
# CONSUMER_KEY = os.getenv("CONSUMER_KEY", None)
CONSUMER_SECRET = ''
# CONSUMER_SECRET = os.getenv("CONSUMER_SECRET", None)
ACCESS_TOKEN = ''
# ACCESS_TOKEN = os.getenv("ACCESS_TOKEN", None)
ACCESS_TOKEN_SECRET = ''
# ACCESS_TOKEN_SECRET = os.getenv("ACCESS_TOKEN_SECRET", None)

# Since we're going to be using a streaming endpoint, there is no need to worry
# about rate limits.
api = Api(CONSUMER_KEY,
          CONSUMER_SECRET,
          ACCESS_TOKEN,
          ACCESS_TOKEN_SECRET)



parser = argparse.ArgumentParser()
parser.add_argument('-id','--pub_name',type= str, help='Name of the publisher', required=True)
parser.add_argument('-s', '--server',type=str,help='IP address of the pubsub service', default='127.0.0.1')
parser.add_argument('-p', '--port',type=int,help='port of the pubsub service', default='9999')
parser.add_argument('-k','--keywords', action='append', help='twitter keywords to stream', required=True)
parser.add_argument('-t', '--topic',type=str,help='topic name', required=True)

args = parser.parse_args()

publisher_name = args.pub_name
service_addr = args.server
service_port = args.port
service_url = 'http://' + service_addr + ':' + str(service_port)
keyword_list = args.keywords
topic = args.topic

# print(args.server)


def create_topic(topic_name):
    data = {'pub_name':publisher_name,'topic_name':topic_name}
    r = requests.post(service_url + '/createtopic', json=data)
    if r.status_code == 200 :
        print('Topic successfully created')
    else :
        print('HTTP Response code : ' + str(r.status_code) + '  ' + r.reason)
    print('Response from server : ' + r.text)


def publish_to_topic(topic_name, msgs):
    r = requests.post(service_url + '/publish/' + topic_name, json=msgs)
    if r.status_code == 200 :
        print('Publish successful')
    else :
        print('HTTP Respone code : ' + str(r.status_code) + '  ' + r.reason)
    print('Response from server : ' + r.text)

def main():
    print(topic)
    print(keyword_list)

    create_topic(topic)
    for line in api.GetStreamFilter(track=['f1', 'formula 1', 'wtf1']):
        print("Publishing: ", line['text'])
        print("----------------------------------------------------------")
        publish_to_topic(topic,[line['text']])

    # msgs = StringGenerator('[\\w\\p\\d]{20}').render_list(10,unique=True)
    # publish_to_topic('Apache',msgs)
    # msgs = StringGenerator('[\\w\\p\\d]{20}').render_list(10,unique=True)
    # publish_to_topic('Apache',msgs)
    # # print(msgs)
    # msgs = StringGenerator('[\\w\\p\\d]{20}').render_list(10,unique=True)
    # publish_to_topic('Apache',msgs)


main()