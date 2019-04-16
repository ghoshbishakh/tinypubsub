import requests
import argparse
from strgen import StringGenerator
import json
from time import sleep

parser = argparse.ArgumentParser()
parser.add_argument('-id','--pub_name',type= str, help='Name of the publisher', required=True)
parser.add_argument('-s', '--server',type=str,help='IP address of the pubsub service', default='127.0.0.1')
parser.add_argument('-p', '--port',type=int,help='port of the pubsub service', default='9999')

args = parser.parse_args()

publisher_name = args.pub_name
service_addr = args.server
service_port = args.port
service_url = 'http://' + service_addr + ':' + str(service_port)
# print(args.server)


def create_topic(topic_name):
    data = {'pub_name':publisher_name,'topic_name':topic_name}
    
    try:
        r = requests.post(service_url + '/createtopic', json=data)
    except:
        print("Broker unavailable!!")
        return

    if r.status_code == 200 :
        print('Topic successfully created')
    else :
        print('HTTP Response code : ' + str(r.status_code) + '  ' + r.reason)
    print('Response from server : ' + r.text)


def publish_to_topic(topic_name, msgs):
    try:
        r = requests.post(service_url + '/publish/' + topic_name, json=msgs)
    except:
        print("Broker unavailable!!")
        return

    if r.status_code == 200 :
        print('Publish successful')
    else :
        print('HTTP Response code : ' + str(r.status_code) + '  ' + r.reason)
    print('Response from server : ' + r.text)

def main():
    create_topic('Apache')
    count = 1
    while(1):
        msgs = StringGenerator('[\\w\\p\\d]{20}').render_list(10,unique=True)
        msgs = [publisher_name + "-" + str(count) + ":" + msg for msg in msgs]
        print(msgs)
        publish_to_topic('Apache',msgs)
        count += 1
        sleep(1)
main()