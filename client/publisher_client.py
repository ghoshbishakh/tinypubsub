import requests
import argparse
from strgen import StringGenerator

parser = argparse.ArgumentParser()
parser.add_argument('-id','--pub_name',type= str, help='Name of the publisher', required=True)
parser.add_argument('-s', '--server',type=str,help='IP address of the pubsub service', default='127.0.0.1')
parser.add_argument('-p', '--port',type=int,help='por of the pubsub service', default='9999')

args = parser.parse_args()

publisher_name = args.pub_name
service_addr = args.server
service_port = args.port
service_url = 'http://' + service_addr + ':' + str(service_port)
# print(args.server)


def create_topic(topic_name):
    data = {'pub_name':publisher_name,'topic_name':topic_name}
    r = requests.post(service_url + '/createtopic', json=data)
    if r.status_code == 200 :
        print('POST successful')
    else :
        print('HTTP Respone code : ' + str(r.status_code) + '  ' + r.reason)
    print(r.text)


def publish_to_topic(topic_name, msgs):
    data = {'pub_name':publisher_name,'payload':msgs}
    r = requests.post(service_url + '/publish/' + topic_name, json=data)
    if r.status_code == 200 :
        print('POST successful')
    else :
        print('HTTP Respone code : ' + str(r.status_code) + '  ' + r.reason)
    print(r.text)

def main():
    create_topic('Apache')
    # msgs = StringGenerator('[\\w\\p\\d]{20}').render_list(1000,unique=True)
    # publish_to_topic('Apache',msgs)
    # msgs = StringGenerator('[\\w\\p\\d]{20}').render_list(1000,unique=True)
    # publish_to_topic('Apache',msgs)
    # msgs = StringGenerator('[\\w\\p\\d]{20}').render_list(1000,unique=True)
    # publish_to_topic('Apache',msgs)


main()