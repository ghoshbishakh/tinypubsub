import requests
import argparse
import time,json

parser = argparse.ArgumentParser()
parser.add_argument('-id','--sub_name',type= str, help='Name of the subsciber', required=True)
parser.add_argument('-s', '--server',type=str,help='IP address of the pubsub service', default='127.0.0.1')
parser.add_argument('-p', '--port',type=int,help='por of the pubsub service', default='9999')
parser.add_argument('-t', '--topic',type=str,help='name of topic to subscribe and pull', required=True)

args = parser.parse_args()

subscriber_name = args.sub_name
service_addr = args.server
service_port = args.port
service_url = 'http://' + service_addr + ':' + str(service_port)

topic_offsets = {}
topic = args.topic

def subscribe_to(topic_name):
    data = {'sub_name':subscriber_name,'topic_name': topic_name}
    r = requests.post(service_url + '/subscribe',json=data)
    if r.status_code == 200 :
        print("POST successful")
        topic_offsets[topic_name] = 1
    else :
        print("HTTP Response code : " + str(r.status_code) + "  " + r.reason)
    print('Response from server : ' + r.text)

def unsubscribe_from(topic_name):
    data = {'sub_name':subscriber_name,'topic_name': topic_name}
    r = requests.post(service_url + '/unsubscribe',json=data)
    if r.status_code == 200 :
        print("POST successful")
        del topic_offsets[topic_name]
    else :
        print("HTTP Response code : " + str(r.status_code) + "  " + r.reason)
    print('Response from server : ' + r.text)

def read_data(topic_name):
    # print(service_url + '/readfrom/' + topic_name + '/' + str(topic_offsets[topic_name]))
    r = requests.get(service_url + '/readfrom/' + topic_name + '/' + str(topic_offsets[topic_name]),params = {'sub_name':subscriber_name})
    # read data
    if r.status_code == 200 :
        print("READ successful")
        topic_offsets[topic_name] += 1
    else :
        print("HTTP Response code : " + str(r.status_code) + "  " + r.reason)
    print('Response from server : ' + r.text)

def read_all(topic_name):
    r = requests.get(service_url + '/readallfrom/' + topic_name + '/' + str(topic_offsets[topic_name]),params = {'sub_name':subscriber_name})
    if r.status_code == 200 :
        print("READ successful")
        data_list = json.loads(r.text)
        # print(data_list)
        topic_offsets[topic_name] += len(data_list)
    else :
        print("HTTP Response code : " + str(r.status_code) + "  " + r.reason)
    print('Response from server : ' + r.text)

def perioidic_pull(topic_name,time_interval):
    try:
        while True:
            read_all(topic_name)
            time.sleep(time_interval)
    except KeyboardInterrupt:
        print('')

def main():
    subscribe_to(topic)
    # for _ in range(1,10):
    #     read_data('Apache')
    perioidic_pull(topic,3)
    print("hello, I am back")

main()