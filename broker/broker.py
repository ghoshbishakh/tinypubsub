from flask import Flask
from flask import render_template
from flask import request
from flask import redirect, url_for
import os
from broker import metadata_manager
import json
import pickle
import requests
from threading import Lock
import time

storage_dir = './storage'

app = Flask(__name__)

publish_lock = Lock()

replicas = ['http://10.5.18.101:9999'] 

# Utility functions

def publish(topic, data):
    publish_lock.acquire()
    # Open index and data files
    index_file = open(os.path.join(storage_dir, topic, 'index.pickle'),'rb+')
    old_index = pickle.load(index_file)
    data_file = open(os.path.join(storage_dir, topic, 'data.dat'),'ab')
    for data_item in data:
        data_item = data_item.encode()
        old_index.append(old_index[-1] + len(data_item))
        print("appending -->",data_item)
        data_file.write(data_item)

    # Update index file
    index_file.seek(0)
    pickle.dump(old_index, index_file)

    # Close files
    index_file.close()
    data_file.close()
    publish_lock.release()


def read(topic, offset, readall=False):
    # Open index and data files
    index_file = open(os.path.join(storage_dir, topic, 'index.pickle'),'rb')
    old_index = pickle.load(index_file)
    data_file = open(os.path.join(storage_dir, topic, 'data.dat'),'rb')
    if offset >= len(old_index):
        return None

    if readall:
        data_list = []
        data_offset = old_index[offset-1]
        data_file.seek(data_offset)
        for next_data_offset in old_index[offset:]:
            data_size = next_data_offset - data_offset
            data = data_file.read(data_size)
            print(data)
            data_list.append(data.decode('utf-8'))
            data_offset = next_data_offset
        return data_list

    else:
        data_offset = old_index[offset-1]
        data_end_offset = old_index[offset]
        data_size = data_end_offset - data_offset
        # print(data_offset, data_end_offset, data_size)

        # Seek data file
        data_file.seek(data_offset)
        data = data_file.read(data_size)
        # Close files
        index_file.close()
        data_file.close()
        return data


@app.route('/')
def index_view():
    return "This is Tiny PubSub"


# Publisher API

@app.route('/publish/<topic>', methods=['POST'])
def publish_view(topic):
    # CHECK IF TOPIC EXISTS
    if not topic in metadata_manager.get_topics():
        return 'Topic not found', 404

    data = json.loads(request.data)
    publish(topic, data)

    for replica in replicas:
        r = requests.post(replica + '/publish_repl/' + topic,data=request.data)
        if r.status_code == 200 :
            print('Publish successful to replica : ' + replica)
    return 'Success'

@app.route('/publish_repl/<topic>', methods=['POST'])
def publish_view_replica(topic):
    if not topic in metadata_manager.get_topics():
        return 'Topic not found', 404

    data = json.loads(request.data)
    publish(topic, data)
    return 'Success'



@app.route('/createtopic', methods=['POST'])
def createtopic_view():
    try:
        data = json.loads(request.data)
        publisher_name = data['pub_name']
        topic_name = data['topic_name']
    except:
        return 'Invalid POST data', 404
    topic_list = metadata_manager.get_topics()
    if topic_name in topic_list:
        return 'Topic already exists', 404
    # CREATE IF DOES NOT EXIST
    metadata_manager.add_topic(topic_name)
    # CREATE DATA DIRECTORY AND INDEX
    try:
        os.makedirs(os.path.join(storage_dir,topic_name))
        with open(os.path.join(storage_dir,topic_name, 'index.pickle'), 'wb') as f:
            pickle.dump([0], f)
    except:
        raise
        return 'Topic creation failed', 404
        # TODO remove topic
    for replica in replicas:
        r = requests.post(replica + '/createtopic_repl' ,data=request.data)
        if r.status_code == 200 :
            print('Topic added successfully to replica : ' + replica)
    return 'Topic added successfully: ' + topic_name


@app.route('/createtopic_repl', methods=['POST'])
def createtopic_view_replica():
    try:
        data = json.loads(request.data)
        publisher_name = data['pub_name']
        topic_name = data['topic_name']
    except:
        return 'Invalid POST data', 404
    topic_list = metadata_manager.get_topics()
    if topic_name in topic_list:
        return 'Topic already exists', 404
    # CREATE IF DOES NOT EXIST
    metadata_manager.add_topic(topic_name)
    # CREATE DATA DIRECTORY AND INDEX
    try:
        os.makedirs(os.path.join(storage_dir,topic_name))
        with open(os.path.join(storage_dir,topic_name, 'index.pickle'), 'wb') as f:
            pickle.dump([0], f)
    except:
        raise
        return 'Topic creation failed', 404
        # TODO remove topic
    return 'Topic added successfully: ' + topic_name

# Subscriber Endpoints

@app.route('/readfrom/<topic>/<offset>', methods=['GET'])
def read_view(topic, offset):
    """
    Get message from topic with an offset 
    Parameters:
    topic (str): Name of topic
    offset (int): Offset of message starting from 1

    Returns: 
    str: message 
  
    """

    if 'sub_name' not in request.args:
        return 'Invalid request', 404
    try:
        offset = int(offset)
    except:
        return 'Invalid request', 404
    subscriber_name = request.args['sub_name']
    # CHECK IF TOPIC EXISTS
    topic_list = metadata_manager.get_topics()
    if topic not in topic_list:
        return 'Topic does not exist', 404
    
    # CHECK IF SUBSCRIBER SUBSCRIBED
    if not metadata_manager.check_subscription(subscriber_name, topic):
        return 'Subscriber not subscribed to topic', 404

    # READ
    data = read(topic, offset)
    if data:
        return data, 200
    else:
        return 'Offset invalid', 404    


@app.route('/readallfrom/<topic>/<offset>', methods=['GET'])
def read_all_view(topic, offset):
    """
    Get list of all messages from topic with an offset 
    Parameters:
    topic (str): Name of topic
    offset (int): Offset of message starting from 1

    Returns: 
    List: List of messages
  
    """

    if 'sub_name' not in request.args:
        return 'Invalid request', 404
    try:
        offset = int(offset)
    except:
        return 'Invalid request', 404
    subscriber_name = request.args['sub_name']
    # CHECK IF TOPIC EXISTS
    topic_list = metadata_manager.get_topics()
    if topic not in topic_list:
        return 'Topic does not exist', 404
    
    # CHECK IF SUBSCRIBER SUBSCRIBED
    if not metadata_manager.check_subscription(subscriber_name, topic):
        return 'Subscriber not subscribed to topic', 404

    if offset <= 0:
        return 'Offset invalid', 404

    # READ
    data_list = read(topic, offset, True)
    if data_list:
        return json.dumps(data_list), 200
    else:
        return 'Subscriber upto date', 404


@app.route('/subscribe', methods=['POST'])
def subscribe_view():
    # Check POST data
    try:
        data = json.loads(request.data)
        subscriber_name = data['sub_name']
        topic_name = data['topic_name']
    except:
        return 'Invalid POST data', 404
    
    # CHECK IF TOPIC EXISTS
    topic_list = metadata_manager.get_topics()
    if topic_name not in topic_list:
        return 'Topic does not exist', 404
    # SUBSCRIBE
    if metadata_manager.check_subscription(subscriber_name, topic_name):
        return 'Already subscribed', 200
    else:
        subs = metadata_manager.add_subscription(subscriber_name, topic_name)
    return 'Successfully subscribed', 200    


@app.route('/unsubscribe/<topic>', methods=['GET'])
def unsubscribe_view(topic):
    # CHECK IF TOPIC EXISTS
    # UNSUBSCRIBE
    return 'Success/Failure'    


@app.route('/heartbeat', methods=['POST'])
def heartbeat_view():
    return 'Success', 200

def heartbeat_exchange(replica):
    failed_tries = 0
    while True:
        try:
            r = requests.post(replica + '/heartbeat')
        except:
            failed_tries += 1
            if failed_tries == 3:
                print('Replica at ' + replica + ' is down!!')
                # MIGHT NEED A LOCK
                replicas.remove(replica)
                return
            continue
        if r.status_code == 200:
            failed_tries = 0
        time.sleep(3)
    return 
