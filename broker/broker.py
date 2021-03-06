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
from apscheduler.schedulers.background import BackgroundScheduler

from .config import replicas
from .config import my_address

replicas.remove(my_address)

primary_replica = max(replicas + [my_address])

print("""========== TINY PUBSUB ===========
ADDRESS: %s
REPLICAS: %s
PRIMARY: %s
================================="""%(my_address, str(replicas), primary_replica))


storage_dir = './storage'

app = Flask(__name__)

MAXTRIES = 3

publish_lock = {}
replicas_lock = Lock()

# Load locks
topic_list = metadata_manager.get_topics()
for topic in topic_list:
    publish_lock[topic] = Lock()

# Utility functions


def publish(topic, data):
    # publish_lock[topic].acquire()
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
    # publish_lock[topic].release()


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


# Publisher Endpoints  =================================================================================

@app.route('/publish/<topic>', methods=['POST'])
def publish_view(topic):
    # Redirect if not primary
    if primary_replica != my_address:
        return redirect(primary_replica + '/publish/' + topic, code=307)

    # CHECK IF TOPIC EXISTS
    if not topic in metadata_manager.get_topics():
        return 'Topic not found', 404

    publish_lock[topic].acquire()
    data = json.loads(request.data)
    publish(topic, data)

    for replica in replicas:
        tries = 0
        while tries < MAXTRIES:
            try:
                r = requests.post(replica + '/publish_repl/' + topic, data=request.data, timeout=1)
                if r.status_code == 200 :
                    print('Publish successful to replica : ' + replica)
                    break
            except:
                print("Timeout replica: %s try number: %s"%(replica, tries))
                tries += 1
        if tries == MAXTRIES:
            print('Replica at ' + replica + ' is down!!')
            remove_replica(replica)

    publish_lock[topic].release()
    return 'Success'

@app.route('/publish_repl/<topic>', methods=['POST'])
def publish_view_replica(topic):
    if not topic in metadata_manager.get_topics():
        return 'Topic not found', 404

    data = json.loads(request.data)
    publish_lock[topic].acquire()
    publish(topic, data)
    publish_lock[topic].release()

    return 'Success'



@app.route('/createtopic', methods=['POST'])
def createtopic_view():
    # Redirect if not primary
    if primary_replica != my_address:
        return redirect(primary_replica + '/createtopic', code=307)

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
        tries = 0
        while tries < MAXTRIES:
            try:
                r = requests.post(replica + '/createtopic_repl' ,data=request.data, timeout=1)
                if r.status_code == 200 :
                    print('Topic added successfully to replica : ' + replica)
                    break
            except:
                print("Timeout replica: %s try number: %s"%(replica, tries))
                tries += 1
        if tries == MAXTRIES:
            print('Replica at ' + replica + ' is down!!')
            remove_replica(replica)

    publish_lock[topic_name] = Lock()
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
    publish_lock[topic_name] = Lock()
    return 'Topic added successfully: ' + topic_name

# Subscriber Endpoints ========================================================================

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
    # Redirect to primary
    if primary_replica != my_address:
        return redirect(primary_replica + '/subscribe', code=307)

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

    for replica in replicas:
        tries = 0
        while tries < MAXTRIES:
            try:
                r = requests.post(replica + '/subscribe_repl' ,data=request.data, timeout=1)
                if r.status_code == 200 :
                    print('Subscription added successfully to replica : ' + replica)
                    break
            except:
                print("Timeout replica: %s try number: %s"%(replica, tries))
                tries += 1
        if tries == MAXTRIES:
            print('Replica at ' + replica + ' is down!!')
            remove_replica(replica)
    return 'Successfully subscribed', 200    


@app.route('/subscribe_repl', methods=['POST'])
def subscribe_view_replica():
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

# Replication routines ====================================================================

@app.route('/heartbeat', methods=['POST'])
def heartbeat_view():
    global primary_replica
    try:
        data = json.loads(request.data)
        replica_name = data['address']
    except:
        return 'Invalid POST data', 404
    if replica_name not in replicas:
        replicas_lock.acquire()
        replicas.append(replica_name)
        # update primary
        old_primary = primary_replica
        primary_replica = max(replicas + [my_address])
        if primary_replica != old_primary:
            print("PRIMARY CHANGED =========> ", primary_replica)
        replicas_lock.release()

    return 'Success', 200

def heartbeat_exchange():
    print("Checking heartbeat..")
    data = {'address':my_address}
    for replica in replicas:
        print("Checking heartbeat for %s"%(replica,))
        tries = 0
        while tries < MAXTRIES:
            try:
                r = requests.post(replica + '/heartbeat',json=data, timeout=1)
                if r.status_code == 200:
                    break
            except:
                print("heartbeat timeout: %s"%(replica,))
                tries += 1
        
        if tries == MAXTRIES:
            print('Replica at ' + replica + ' is down!!')
            # MIGHT NEED A LOCK
            remove_replica(replica)

def remove_replica(replica):
    global primary_replica
    replicas_lock.acquire()
    if replica in replicas:
        replicas.remove(replica)
    old_primary = primary_replica
    primary_replica = max(replicas + [my_address])
    if primary_replica != old_primary:
        print("PRIMARY CHANGED =========> ", primary_replica)
    replicas_lock.release()


scheduler = BackgroundScheduler()
scheduler.add_job(func=heartbeat_exchange, trigger="interval", seconds=3)
scheduler.start()
