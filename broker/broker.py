from flask import Flask
from flask import render_template
from flask import request
from flask import redirect, url_for
import os
from broker import metadata_manager
import json
import pickle

storage_dir = './storage'

app = Flask(__name__)



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

    # Open index and data files
    index_file = open(os.path.join(storage_dir, topic, 'index.pickle'),'rb+')
    old_index = pickle.load(index_file)
    data_file = open(os.path.join(storage_dir, topic, 'data.dat'),'ab')
    for data_item in data:
        old_index.append(old_index[-1] + len(data_item))
        print("appending -->",data_item)
        data_file.write(data_item.encode())

    # Update index file
    index_file.seek(0)
    pickle.dump(old_index, index_file)

    # Close files
    index_file.close()
    data_file.close()
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
    return 'Topic added successfully: ' + topic_name


# Subscriber Endpoints

@app.route('/readfrom/<topic>/<offset>', methods=['GET'])
def read_view(topic):
    # CHECK IF TOPIC EXISTS
    # READ SUBSCRIBER IF FROM request
    # CHECK IF SUBSCRIBER SUBSCRIBED
    return 'Success/Failure with payload'    


@app.route('/subscribe/<topic>', methods=['POST'])
def subscribe_view(topic):
    # CHECK IF TOPIC EXISTS
    # SUBSCRIBE
    return 'Success/Failure'    


@app.route('/unsubscribe/<topic>', methods=['GET'])
def unsubscribe_view(topic):
    # CHECK IF TOPIC EXISTS
    # UNSUBSCRIBE
    return 'Success/Failure'    

