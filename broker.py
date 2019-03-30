from flask import Flask
from flask import render_template
from flask import request
from flask import redirect, url_for
import json
import metadata_manager


app = Flask(__name__)



@app.route('/')
def index_view():
    return "hi"


# Publisher API

@app.route('/publish/<topic>', methods=['POST'])
def publish_view(topic):
    # CHECK IF TOPIC EXISTS
    # WRITE TO CORRECT FILE
    print(topic)
    print(request.data)
    return 'Success/Failure'    


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
    # CREATE IF FOES NOT EXIST
    metadata_manager.add_topic(topic_name)
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

