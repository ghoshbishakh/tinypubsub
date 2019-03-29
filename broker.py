# export FLASK_APP=broker.py
from flask import Flask
from flask import render_template
from flask import request
from flask import redirect, url_for
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
def createtopic_view(topicname):
	# CHECK IF TOPIC EXISTS
	# CREATE IF FOES NOT EXIST
	return 'Success/Failure'    


# Subscriber Endpoints

@app.route('/readfrom/<topic>/<offset>', methods=['GET'])
def read_view(topic):
	# CHECK IF TOPIC EXISTS
	# READ SUBSCRIBER IF FROM request
	# CHECK IF SUBSCRIBER SUBSCRIBED
	return 'Success/Failure with payload'    


@app.route('/subscribe/<topic>', methods=['GET'])
def subscribe_view(topic):
	# CHECK IF TOPIC EXISTS
	# SUBSCRIBE
	return 'Success/Failure'    


@app.route('/unsubscribe/<topic>', methods=['GET'])
def unsubscribe_view(topic):
	# CHECK IF TOPIC EXISTS
	# UNSUBSCRIBE
	return 'Success/Failure'    

