# export FLASK_APP=broker.py
from flask import Flask
from flask import render_template
from flask import request
from flask import redirect, url_for

app = Flask(__name__)



@app.route('/')
def index_view():
    return "hi"


@app.route('/publish/<topic>', methods=['POST'])
def publish_view(topic):
	print(topic)
	print(request.data)
	return 'Success'    
