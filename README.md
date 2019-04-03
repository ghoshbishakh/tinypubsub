# Tiny PubSub
[![Build Status](https://travis-ci.org/ghoshbishakh/tinypubsub.svg?branch=master)](https://travis-ci.org/ghoshbishakh/tinypubsub)

## Development Environment Setup

1. Install `virtulenv`
2. Create a virtualenv `virtualenv venv`
3. Activte it `source venv/bin/activate`
4. Install requirements `pip install -r requirements.txt`
5. Run tests `./test.sh`

## Running brokers and clients

### Running broker
```
./run.sh
```

### Running publisher and subscribers
Publisher:
```
python client/publisher_client_random.py [-h] -id PUB_NAME [-s SERVER] [-p PORT]
```
Subscriber:
```
python subscriber_client.py [-h] -id SUB_NAME [-s SERVER] [-p PORT]
```