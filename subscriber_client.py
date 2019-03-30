import requests
import argparse


parser = argparse.ArgumentParser()
parser.add_argument('-id','--sub_name',type= str, help='Name of the subsciber', required=True)
parser.add_argument('-s', '--server',type=str,help='IP address of the pubsub service', default='127.0.0.1')
parser.add_argument('-p', '--port',type=int,help='por of the pubsub service', default='9999')

args = parser.parse_args()

publisher_name = args.pub_name
service_addr = args.server
