import pickle

metadata = {}

def load_metadata_from_file():
	with open('metadata.pickle', 'r') as f:
		try:
			metadata = pickle.load(f)
		except:
			print('No metadata file found, initializing metadata..')
			metadata = {}
			metadata['topics'] = []
			metadata['subscriptions'] = {}
			ff = open('metadata.pickle', 'w')
			pickle.dump(metadata, ff)
			ff.close()

def get_topics():
	if metadata:
		return metadata['topics']
	else:
		load_metadata_from_file()
		return metadata['topics']

def check_subscription(subsciber_name, topic_name):
	if topic_name in metadata['topics'] and subsciber_name in metadata['subscriptions'][topic_name]:
		return True
	return False