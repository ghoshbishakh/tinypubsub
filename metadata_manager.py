import pickle

metadata = None

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
	# TOPDO