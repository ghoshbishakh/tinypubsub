import pickle

metadata_file_name = 'metadata.pickle'
metadata = None

def load_metadata_from_file():
    global metadata
    try:
        f = open(metadata_file_name, 'rb')
        metadata = pickle.load(f)
        f.close()
    except IOError:
        print('No metadata file found, initializing metadata..')
        metadata = {}
        metadata['topics'] = []
        metadata['subscriptions'] = {}
        ff = open('metadata.pickle', 'wb')
        pickle.dump(metadata, ff)
        ff.close()

def load_metadata():
    global metadata
    if metadata:
        return
    else:
        load_metadata_from_file()

def write_metadata():
    global metadata
    with open(metadata_file_name, 'wb') as f:
        pickle.dump(metadata, f)

def get_topics():
    """
    Get list of topics 
    
    Returns: 
    List: List of topics 
  
    """
    load_metadata()
    global metadata
    return metadata['topics']

def check_subscription(subscriber_name, topic_name):
    """
    Check if a subscriber is subscribed to a topic 
  
    Parameters:
    subscriber_name (str): Name of subscriber
    topic_name (str): Name of topic
  
    Returns:
    bool: True if subscribed, false if not subscribed
  
    """
    load_metadata()
    global metadata
    if subscriber_name in metadata['subscriptions']:
        if topic_name in metadata['subscriptions'][subscriber_name]:
            return True
    return False

def add_topic(topic_name):
    """
    Add a topic
  
    Parameters:
    topic_name (str): Name of topic
  
    Returns:
    List: List of updated topics
  
    """
    load_metadata()
    global metadata
    if topic_name not in metadata['topics']:
        metadata['topics'].append(topic_name)
        write_metadata()
    return metadata['topics']

def add_subscription(subscriber_name, topic_name):
    """
    Add a new subscription 
  
    Parameters:
    subscriber_name (str): Name of subscriber
    topic_name (str): Name of topic
  
    Returns:
    List: Updated list of the subscriber's subscriptions
  
    """
    load_metadata()
    global metadata
    if subscriber_name not in metadata['subscriptions']:
        metadata['subscriptions'][subscriber_name] = []
    if topic_name not in metadata['subscriptions'][subscriber_name]:
        metadata['subscriptions'][subscriber_name].append(topic_name)
        write_metadata()
    return metadata['subscriptions'][subscriber_name]
