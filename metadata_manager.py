import pickle

metadata = {}

def load_metadata_from_file():
    global metadata
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

def load_metadata():
    global metadata
    if metadata:
        return
    else:
        load_metadata_from_file()

def write_metadata():
    global metadata
    with open('metadata.pickle', 'w') as f:
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

def check_subscription(subsciber_name, topic_name):
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
    if subsciber_name in metadata['subscriptions']:
        if topic_name in metadata['subscriptions']['subsciber_name']:
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
        metadata['subscriptions'][subsciber_name] = []
    if topic_name not in metadata['subscriptions']['subsciber_name']:
        metadata['subscriptions']['subsciber_name'].append(topic_name)
        write_metadata()
    return metadata['subscriptions']['subsciber_name']
