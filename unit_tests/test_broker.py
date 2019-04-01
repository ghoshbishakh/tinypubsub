import unittest
from broker import metadata_manager
import os
import shutil
from broker import broker
import json

storage_dir = './test_storage'
subscriber1 = 'test_subuscriber_1'
subscriber2 = 'test_subuscriber_2'
topic1 = 'test_topic_1'
topic2 = 'test_topic_2'
topic3 = 'test_topic_3'

class TestAPI(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        broker.app.testing = True
        broker.storage_dir = storage_dir
        cls.client = broker.app.test_client()

        # backup metadata if exists
        if os.path.isfile(metadata_manager.metadata_file_name):
            print('Backing up original metadata file...')
            shutil.copyfile(metadata_manager.metadata_file_name, metadata_manager.metadata_file_name+ '.BAK')
            cls.backup = True
            os.remove(metadata_manager.metadata_file_name)
        else:
            cls.backup = False
        metadata_manager.load_metadata_from_file()

    def test_basic_request(self):
        res = self.client.get('/')
        self.assertEqual(res.status_code, 200)

    def test_createtopic_view(self):
        # Create topic
        data = {'pub_name':subscriber1, 'topic_name':topic1}
        data = json.dumps(data)
        res = self.client.post('/createtopic', data=data)
        # print(res.data)
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.data.decode(), 'Topic added successfully: ' + topic1)
        
        # Create duplicate topic
        data = {'pub_name':subscriber1, 'topic_name':topic1}
        data = json.dumps(data)
        res = self.client.post('/createtopic', data=data)
        # print(res.data)
        self.assertEqual(res.status_code, 404)
        self.assertEqual(res.data.decode(), 'Topic already exists')

        # Create another topic
        data = {'pub_name':subscriber2, 'topic_name':topic2}
        data = json.dumps(data)
        res = self.client.post('/createtopic', data=data)
        # print(res.data)
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.data.decode(), 'Topic added successfully: ' + topic2)


    def test_publish_view(self):
        # Create topic
        data = {'pub_name':subscriber1, 'topic_name':topic3}
        data = json.dumps(data)
        res = self.client.post('/createtopic', data=data)
        # print(res.data)
        self.assertEqual(res.status_code, 200)

        data = ['123', '12345', '1234567']
        data = json.dumps(data)
        res = self.client.post('/publish/' + topic3, data=data)
        # print(res.data)
        self.assertEqual(res.status_code, 200)


    @classmethod
    def tearDownClass(cls):
        if cls.backup:
            print('Removing backup ...')
            shutil.copyfile(metadata_manager.metadata_file_name+ '.BAK', metadata_manager.metadata_file_name)
            cls.backup = False
            os.remove(metadata_manager.metadata_file_name+ '.BAK')
        else:
            print('Removing metadata file...')
            os.remove(metadata_manager.metadata_file_name)
        try:
            shutil.rmtree(storage_dir)
            pass
        except:
            pass

if __name__ == '__main__':
    unittest.main()