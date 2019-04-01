import unittest
from broker import metadata_manager
import os
import shutil
from broker import broker

subscriber1 = 'test_subuscriber_1'
subscriber2 = 'test_subuscriber_2'
topic1 = 'test_topic_1'
topic2 = 'test_topic_2'

class TestPublisherAPI(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        broker.app.testing = True
        cls.client = broker.app.test_client()
        if os.path.isfile(metadata_manager.metadata_file_name):
            print('Backing up original metadata file...')
            shutil.copyfile(metadata_manager.metadata_file_name, metadata_manager.metadata_file_name+ '.BAK')
            cls.backup = True
            os.remove(metadata_manager.metadata_file_name)
        else:
            cls.backup = False

    def test_add_topic(self):
        res = self.client.get('/')
        print(res.data)


        
    # def test_get_topic(self):
    #     metadata_manager.add_topic(topic1)
    #     t = metadata_manager.add_topic(topic2)
    #     t2 = metadata_manager.get_topics()
    #     self.assertListEqual(t, t2)

    @classmethod
    def tearDownClass(cls):
        if cls.backup:
            print('Removing backup ...')
            shutil.copyfile(metadata_manager.metadata_file_name+ '.BAK', metadata_manager.metadata_file_name)
            cls.backup = False
            os.remove(metadata_manager.metadata_file_name+ '.BAK')
        else:
            try:
                os.remove(metadata_manager.metadata_file_name)
            except:
                pass

if __name__ == '__main__':
    unittest.main()