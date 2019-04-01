import unittest
from broker import metadata_manager
import os
import shutil

subscriber1 = 'test_subuscriber_1'
subscriber2 = 'test_subuscriber_2'
topic1 = 'test_topic_1'
topic2 = 'test_topic_2'

class TestTopics(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if os.path.isfile(metadata_manager.metadata_file_name):
            print('Backing up original metadata file...')
            shutil.copyfile(metadata_manager.metadata_file_name, metadata_manager.metadata_file_name+ '.BAK')
            cls.backup = True
            os.remove(metadata_manager.metadata_file_name)
        else:
            cls.backup = False
        metadata_manager.load_metadata_from_file()

    def test_add_topic(self):
        t = metadata_manager.add_topic(topic1)
        self.assertListEqual(t, [topic1])
        t = metadata_manager.add_topic(topic2)
        self.assertListEqual(t, [topic1, topic2])

    def test_get_topic(self):
        metadata_manager.add_topic(topic1)
        t = metadata_manager.add_topic(topic2)
        t2 = metadata_manager.get_topics()
        self.assertListEqual(t, t2)
    
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


class TestSubscriptions(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if os.path.isfile(metadata_manager.metadata_file_name):
            print('Backing up original metadata file...')
            shutil.copyfile(metadata_manager.metadata_file_name, metadata_manager.metadata_file_name+ '.BAK')
            cls.backup = True
            os.remove(metadata_manager.metadata_file_name)
        else:
            cls.backup = False
        metadata_manager.load_metadata_from_file()

    def test_add_subscription(self):
        s = metadata_manager.add_subscription(subscriber1, topic1)
        self.assertListEqual(s, [topic1])
        s = metadata_manager.add_subscription(subscriber1, topic2)
        self.assertListEqual(s, [topic1, topic2])
        s = metadata_manager.add_subscription(subscriber2, topic2)
        self.assertListEqual(s, [topic2])


    def test_check_subscription(self):
        s = metadata_manager.add_subscription(subscriber1, topic1)
        s = metadata_manager.add_subscription(subscriber1, topic2)
        s = metadata_manager.add_subscription(subscriber2, topic2)
        self.assertTrue(metadata_manager.check_subscription(subscriber1, topic1))
        self.assertTrue(metadata_manager.check_subscription(subscriber1, topic2))
        self.assertTrue(metadata_manager.check_subscription(subscriber2, topic2))
        self.assertFalse(metadata_manager.check_subscription(subscriber2, topic1))
    
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

if __name__ == '__main__':
    unittest.main()