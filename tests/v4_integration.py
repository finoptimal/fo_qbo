import unittest
from fo_qbo.qba import QBAuth, QBAuth2

class TestQBAuthSetup(unittest.TestCase):
    def test_init(self):
        consumer_key = None
        consumer_secret = None
        qba  = QBAuth(consumer_key, consumer_secret)
        self.assertIsInstance(qba, QBAuth)
        self.assertIsNone(qba.session)

class TestQBAuth2Setup(unittest.TestCase):
    def test_init(self):
        qba = QBAuth2(None, None)
        self.assertIsInstance(qba, QBAuth2)
        self.assertIsNone(qba.session)

    def test_app_connection(self):
        client_id = 'Q0sdekF9eJZd1KOyowv5RfVlNxqt00HqyXF5O0MRDEqvtQa5Ca'
        client_secret = 'zHThPl1sWXBjNU3Qt3zpbgqjGMAoMzxEExOfxRK2'
        qba = QBAuth2(client_id, client_secret)
