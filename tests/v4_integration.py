import unittest
from fo_qbo.qba import QBAuth, QBAuth2
from fo_qbo.qbs import QBS

class TestQBAuthSetup(unittest.TestCase):
    def test_init(self):
        consumer_key = None
        consumer_secret = None
        qba  = QBAuth(consumer_key, consumer_secret)
        self.assertIsInstance(qba, QBAuth)
        self.assertIsNone(qba.session)

client_secret = 'zHThPl1sWXBjNU3Qt3zpbgqjGMAoMzxEExOfxRK2'
client_id = 'Q0sdekF9eJZd1KOyowv5RfVlNxqt00HqyXF5O0MRDEqvtQa5Ca'
# gotten from one-time browser permission per project, scope: accounting
refresh_token = 'Q011559851403hKb1KIYhxZWhnpgGrJsVGbtpBVj7RGSbqitTV'
realm_id = '123146326724784'
access_token = None

class TestQBAuth2Setup(unittest.TestCase):
    def test_init(self):
        qba = QBAuth2(None, None)
        self.assertIsInstance(qba, QBAuth2)
        self.assertIsNone(qba.session)

    def test_app_connection(self):
        # test app values
        qba = QBAuth2(client_id, client_secret)
        self.assertIsNotNone(qba.session)
        auth_url = qba.get_authorize_url()
        print(auth_url)
        self.assertIsInstance(auth_url, str)

    def test_tokens(self):
        qba = QBAuth2(client_id, client_secret)
        self.assertIsNone(qba.session.access_token)
        self.assertIsNone(qba.session.refresh_token)
        qba = QBAuth2(client_id, client_secret, refresh_token=refresh_token, realm_id=realm_id, access_token=access_token)
        self.assertIsNotNone(qba.session.refresh_token)

    def test_api_call(self):
        qba = QBAuth2(client_id, client_secret, refresh_token=refresh_token, realm_id=realm_id, access_token=access_token)
        r = qba.request()
        self.assertEqual(r.status_code, 401)
        qba.refresh()
        r = qba.request()
        self.assertEqual(r.status_code, 200)

class TestQBS(unittest.TestCase):
    def test_init(self):
        self.assertRaises(ValueError, QBS)
        qbs = QBS(client_id=client_id, client_secret=client_secret,
                  refresh_token=refresh_token, company_id=realm_id, verbosity=0)
        self.assertIsNotNone(qbs.qba)

    def test_api_call(self):
        qbs = QBS(client_id=client_id, client_secret=client_secret,
                  refresh_token=refresh_token, company_id=realm_id, verbosity=0)
        

if __name__ == "__main__":
    qba = QBAuth2(client_id, client_secret, refresh_token=refresh_token, realm_id=realm_id, access_token=access_token)
    r = qba.request()
    print(qba.session.access_token)
    print(r.status_code)
    qba.refresh()
    print(qba.session.access_token)
    r = qba.request()
    print(r.status_code)
