import unittest
from fo_qbo.qba import QBAuth, QBAuth2

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
auth_code = 'L011550699860jDAzhROoFFDxGgyREF6Jf9sjmrxD1pgrS9rZY'

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
        qba.get_tokens_and_expiry(auth_code)
        self.assertIsNotNone(qba.session.access_token)
        self.assertIsNotNone(qba.session.refresh_token)

    # def test_sample_api_call(self):
    #     qba = QBAuth2(client_id, client_secret)
    #     qba.get_tokens_and_expiry(auth_code)
    #
    #     base_url = 'https://sandbox-quickbooks.api.intuit.com'
    #     url = '{0}/v3/company/{1}/companyinfo/{1}'.format(base_url, qba.session.realm_id)
    #     auth_header = 'Bearer {0}'.format(qba.session.access_token)
    #     headers = {
    #         'Authorization': auth_header,
    #         'Accept': 'application/json'
    #     }
    #     response = requests.get(url, headers=headers)
    #     self.assertEqual(response.status, 200)

if __name__ == "__main__":
    # tester = TestQBAuth2Setup()
    # tester.test_app_connection()
    tester = QBAuth2(client_id, client_secret)
    url = tester.get_authorize_url()
    print(url)
    url = input('paste the callback URL:')
    tester.handle_authorized_callback_url(url)
    tester.sample_call()
