from __future__ import print_function
from future import standard_library
standard_library.install_aliases()
from builtins import input
from builtins import str
from builtins import object
from rauth import OAuth1Service, OAuth1Session
from io import StringIO
import datetime, json, time

# Intuit OAuth Service URLs
REQUEST_TOKEN_URL = "https://oauth.intuit.com/oauth/v1/get_request_token"
ACCESS_TOKEN_URL  = "https://oauth.intuit.com/oauth/v1/get_access_token"
AUTHORIZE_URL     = "https://appcenter.intuit.com/Connect/Begin"

RECONNECT_URL     = "https://appcenter.intuit.com/api/v1/connection/reconnect"
DISCONNECT_URL    = "https://appcenter.intuit.com/api/v1/connection/disconnect"

CALLBACK_URL      = "http://a.b.com"

RENEW_WINDOW_DAYS = 30

class QBAuth(object):
    """
    If you pass in all of the first five arguments,  
    """
    def __init__(self, consumer_key, consumer_secret, access_token=None,
                 access_token_secret=None, expires_on=None, callback_url=None,
                 oauth_token=None, oauth_token_secret=None, verbosity=0):
        """
        What are we working with? We need to know that before setup begins.

        """
        self.consumer_key        = consumer_key
        self.consumer_secret     = consumer_secret
        self.access_token        = access_token
        self.access_token_secret = access_token_secret
        self.expires_on          = expires_on
        self.callback_url        = callback_url
        self.oauth_token         = access_token
        self.oauth_token_secret  = access_token_secret
        self.vb                  = verbosity

        # lets instantiator know to store new persistent data (if applicable)
        self.new_token           = False     
        
        self.session = None  # until setup is complete

        self._setup()

    def _setup(self):
        """
        Figure out if any action is required or if we can just create the
         OAuth1Session object without further ado.
        """
        if not self.access_token or not self.access_token_secret:
            if self.vb > 1:
                print("Need access_token and access_token_secret!")
            return
        
        # Make sure the token isn't within the reconnect window. If it is,
        #  reconnect, otherwise, do nothing else.
        if self.expires_on and self.time_to_renew:
            return self._reconnect()
                
        self.session = OAuth1Session(
            self.consumer_key, self.consumer_secret, self.access_token,
            self.access_token_secret)
        return

    def oob(self, callback_url=CALLBACK_URL):
        """
        Out of Band solution.
        """
        self.request_token, self.request_token_secret, self.authorize_url = \
                self.get_authorize_url()
            
        print("Please send the user here to authorize this app to access ")
        print(" their QBO data:\n")
        print(self.authorize_url)
        authorized_callback_url = None
        while not authorized_callback_url:
            authorized_callback_url = input(
                "\nPaste the entire callback URL back here (or ctrl-c):")
                
        tail = authorized_callback_url.split("?")[1].strip()

        params = dict([ tuple(param.split("=")) for param in tail.split("&") ])

        access_token, access_token_secret = \
            self.get_access_token_response(
                params['oauth_token'], params['oauth_verifier'])
        self.company_id                   = params["realmId"]
        print("This company's (realm) ID: {}".format(self.company_id))

        self._set_access_token(access_token, access_token_secret)
            
    def _set_access_token(self, access_token, access_token_secret):
        # In case of access token retrieval after authorization or reconnect
        self.access_token        = access_token
        self.access_token_secret = access_token_secret
        if self.vb > 1:
            print("New access token and secret set. Store these things!")
        self.new_token           = True
        self.expires_on          = str(
            datetime.datetime.now().date() + datetime.timedelta(days=180))

        self._setup()

    def get_authorize_url(self):

        # Begin authorization process.
        # To be used when no access token/secret is supplied
        qbService = OAuth1Service(
            name="quickbooks-wrapper",
            consumer_key=self.consumer_key,
            consumer_secret=self.consumer_secret,
            request_token_url=REQUEST_TOKEN_URL,
            access_token_url=ACCESS_TOKEN_URL,
            authorize_url=AUTHORIZE_URL,
            base_url=None)

        cbu = self.callback_url
        if not cbu:
            cbu = CALLBACK_URL

        try:
            # We will need self.request_token and
            #  self.request_token_secret later to exchange for an
            #  access_token_secret
            request_token, request_token_secret = \
                qbService.get_request_token(params = { 'oauth_callback' : cbu })
        
            # User should be redirected here to authorize
            # Access token will be sent to callback url to be processed
            #  by rest of workflow
            authorize_url = qbService.get_authorize_url(request_token)
        except:
            raise

        return request_token, request_token_secret, authorize_url

    def get_access_token_response(self, oauth_token, oauth_verifier):
        """
        Use self.request_token and self.request_token_secret (from before) and
         one or both of the two passed parameters (but at least the
         oauth_verifier) to get an access token and an access token secret.

        https://oauth.intuit.com/oauth/v1/get_access_token
        """

        if self.request_token is None or self.request_token_secret is None:
            raise Exception("Request token and secret required for " \
                    "access token retrieval")
        
        qbService = OAuth1Service(
            name="quickbooks-wrapper",
            consumer_key=self.consumer_key,
            consumer_secret=self.consumer_secret,
            access_token_url=ACCESS_TOKEN_URL,
            base_url=None)

        access_token, access_token_secret = \
            qbService.get_access_token(self.request_token, 
                                       self.request_token_secret,
                                       params = { 'oauth_token': oauth_token, 
                                           'oauth_verifier': oauth_verifier })

        return access_token, access_token_secret

    @property
    def time_to_renew(self):
        """
        Figure out if we're inside the 30 day window preceding the expiration
         date of this access token, returning True if so
        """
        if not isinstance(self.expires_on, str):
            #standardize; a string is how the date will likely
            #  be stored (in a json file at least)
            if isinstance(self.expires_on, datetime.datetime):
                self.expires_on = self.expires_on.date()
            self.expires_on = str(self.expires_on)

        test_date = datetime.datetime.strptime(
            self.expires_on, "%Y-%m-%d").date()
        if (test_date - datetime.date.today()).days <= RENEW_WINDOW_DAYS:
            return True

        return False
    
    def _reconnect(self):
        if self.access_token is None or self.access_token_secret is None:
            raise Exception(
                "Access token and access token secret are required!")
        
        try:
            qbSession = OAuth1Session(
                    self.consumer_key, self.consumer_secret,
                    self.access_token, self.access_token_secret)
            resp      = qbSession.get(RECONNECT_URL,
                    params = { 'format' : 'json' })
            if resp.status_code >= 400:
                raise Exception("Request failed with status %s (%s)" % 
                                (resp.status_code, resp.text))
        except:
            import traceback;traceback.print_exc()
            if self.vb > 1:
                import ipdb;ipdb.set_trace()
            raise

        rj = resp.json()

        if rj['ErrorCode'] > 0:
            print(json.dumps(rj, indent=4))
            raise Exception("Reconnect failed with code %s (%s)" %
                (rj['ErrorCode'], rj['ErrorMessage']))

        access_token        = rj["OAuthToken"]
        access_token_secret = rj["OAuthTokenSecret"]

        self._set_access_token(access_token, access_token_secret)

    def disconnect(self):
        if self.access_token is None or self.access_token_secret is None:
            raise Exception(
                "Access token and access token secret are required!")

        try:
            qbSession = OAuth1Session(
                    self.consumer_key, self.consumer_secret,
                    self.access_token, self.access_token_secret)
            resp      = qbSession.get(DISCONNECT_URL, 
                    params = { 'format': 'json' })
            if resp.status_code >= 400:
                raise Exception("Request failed with status %s (%s)" % 
                                (resp.status_code, resp.text))
        except:
            raise

        if resp.json()['ErrorCode'] > 0:
            print(jsond.dumps(resp.json(), indent=4))
            raise Exception("Reconnect failed with code %s (%s)" %
                (resp.json()['ErrorCode'], resp.json()['ErrorMessage']))
