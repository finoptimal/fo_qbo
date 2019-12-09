from __future__ import print_function

import datetime, json, requests, time

from rauth               import OAuth1Service, OAuth1Session
from intuitlib.client    import AuthClient
from intuitlib.enums     import Scopes
from intuitlib.migration import migrate
from io                  import StringIO
from urllib.parse        import urlparse, parse_qs

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
        self.new_access_token    = False
        
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
        self.realm_id                     = params["realmId"]
        print("This company's (realm) ID: {}".format(self.realm_id))

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

        qs = OAuth1Service(
            name="quickbooks-wrapper",
            consumer_key=self.consumer_key,
            consumer_secret=self.consumer_secret,
            access_token_url=ACCESS_TOKEN_URL,
            base_url=None)

        access_token, access_token_secret = qs.get_access_token(self.request_token, self.request_token_secret, params = { 'oauth_token': oauth_token, 'oauth_verifier': oauth_verifier })

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

        print(f"Disconnecting access token {self.access_token}!") 
        
        qbSession = OAuth1Session(
            self.consumer_key, self.consumer_secret,
            self.access_token, self.access_token_secret)
        resp      = qbSession.get(DISCONNECT_URL,
                                  params = { 'format': 'json' })
        if resp.status_code >= 400:
            raise Exception("Disconnection request failed with status %s (%s)" %
                            (resp.status_code, resp.text))
        
        if resp.json()['ErrorCode'] > 0:
            print(json.dumps(resp.json(), indent=4))
            raise Exception("Reconnect failed with code %s (%s)" %
                (resp.json()['ErrorCode'], resp.json()['ErrorMessage']))

    def request(self, request_type, url, header_auth=True, realm=None,
                verify=True,
                headers='', data='', **params):
        resp = self.session.request(
            request_type.upper(), url, header_auth=True, realm=realm,
            verify=True, headers=headers, data=data, **params)
       
        return resp

    def __repr__(self):
        return "<QBAuth (Oauth Version 1)>"
        
class QBAuth2():
    def __init__(self, client_id, client_secret,  realm_id=None,
                 refresh_token=None, access_token=None,
                 callback_url=CALLBACK_URL, verbosity=0):
        self.client_id           = client_id
        self.client_secret       = client_secret
        self.refresh_token       = refresh_token
        self.access_token        = access_token
        self.realm_id            = realm_id
        self.vb                  = verbosity
        self.environment         = "production"

        self.session             = None
        self.new_token           = False
        self.new_refresh_token   = False
        self.callback_url        = callback_url

        self._setup()

    SCOPES = [
        Scopes.ACCOUNTING,
        Scopes.ADDRESS,
        Scopes.EMAIL,
        # Scopes.INTUIT_NAME,
        Scopes.OPENID,
        # Scopes.PAYMENT,
        # Scopes.PAYROLL,
        # Scopes.PAYROLL_TIMETRACKING,
        Scopes.PHONE,
        Scopes.PROFILE
    ]
        
    def _setup(self):
        if self.client_id is None or self.client_secret is None:
            raise Exception(
                "Need a client_id and client_secret to get started!")

        self.session = AuthClient(
            self.client_id,
            self.client_secret,
            self.callback_url,
            self.environment,
            refresh_token=self.refresh_token,
            access_token=self.access_token,
            realm_id=self.realm_id)

    def request(self, request_type, url, header_auth=True, realm='',
                verify=True, headers='', data='', **params):
        """
        We don't handle authorization until the session's first request happens.
        """
        self.establish_access()
        auth_header = 'Bearer {0}'.format(self.session.access_token)
        _headers = {
            'Authorization': auth_header,
        }
        for key,val in headers.items():
            _headers[key] = val
            
        if self.vb > 19:
            print("QBA headers", _headers)
            
        response = requests.request(
            request_type.upper(), url, headers=_headers, data=data, **params)
        
        if response.status_code == 401:
            self.refresh()
            
        if self.vb > 10:
            print("response code:", response.status_code)
            
        return response

    def establish_access(self):
        if getattr(self, "_has_access", False):
            return
        
        if self.refresh_token is None:
            if self.vb < 8:
                if self.vb > 1:
                    print("Rerun with verbosity >= 8 to request access!")
                self._has_access = False
                return self._has_access
            self.oob()

        if self.access_token is None:
            if self.vb > 5:
                print(f"\nNo {self.realm_id} access_token available,", 
                      "so attempting refresh using available refresh_token...")
                
            try:
                self.refresh()
            except Exception as exc:
                print("\n Couldn't refresh access_token / refresh_token:", exc) 
                raise

        
        self._has_access = True
            
    # the following functions correspond to those in the Intuit OAuth client
    # docs: https://oauth-pythonclient.readthedocs.io/en/latest/user-guide.html
    #  #authorize-your-app
    def get_authorize_url(self):
        url = self.session.get_authorization_url(self.SCOPES)
        return url

    def get_tokens_and_expiry(self, auth_code):
        return self.session.get_bearer_token(auth_code)

    def oob(self, callback_url=CALLBACK_URL):
        """
        Out of Band solution adapted from QBAuth.
        """
        self.authorize_url = self.get_authorize_url()
        print("Please send the user here to authorize this app to access ")
        print(" their QBO data:\n")
        print(self.authorize_url)
        authorized_callback_url     = None
        while not authorized_callback_url:
            authorized_callback_url = input(
                "\nPaste the entire callback URL back here (or ctrl-c):")
        self.handle_authorized_callback_url(authorized_callback_url)

    def handle_authorized_callback_url(self, url):
        tail               = url.split("?")[1].strip()
        params             = dict([
            tuple(param.split("=")) for param in tail.split("&") ])
        resp               = self.get_tokens_and_expiry(params['code'])
        self.realm_id      = params["realmId"]
        # We definitely have a new refresh token...
        self.access_token  = self.session.access_token
        self.refresh_token = self.session.refresh_token
        if self.vb > 2:
            print("\nThis company's (realm) ID: {}".format(self.realm_id))
            print("     new refresh token:", self.session.refresh_token)
            print("     new access token:", self.session.access_token, "\n")
        self.new_token         = True
        self.new_refresh_token = True

    def refresh(self):
        if self.vb > 2:
            print(f"\nRefreshing {self.realm_id}'s refresh and access tokens!") 
        self.session.refresh()
        self.access_token  = self.session.access_token
        self.refresh_token = self.session.refresh_token
        if self.vb > 2:
            print("  Success!\n")
        self.new_token     = True

    def disconnect(self):
        print(f"Disconnecting {self.realm_id}'s access token!") 
        resp = self.session.revoke(token=self.refresh_token)
        
    def __repr__(self):
        return "<QBAuth (Oauth Version 2)>"
