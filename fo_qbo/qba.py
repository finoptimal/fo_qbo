"""
QBO Rest API Client

Copyright 2016-2022 FinOptimal, Inc. All rights reserved.
"""
import logging
from pprint import pformat

import requests

from intuitlib.client    import AuthClient
from intuitlib.enums     import Scopes
from intuitlib.migration import migrate
from io                  import StringIO
from urllib.parse        import urlparse, parse_qs

from finoptimal.logging import get_logger, get_file_logger, LoggedClass

logger = get_logger(__name__)
qbo_logger = get_file_logger('qbo')

CALLBACK_URL      = "http://a.b.com"


class QBAuth2(LoggedClass):
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

    @logger.timeit()
    def request(self, request_type, url, header_auth=True, realm='',
                verify=True, headers=None, data=None, **params):
        """
        We don't handle authorization until the session's first request happens.
        """
        self.establish_access()

        auth_header = f'Bearer {self.session.access_token}'
        _headers = {
            'Authorization': auth_header,
        }

        for key, val in headers.items():
            _headers[key] = val
            
        if self.vb > 19:
            self.print("QBA headers", _headers)

        resp = requests.request(method=request_type.upper(), url=url, headers=_headers, data=data, **params)

        self.debug(f'resp = {resp.status_code} {resp.reason}')
        self.debug(f'resp.json() = {pformat(resp.json())}')

        qbo_logger.debug(f"{resp.__hash__()} - {resp.status_code} {resp.reason} - "
                        f"{resp.request.method.ljust(4)} {resp.url}")

        if resp.status_code == 401:
            if not hasattr(self, "_attempts"):
                self._attempts  = 1
            else:
                self._attempts += 1

            if self._attempts > 3:
                raise Exception(resp.text)
                
            self.refresh()
            
        if self.vb > 10:
            self.print("response code:", resp.status_code)
            
        return resp

    def establish_access(self):
        if getattr(self, "_has_access", False):
            return
        
        if self.refresh_token is None:
            if self.vb < 8:
                if self.vb > 1:
                    self.print("Rerun with verbosity >= 8 to request access!")

                self._has_access = False
                return self._has_access

            self.oob()

        if self.access_token is None:
            if self.vb > 5:
                self.print(f"\nNo {self.realm_id} access_token available,",
                           "so attempting refresh using available refresh_token...")
                
            try:
                self.refresh()
            except Exception as exc:
                # self.print("\n Couldn't refresh access_token / refresh_token:", exc)
                self.refresh_failure = True
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
        self.print("Please send the user here to authorize this app to access ")
        self.print(" their QBO data:\n")
        self.print(self.authorize_url)

        authorized_callback_url     = None

        while not authorized_callback_url:
            authorized_callback_url = input(
                "\nPaste the entire callback URL back here (or ctrl-c):")

        self.handle_authorized_callback_url(authorized_callback_url)

    def handle_authorized_callback_url(self, url):
        tail               = url.split("?")[1].strip()
        params             = dict([tuple(param.split("=")) for param in tail.split("&")])
        resp               = self.get_tokens_and_expiry(params['code'])
        self.realm_id      = params["realmId"]
        # We definitely have a new refresh token...
        self.access_token  = self.session.access_token
        self.refresh_token = self.session.refresh_token

        if self.vb > 2:
            self.print(f"\nThis company's (realm) ID: {self.realm_id}")
            self.print(f"     new refresh token:", self.session.refresh_token)
            self.print(f"     new access token:", self.session.access_token, "\n")

        self.new_token         = True
        self.new_refresh_token = True

    def refresh(self):
        if self.vb > 2:
            self.print(f"\nRefreshing {self.realm_id}'s refresh and access tokens!")

        self.session.refresh()
        self.access_token  = self.session.access_token
        self.refresh_token = self.session.refresh_token

        if self.vb > 2:
            self.print("  Success!\n")

        self.new_token     = True

    def disconnect(self):
        self.print(f"Disconnecting {self.realm_id}'s access token!")
        resp = self.session.revoke(token=self.refresh_token)
        
    def __repr__(self):
        return "<QBAuth (Oauth Version 2)>"
