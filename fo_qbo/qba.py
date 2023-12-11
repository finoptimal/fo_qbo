"""
QBO Rest API Client

Copyright 2016-2022 FinOptimal, Inc. All rights reserved.
"""
import os
import requests
import sys
from typing import Union

from intuitlib.client import AuthClient
from intuitlib.enums import Scopes

from finoptimal.logging import get_logger, LoggedClass, void, returns
from finoptimal.utilities import retry

import google.cloud.logging as logging_gcp

logger = get_logger(__name__)

client = logging_gcp.Client()
api_logger = client.logger('api-qbo')
token_logger = client.logger('tokens-qbo')

CALLBACK_URL      = "http://a.b.com"


class QBAuth2(LoggedClass):    
    def __init__(self, client_id, client_secret,  realm_id=None,
                 refresh_token=None, access_token=None,
                 callback_url=CALLBACK_URL, verbosity=0, env=None,
                 client_code=None, business_context=None):
        super().__init__()
        self.client_id           = client_id
        self.client_secret       = client_secret
        self.refresh_token       = refresh_token
        self.access_token        = access_token
        self.realm_id            = realm_id
        self.vb                  = verbosity
        self.environment         = "sandbox" if env and env == "sandbox" else "production"

        self.session             = None
        self.new_token           = False
        self.new_refresh_token   = False
        self.callback_url        = callback_url
        self.client_code         = client_code
        self.business_context    = business_context

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

    @property
    def caller(self) -> Union[str, None]:
        """The name of the process responsible for this instance."""
        if not hasattr(self, '_caller'):
            try:
                self._caller = os.path.split(sys.argv[0])[-1]
            except Exception:
                self._caller = None

        return self._caller

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

    @logger.timeit(**returns, expand=True)
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
        status_code = str(resp.status_code)
        method = str(resp.request.method.ljust(4))
        reason = str(resp.reason)
        response_url = str(resp.url)
        
        try:
            msg = (f"{resp.__hash__()} - {self.caller} - {self.client_code}({self.business_context}) - "
                   f"{status_code} {reason} - {method} {response_url} - {resp.json()}")
        except Exception as ex:
            msg = (f"{resp.__hash__()} - {self.caller} - {self.client_code}({self.business_context}) - "
                   f"{status_code} {reason} - {method} {response_url} - None")

        try:
            api_logger.log(
                msg[:5000],
                labels={
                    'client_code': self.client_code,
                    'context': self.business_context,
                    'caller': self.caller,
                    'method': method,
                    'status_code': status_code,
                    'reason': reason,
                    'url': response_url,
                    'realm_id': self.realm_id
                }
            )
        except Exception:
            self.exception()
            self.info(msg)

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

    @logger.timeit(**void)
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
    @logger.timeit(**returns)
    def get_authorize_url(self):
        url = self.session.get_authorization_url(self.SCOPES)
        return url

    def get_tokens_and_expiry(self, auth_code):
        return self.session.get_bearer_token(auth_code)

    @logger.timeit(**void)
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

    @logger.timeit(**void)
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

    @retry(delay_secs=30)
    @logger.timeit(**void)
    def refresh(self):
        # if self.vb > 2:
        self.info(f"\nRefreshing {self.realm_id}'s refresh and access tokens!")

        try:
            token_logger.log(
                f"Refreshing {self.realm_id}'s refresh and access tokens!",
                labels={
                    'context': self.business_context,
                    'client_code': self.client_code,
                    'caller': self.caller,
                    'realm_id': self.realm_id
                }
            )
        except Exception:
            self.exception()

        self.session.refresh()
        self.access_token  = self.session.access_token
        self.refresh_token = self.session.refresh_token

        self.info(f'New access token for realm id {self.realm_id}: {self.access_token}')
        self.info(f'New refresh token for realm id {self.realm_id}: {self.refresh_token}')

        try:
            token_logger.log(
                f'New tokens for {self.realm_id}',
                labels={
                    'refresh_token': self.refresh_token,
                    'access_token': self.access_token,
                    'context': self.business_context,
                    'client_code': self.client_code,
                    'caller': self.caller,
                    'realm_id': self.realm_id
                },
            )
        except Exception:
            self.exception()

        if self.vb > 2:
            self.print("  Success!\n")

        self.new_token     = True

    @logger.timeit(**void)
    def disconnect(self):
        self.print(f"Disconnecting {self.realm_id}'s access token!")
        resp = self.session.revoke(token=self.refresh_token)
        
    def __repr__(self):
        return "<QBAuth (Oauth Version 2)>"
