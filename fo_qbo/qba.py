"""
QBO Rest API Client

Copyright 2016-2022 FinOptimal, Inc. All rights reserved.
"""
import datetime
import os
import json
import requests
import sys
from typing import Union, Optional

from intuitlib.client import AuthClient
from intuitlib.enums import Scopes

from finoptimal.logging import get_logger, LoggedClass, void, returns
from finoptimal.storage.fo_darkonim_bucket import FODarkonimBucket
from finoptimal.utilities import retry

import google.cloud.logging as logging_gcp

logger = get_logger(__name__)

client = logging_gcp.Client()
api_logger = client.logger('api-qbo')
token_logger = client.logger('tokens-qbo')

CALLBACK_URL      = "http://a.b.com"


class QBAuth2(LoggedClass):
    """Facilitates interaction with the QBO API at the lowest level.

    Parameters
    ----------
    client_code : str
    modifier : str, optional
    verbosity : int,
    env : str, optional

    """

    SCOPES = [
        Scopes.ACCOUNTING,
        Scopes.ADDRESS,
        Scopes.EMAIL,
        Scopes.OPENID,
        Scopes.PHONE,
        Scopes.PROFILE
    ]

    # Keys for the client-specific credentials saved in Google Cloud
    CLIENT_CREDENTIAL_KEYS = [
        'access_token',
        'company_id',
        'expires_at',
        'refresh_token',
        'rt_acquired_at'
    ]

    MINOR_API_VERSION = 65

    def __init__(self, client_code: str, modifier: Optional[str] = None, verbosity: int = 0, env: Optional[str] = None):
        super().__init__()
        # Bind relevant arguments
        self.client_code = client_code
        self.vb = verbosity
        self.environment = "sandbox" if env and env == "sandbox" else "production"

        # We never start out with new tokens, they have to be retrieved from the API
        self.new_token = False
        self.new_refresh_token = False

        # Set up the Google cloud bucket that stores the credentials
        self._sub_client_code = None
        self.fo_darkonim = FODarkonimBucket(
            client_code=self.client_code,
            service_name=f'qbo_{modifier}'.replace('_None', '')  # Simply 'qbo' if modifier is None
        )
        self._business_context = 'saas' if self.fo_darkonim.saas else 'service'

        # Grab credentials and set the instance attributes used to initialize the session
        self._refresh_credential_attributes()

        # Initialize the session, using the saved credentials (if any)
        self.session = AuthClient(
            self.client_id,
            self.client_secret,
            self.callback_url,
            self.environment,
            refresh_token=self.refresh_token,
            access_token=self.initial_access_token,
            realm_id=self.realm_id
        )

        if self.realm_id is None:
            self.establish_access()
            self.save_new_tokens()
            self._refresh_credential_attributes()

        self._login()

    @property
    def initial_access_token(self) -> Union[str, None]:
        """str or None: The access token used when initializing AuthClient.

        None is returned if the access token has expired. I don't know the ramifications of NOT doing it like this, so
        for now I will match the legacy workflow.
        """
        if not hasattr(self, '_initial_access_token'):
            self._initial_access_token = self.access_token

            if self.expires_at < str(datetime.datetime.utcnow()):
                self._initial_access_token = None
                self.info(f"\n{self.realm_id}'s access_token has expired; not passing to AuthClient")

        return self._initial_access_token

    @property
    def business_context(self) -> str:
        """str: 'service' for service business client, 'saas' for SaaS client."""
        return self._business_context

    @property
    def credentials(self) -> dict:
        """dict: The QBO credentials (client + service account) from the Google Cloud bucket."""
        if not hasattr(self, '_credentials'):
            self._credentials = self._get_credentials()

        return self._credentials

    @credentials.setter
    def credentials(self, credentials: dict) -> None:
        # This will NOT update the service account credentials
        self._update_credentials(credentials)

    @credentials.deleter
    def credentials(self) -> None:
        del self._credentials

    @property
    def logged_in(self) -> bool:
        """bool: This means, at the very least, that we had a refresh token upon instantiation."""
        return self._logged_in

    @property
    def sub_client_code(self) -> Union[str, None]:
        """
        str or None: The substitute client code.

        A substitute client code, though related to the underlying entity, differs from `self.client_code`. The
        substitute refers to a separate QBO instance that `self.client_code` will utilize (e.g. excel -> excel2).
        """
        return self._sub_client_code

    @property
    def client_id(self) -> str:
        """str: The client id, which is used to authenticate our app."""
        return self._client_id

    @property
    def client_secret(self) -> str:
        """str: The client secret, which is used to authenticate our app."""
        return self._client_secret

    @property
    def callback_url(self) -> str:
        """str: The callback URL for OAuth."""
        return self._callback_url

    @property
    def access_token(self) -> str:
        """str: The access token, which expires every hour."""
        return self._access_token

    @property
    def refresh_token(self) -> str:
        """
        str: The refresh token, which is used to refresh the access token when it expires.
        Refresh tokens may change, too.
        """
        return self._refresh_token

    @property
    def realm_id(self) -> str:
        """str: The client's realm id."""
        return self._realm_id

    @property
    def expires_at(self) -> str:
        """str: The timestamp of when the access_token expires."""
        return self._expires_at

    @property
    def rt_acquired_at(self) -> str:
        """str: The timestamp of when the refresh token was acquired."""
        return self._rt_acquired_at

    @property
    def minor_api_version(self) -> int:
        """int: The minor API version the client is using."""
        return self._minor_api_version

    @property
    def caller(self) -> Union[str, None]:
        """The name of the process responsible for this instance."""
        if not hasattr(self, '_caller'):
            try:
                self._caller = os.path.split(sys.argv[0])[-1]
            except Exception:
                self._caller = None

        return self._caller

    def _get_credentials(self) -> dict:
        """Returns the credentials (client + service account) from the Google Cloud bucket."""
        credentials = self.fo_darkonim.credentials.copy()

        if self.fo_darkonim.saas:
            # Remove SaaS keys not used by AuthClient
            credentials = {k: v for k, v in credentials.items() if k not in ['redirect_uri', 'environment', 'base_url']}
        else:
            self._sub_client_code = credentials.get('substitute_client_code')

            if self._sub_client_code:
                self.fo_darkonim.client_code = self._sub_client_code
                credentials = self.fo_darkonim.credentials.copy()

        return credentials

    def _refresh_credential_attributes(self) -> None:
        """Use `self.credentials` to update the associated instance attributes."""
        # From service account
        self._client_id = self.credentials.get('client_id')
        self._client_secret = self.credentials.get('client_secret')
        self._callback_url = self.credentials.get('callback_url')

        # From client
        self._access_token = self.credentials.get('access_token')
        self._refresh_token = self.credentials.get('refresh_token')
        self._realm_id = self.credentials.get('company_id')
        self._expires_at = self.credentials.get('expires_at')
        self._rt_acquired_at = self.credentials.get('rt_acquired_at')
        self._minor_api_version = self.credentials.get('minor_api_version')
        self._minor_api_version = self._minor_api_version if self._minor_api_version else self.MINOR_API_VERSION

    def _update_credentials(self, credentials: dict) -> None:
        """Update the credentials (client only) in Google Cloud.

        Parameters
        ----------
        credentials : dict
            The updated credentials.
        """
        client_credentials = self.fo_darkonim.client_credentials.copy()
        client_credentials = {k: v for k, v in client_credentials.items() if k in self.CLIENT_CREDENTIAL_KEYS}
        client_credentials.update(credentials)
        self.fo_darkonim.client_credentials = client_credentials

    def _delete_credentials(self) -> None:
        """Deletes the client's credentials from the Google Cloud bucket."""
        self.fo_darkonim.delete_client_credentials()

    def _login(self) -> bool:
        """
        This is called every time this class gets instantiated and aims to support service business connections. It is
        similar to self.establish_access(), but not identical. At some point we should refactor a lot of redundancy out
        of these processes.
        """
        if not self.refresh_token:
            self._logged_in = False

            if self.vb < 8:
                self.info(f'\nRerun with verbosity >= 8 to request {self.client_code} QBO token\n')

            else:
                request_token = input(f'Request OAuth token for {self.client_code} QBO session (y/n)?')

                if request_token.lower().startswith('y'):
                    self.oob(callback_url=self.callback_url)

                    if self.new_token and self.new_refresh_token:
                        self.save_new_tokens()
                        self._logged_in = True
                        self.info(f'New {self.client_code} tokens saved successfully')

        else:
            self._logged_in = True

        return self._logged_in

    def save_new_tokens(self) -> bool:
        """Returns True if new token(s) were saved to the Google Cloud bucket."""
        if not self.new_token:
            return False

        self._expires_at = str(datetime.datetime.utcnow() + datetime.timedelta(minutes=55))

        new_credentials = {
            'access_token': self.access_token,
            'refresh_token': self.refresh_token,
            'expires_at': self.expires_at,
            'company_id': self.realm_id
        }

        self.info(f'New credentials: {new_credentials}')

        if self.new_refresh_token:
            new_credentials['rt_acquired_at'] = str(datetime.datetime.utcnow())

        self.credentials = new_credentials
        self.new_token = False
        self.new_refresh_token = False
        return True

    def reload_credentials(self) -> None:
        """Reload credentials from the Google Cloud bucket and reset the related attributes."""
        del self.credentials
        self._refresh_credential_attributes()

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

        msg = f'Making {request_type.upper()} request to {url}'

        try:
            api_logger.log(
                msg,
                labels={
                    'client_code': self.client_code,
                    'context': self.business_context,
                    'caller': self.caller,
                    'method': request_type.upper(),
                    'url': url,
                    'realm_id': self.realm_id,
                    'data': json.dumps(data)[:5000]
                }
            )
        except Exception:
            self.exception()
            self.info(msg)

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
    def establish_access(self) -> None:
        """
        This is called at the beginning of every request in QBS. Though, after the first successful call, this method
        should exit immediately. If we don't start with a refresh and access token, we will attempt to get those here.
        """
        if getattr(self, "_has_access", False):
            return
        
        if self.refresh_token is None:
            if self.vb < 8:
                self.info("Rerun with verbosity >= 8 to request access!")
                self._has_access = False
                return

            self.oob()

        if self.access_token is None:
            self.info(f'\nNo {self.realm_id} access_token available, attempting to refresh using refresh_token...')
                
            try:
                self.refresh()
            except Exception:
                self.refresh_failure = True
                raise
        
        self._has_access = True

    @logger.timeit(**void)
    def oob(self, callback_url=CALLBACK_URL):
        """
        Out of Band solution adapted from QBAuth.
        """
        self.authorize_url = self.session.get_authorization_url(self.SCOPES)
        self.print("Please send the user here to authorize this app to access their QBO data:\n")
        self.print(self.authorize_url)

        authorized_callback_url = None

        while not authorized_callback_url:
            authorized_callback_url = input("\nPaste the entire callback URL back here (or ctrl-c):")

        self.handle_authorized_callback_url(authorized_callback_url)

    @logger.timeit(**void)
    def handle_authorized_callback_url(self, url):
        tail = url.split("?")[1].strip()
        params = dict([tuple(param.split("=")) for param in tail.split("&")])
        self.session.get_bearer_token(params['cold'])
        self._realm_id = params["realmId"]
        self._access_token = self.session.access_token
        self._refresh_token = self.session.refresh_token

        if self.vb > 2:
            self.print(f"\nThis company's (realm) ID: {self.realm_id}")
            self.print(f"\tnew refresh token:", self.session.refresh_token)
            self.print(f"\tnew access token:", self.session.access_token, "\n")

        self.new_token         = True
        self.new_refresh_token = True

    def log_pending_token_event(self) -> None:
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

    def log_token_event_outcome(self) -> None:
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

    # @retry(max_tries=2, delay_secs=5)  # I think we have enough retries baked in already...
    @logger.timeit(**void)
    def refresh(self):
        self.log_pending_token_event()

        self.session.refresh()
        self._access_token  = self.session.access_token
        self._refresh_token = self.session.refresh_token

        self.log_token_event_outcome()

        self.new_token = True

    @logger.timeit(**void)
    def disconnect(self):
        self.print(f"Disconnecting {self.realm_id}'s access token!")
        resp = self.session.revoke(token=self.refresh_token)
        self.print(resp)
        self._logged_in = False
        self._delete_credentials()
        
    def __repr__(self):
        return "<QBAuth (Oauth Version 2)>"
