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
from defusedxml import ElementTree

from intuitlib.client import AuthClient
from intuitlib.exceptions import AuthClientError
from intuitlib.enums import Scopes
from google.cloud.logging import DESCENDING

from finoptimal.logging import get_logger, LoggedClass, void, returns, GoogleCloudLogger
from finoptimal.storage.fo_darkonim_bucket import FODarkonimBucket
from finoptimal.utilities import retry
from fo_qbo.errors import RateLimitError, UnauthorizedError


logger = get_logger(__name__)


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

    MINOR_API_VERSION = 70

    def __init__(self, client_code: str, modifier: Optional[str] = None, verbosity: int = 0, env: Optional[str] = None):
        super().__init__()
        # Bind relevant arguments
        self.client_code = client_code
        self.vb = verbosity
        self.environment = "sandbox" if env and env == "sandbox" else "production"

        # Set-up GCP loggers
        self.api_logger = GoogleCloudLogger('api-qbo')
        self.token_logger = GoogleCloudLogger('tokens-qbo')

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
        self.reset_auth_client_error_retry_count()

        if self.realm_id is None:
            # If we don't have a realm_id, we don't have credentials for this client.
            self.establish_access()
            self.save_new_tokens()
            self.reload_credentials()

        self._login()

        # Set logger contexts
        self.api_logger.context = self.logger_context
        self.token_logger.context = self.logger_context


    @property
    def cc(self):
        return self.client_code


    @property
    def auth_client_error_retry_count(self) -> int:
        return self._auth_client_error_retry_count


    def increment_auth_client_error_retry_count(self) -> None:
        self._auth_client_error_retry_count += 1


    def reset_auth_client_error_retry_count(self) -> None:
        self._auth_client_error_retry_count = 0


    @property
    def logger_context(self) -> dict:
        """dict: Labels that are attached to the logged records in GCP."""
        return {
            'client_code': self.client_code,
            'context': self.business_context,
            'caller': self.caller,
            'realm_id': self.realm_id
        }


    @property
    def initial_access_token(self) -> Union[str, None]:
        """str or None: The access token used when initializing AuthClient.

        None is returned if the access token has expired. I don't know the ramifications of NOT doing it like this, so
        for now I will match the legacy workflow.
        """
        if not hasattr(self, '_initial_access_token'):
            self._initial_access_token = self.access_token

            if self.expires_at and self.expires_at < str(datetime.datetime.utcnow()):
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
        if self.last_call_was_unauthorized == True:
            return False

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
    def expires_at(self) -> Union[str, None]:
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

    @minor_api_version.setter
    def minor_api_version(self, version_number):
        self._minor_api_version = version_number

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
        self.minor_api_version = self.credentials.get('minor_api_version')
        self.minor_api_version = self.minor_api_version if self.minor_api_version else self.MINOR_API_VERSION

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

    @property
    def active_credentials(self) -> dict:
        return {
            'access_token': self.access_token,
            'refresh_token': self.refresh_token,
            'expires_at': self.expires_at,
            'company_id': self.realm_id
        }

    def save_new_tokens(self) -> None:
        if self.new_token:
            self._expires_at = str(datetime.datetime.utcnow() + datetime.timedelta(minutes=55))

            new_credentials = self.active_credentials.copy()
            self.info(f'New credentials: {new_credentials}')

            if self.new_refresh_token:
                new_credentials['rt_acquired_at'] = str(datetime.datetime.utcnow())

            self.credentials = new_credentials

            self.new_token = False
            self.new_refresh_token = False

    def reload_credentials(self) -> None:
        """Reload credentials from the Google Cloud bucket and reset the related attributes."""
        del self.credentials
        self._refresh_credential_attributes()

    @retry(max_tries=4, exceptions=(UnauthorizedError,))
    @logger.timeit(**returns, expand=True)
    def request(self, request_type, url, header_auth=True, realm='', verify=True, headers=None, data=None, **params):
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

        self.api_logger.info(msg, method=request_type.upper(), url=url, data=json.dumps(data)[:5000])

        resp = requests.request(method=request_type.upper(), url=url, headers=_headers, data=data, **params)
        status_code = str(resp.status_code)
        method = str(resp.request.method.ljust(4))
        reason = str(resp.reason)
        response_url = str(resp.url)
        
        try:
            msg = (f"{resp.__hash__()} - {self.caller} - {self.client_code}({self.business_context}) - "
                   f"{status_code} {reason} - {method} {response_url} - {resp.json()}")

        except Exception:
            msg = (f"{resp.__hash__()} - {self.caller} - {self.client_code}({self.business_context}) - "
                   f"{status_code} {reason} - {method} {response_url} - None")

        self.api_logger.info(msg[:5000], method=method, status_code=status_code, reason=reason, url=response_url)

        if resp.status_code == 401:
            # Is this an xml error (instead of the expected JSON one)?
            try:
                et = ElementTree.fromstring(resp.text)
                is_xml = True

            except:
                is_xml = False

            if is_xml:
                try:
                    fault_type = et[0].get("type", "Unnamed 401 Fault")
                    code = et[0][0].get("code", "-1")
                    message = et[0][0][0].text
                    detail = et[0][0][1].text
                    fault_time = et.get("time")
                    parsed_xml = True
                    reason = f"{reason} // XML response with Error code {code} at {fault_time}: {detail}"

                except:
                    parsed_xml = False
                    reason = f"Unparsed XML response with reason {reason}"

            if self.vb > 1:
                print(resp.text)
                print("\n\nsee 401 resp.text above ^^\n")
                if self.vb > 4:
                    print("\n\n^^ Inspect 401 error more closely!? ^^")
                    import ipdb;ipdb.set_trace()
            self.refresh()
            self.api_logger.info(f'Retrying {method} request due to UnauthorizedError')
            self.last_call_was_unauthorized = True
            reason = f"{self.realm_id} realm error // {reason}"
            raise UnauthorizedError(f'{status_code} {reason}')

        self.last_call_was_unauthorized = False

        if resp.status_code == 429:
            raise RateLimitError(f'{status_code} {reason}')

        if self.vb > 10:
            self.print("response code:", resp.status_code)
            
        return resp

    @property
    def last_call_was_unauthorized(self):
        if not hasattr(self, "_last_call_was_unauthorized"):
            self.last_call_was_unauthorized = None

        return self._last_call_was_unauthorized

    @last_call_was_unauthorized.setter
    def last_call_was_unauthorized(self, was_unauthed):
        self._last_call_was_unauthorized = was_unauthed

    @logger.timeit(**void)
    def establish_access(self) -> None:
        """
        This is called at the beginning of every request. Looks to me like this was simply meant to establish the
        initial connection, whether that be an access_token refresh or oob().
        """
        if getattr(self, "_has_access", False):
            return
        
        if self.refresh_token is None:
            if self.vb < 8:
                self.info("Rerun with verbosity >= 8 to request access!")
                self._has_access = False
                return

            # If executed successfully, this will save the access and refresh tokens to the session/instance. We aren't
            # saving the credentials to GCP immediately, though, that happens in QBS... I think we can structure this
            # much better. TODO
            self.oob()

        if self.access_token is None:
            self.info(f'\nNo {self.realm_id} access_token available, attempting to refresh using refresh_token...')
                
            try:
                self.refresh()

            except Exception:
                # Now we expect refresh() to handle ALL AuthClientErrors internally, so we WILL raise an exception here.
                raise
        
        self._has_access = True

    @logger.timeit(**void)
    def oob(self):
        """
        Out of Band solution adapted from QBAuth.
        """
        self.authorize_url = self.session.get_authorization_url(self.SCOPES)
        self.print(f"({self}.vb = {self.vb})")
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
        self.session.get_bearer_token(params['code'])
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
        self.token_logger.info(f"Refreshing {self.realm_id}'s refresh and access tokens!")

    def log_token_event_outcome(self) -> None:
        self.info(f'New access token for realm id {self.realm_id}: {self.access_token}')
        self.info(f'New refresh token for realm id {self.realm_id}: {self.refresh_token}')

        self.token_logger.info(
            f'New tokens for {self.realm_id}',
            refresh_token=self.refresh_token,
            access_token=self.access_token,
        )

    @property
    def fixed_from_gcp_logs_text_payload(self):
        return f'Fixed {self.client_code} AuthClientError using GCP logs'

    def log_token_fix(self, from_log: bool) -> None:
        if from_log:
            msg = self.fixed_from_gcp_logs_text_payload

        else:
            msg = f'Fixed {self.client_code} AuthClientError using GCP bucket'

        self.info(msg)

        self.token_logger.info(msg, refresh_token=self.refresh_token, access_token=self.access_token)

    @retry(max_tries=3, delay_secs=5, exceptions=(AuthClientError, ))
    @logger.timeit(**void)
    def refresh(self) -> None:
        # I am adding this as defence against the irrational AuthClientErrors that Intuit throws from time to time,
        # which leads to excessive token exchanges. If we hit this condition there is a good chance Intuit threw a
        # false positive.
        if (
            self.auth_client_error_retry_count == 0 and
            self.expires_at and
            self.expires_at >= str(datetime.datetime.utcnow())
        ):
            self.increment_auth_client_error_retry_count()
            self.token_logger.info('Potential false positive AuthClientError detected')
            return

        # TODO: I think some more refactoring needs to be done to ensure that competing processes attempt to use the
        # same credentials rather than creating new ones.

        self.log_pending_token_event()

        try:
            self.session.refresh()

        except AuthClientError:
            self.last_call_was_unauthorized = True
            self.exception()

            self.token_logger.info(
                f'AuthClientError handling attempt {self.auth_client_error_retry_count}',
                attempt_number=str(self.auth_client_error_retry_count)
            )

            if self.fixed_by_reloading_credentials():
                # Both instance and session attributes were updated, no need to update GCP
                self.log_token_fix(from_log=False)

            elif self.fixed_by_loading_from_log():
                # Instance and session and GCP attributes were updated
                self.log_token_fix(from_log=True)

            else:
                self.increment_auth_client_error_retry_count()
                raise

            self.reset_auth_client_error_retry_count()
            return

        else:
            self.last_call_was_unauthorized = False
            self.reset_auth_client_error_retry_count()
            self.new_token = True
            self._access_token  = self.session.access_token
            self._refresh_token = self.session.refresh_token
            self.save_new_tokens()
            self.log_token_event_outcome()

    @logger.timeit(**void)
    def disconnect(self):
        self.print(f"Disconnecting {self.realm_id}'s access token!")
        try:
            self.session.revoke(token=self.refresh_token)
        except AuthClientError as e:
            if e.status_code == 400:
                pass
            else:
                raise

        self._logged_in = False
        self._delete_credentials()

    @retry(max_tries=3, delay_secs=0.5, drag_factor=2)
    def get_token_log_entries(self) -> list:
        lookback_period = (datetime.datetime.utcnow() - datetime.timedelta(days=30)).isoformat().split('.')[0] + 'Z'

        filters = ' AND '.join([
            f'labels.client_code="{self.client_code}"',
            'labels.access_token!= null',
            f'timestamp>"{lookback_period}"',
            f'text_payload!="{self.fixed_from_gcp_logs_text_payload}"', # Ignore the FIX entries!!!
        ])

        log_entries = self.token_logger.list_entries(filter_=filters, order_by=DESCENDING, max_results=3)

        return [entry for entry in log_entries]

    def get_latest_tokens_from_log(self) -> dict:
        """Get the LATEST one, not just the last; sometimes with race conditions we pull the wrong one!"""
        tokens = {
            'access_token': None,
            'refresh_token': None,
            'expires_at': None
        }

        try:
            entries = self.get_token_log_entries()

        except Exception:
            entries = []

        if len(entries) < 1:
            return tokens

        latest_entry = entries[0]
        tokens['access_token'] = latest_entry.labels.get('access_token')
        tokens['refresh_token'] = latest_entry.labels.get('refresh_token')
        tokens['expires_at'] = str(latest_entry.timestamp + datetime.timedelta(minutes=58)).split('+')[0]

        if self.vb > 4:
            print(json.dumps(tokens, indent=4))
            print("Getting the LATEST successfully-exchanged token (excluding fixes, per TM-1496))")
            import ipdb;ipdb.set_trace()

        return tokens

    def fixed_by_reloading_credentials(self) -> bool:
        """Returns True if the AuthClientError was fixed by reloading credentials from Google Cloud."""
        fixed = False
        # Get a reference to the current value so that we can compare after we reload the credentials
        expires_at = self.expires_at

        self.reload_credentials()

        if not self.expires_at:
            # We must not have any credentials, so we can't fix here
            return False

        if (expires_at and expires_at < self.expires_at) or (not expires_at):
            # Either we are able to compare the access token expiration dates, or we assume we can fix the problem
            # because we didn't have an expiration before and now we do.
            for token in ['access_token', 'refresh_token']:
                if getattr(self, token) != getattr(self.session, token):
                    setattr(self.session, token, getattr(self, token))
                    fixed = True

        return fixed

    def fixed_by_loading_from_log(self) -> bool:
        fixed = False
        token_dict = self.get_latest_tokens_from_log()
        access_token = token_dict.get('access_token')
        refresh_token = token_dict.get('refresh_token')
        expires_at = token_dict.get('expires_at')

        if (access_token and
                access_token != self.access_token and
                (not self.expires_at or expires_at >= self.expires_at)):
            fixed = True
            self._access_token = access_token
            self._expires_at = expires_at
            self.session.access_token = access_token

            if refresh_token != self.refresh_token:
                self._refresh_token = refresh_token
                self.session.refresh_token = refresh_token

            # This actually saves the credentials to GCP
            self.credentials = self.active_credentials

        return fixed

    def __repr__(self):
        return "<QBAuth (Oauth Version 2)>"
