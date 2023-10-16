import datetime
from typing import Union, Optional

from intuitlib.client import AuthClient
from intuitlib.enums import Scopes
from intuitlib.exceptions import AuthClientError

from finoptimal.ledger.qbo2.qbo import QBO
from finoptimal.logging import DatabaseLoggedClass, get_file_logger, void, returns, get_logger
from finoptimal.storage.fo_darkonim_bucket import FODarkonimBucket
from finoptimal.utilities import retry

logger = get_logger(__name__)
api_logger = get_file_logger('api/qbo')


class QBOAuth(DatabaseLoggedClass):

    SUBSTITUTE_CLIENT_CODE = 'substitute_client_code'
    ENVIRONMENT = 'production'
    CALLBACK_URL = "http://a.b.com"

    SCOPES = [
        Scopes.ACCOUNTING,
        Scopes.ADDRESS,
        Scopes.EMAIL,
        Scopes.OPENID,
        Scopes.PHONE,
        Scopes.PROFILE
    ]

    def __init__(self,client_code: str) -> None:
        super().__init__()
        self._client_code = self._effective_client_code = client_code
        self._realm_id = None
        self._access_token = None
        self._refresh_token = None
        self._has_access = False
        self._fo_darkonim = self._get_effective_credentials_bucket(client_code)
        self._set_client_credentials_from_bucket()

        # Set static attributes
        self._app_id = self._fo_darkonim.service_account_credentials.get('client_id')
        self._app_secret = self._fo_darkonim.service_account_credentials.get('client_secret')
        self._callback_url = self._fo_darkonim.service_account_credentials.get('callback_url')

        self._session = AuthClient(
            client_id=self._app_id,
            client_secret=self._app_secret,
            redirect_uri=self._callback_url,
            environment=self.ENVIRONMENT,
            access_token=self._access_token,
            refresh_token=self._refresh_token,
            realm_id=self._realm_id
        )

        self._has_access = self._check_access()

        # TODO: Probably change the way we trigger this, make cmdline arg
        if not self._has_access and self.vb >= 8:
            self._handle_authorization_from_terminal()

    @property
    def client_code(self) -> str:
        return self._client_code

    @property
    def effective_client_code(self) -> str:
        return self._effective_client_code

    @property
    def borrowing_credentials(self) -> bool:
        return self.client_code != self.effective_client_code

    @property
    def realm_id(self) -> Union[str, None]:
        return self._realm_id

    @property
    def access_token(self) -> Union[str, None]:
        return self._access_token

    @property
    def refresh_token(self) -> Union[str, None]:
        return self._refresh_token

    def _get_effective_credentials_bucket(self, client_code: str) -> FODarkonimBucket:
        """Returns the effective FODarkonimBucket for the client.

        Parameters
        ----------
        client_code

        Returns
        -------
        FODarkonimBucket
        """
        bucket = FODarkonimBucket(client_code=client_code, service_name=QBO.NAME)
        substitute_client_code = bucket.client_credentials.get(self.SUBSTITUTE_CLIENT_CODE)

        if not substitute_client_code:
            self._effective_client_code = client_code
            return bucket

        # Some clients "borrow" their credentials from a related company (they substitute another client code for their
        # own), so we recurse through the bucket tree until we find one without a substitute.
        self._get_effective_credentials_bucket(client_code=substitute_client_code)

    def _set_client_credentials_from_bucket(self) -> None:
        self._access_token = self._fo_darkonim.client_credentials.get('access_token')
        self._refresh_token = self._fo_darkonim.client_credentials.get('refresh_token')
        self._realm_id = self._fo_darkonim.client_credentials.get('company_id')

    def _set_client_credentials_from_session(self) -> None:
        self._realm_id = self._session.realm_id
        self._access_token = self._session.access_token
        self._refresh_token = self._session.refresh_token

    def _handle_authorization_from_terminal(self) -> None:
        authorization_url = self._session.get_authorization_url(self.SCOPES)
        authorized_url = self._get_authorized_url(authorization_url)
        self._update_session_credentials(authorized_url)
        self._set_client_credentials_from_session()

    def _get_authorized_url(self, authorization_url: str):
        self.print("Please send the user here to authorize this app to access their QBO data:")
        self.print(f'\t{authorization_url}')

        authorized_url = None

        while not authorized_url:
            authorized_url = input('\nPaste the entire callback URL here (or ctrl-c):')

        return authorized_url

    def _update_session_credentials(self, authorized_url: str) -> dict:
        tail = authorized_url.split("?")[1].strip()
        params = dict([tuple(param.split("=")) for param in tail.split("&")])
        auth_code = params.get('code')
        realm_id = params.get('realmId')
        self._session.get_bearer_token(auth_code=auth_code, realm_id=realm_id)

    def _save_client_credentials(self):
        if self.realm_id and self.access_token and self.refresh_token:
            self._fo_darkonim.client_credentials = {
                'access_token': self.access_token,
                'refresh_token': self.refresh_token,
                'realm_id': self.realm_id,
                'rt_acquired_at': str(datetime.datetime.utcnow()),
                'expires_at': str(datetime.datetime.utcnow() + datetime.timedelta(minutes=55))
            }

    def _refresh_tokens(self) -> None:
        self._session.refresh()
        self._access_token = self._session.access_token
        self._refresh_token = self._session.refresh_token

    @property
    def has_access(self) -> bool:
        return self._has_access

    def _check_access(self) -> bool:
        if self.refresh_token and self.access_token:
            return True

        if not self.refresh_token:
            return False

        try:
            self._refresh_tokens()
        except (ValueError, AuthClientError) as e:
            # Problem with refresh token, response status != 200
            self.exception()
            return False
        else:
            self._save_client_credentials()
            return True

    def disconnect(self) -> None:
        self._session.revoke(token=self.refresh_token)
