
import pandas as pd
import requests
from finoptimal.logging import LoggedClass


class TechnicalError(Exception):

    def __init__(self, *args, **kwargs) -> None:
        for keyword, value in kwargs.items():
            setattr(self, keyword, value)

        super().__init__(*args)


class UnauthorizedError(TechnicalError):
    pass


class RateLimitError(TechnicalError):
    pass


class CachingError(TechnicalError):
    pass


class QBOErrorHandler(LoggedClass):
    """
    Aims to resolve QBO errors inplace.

    Parameters
    ----------
    qbs : QBS
    response : requests.Response
        The API response. This class will determine if there are errors it can resolve, and if a resolution takes place,
        the appropriate error is raised so that callers can retry.
    """
    # 400 status_codes with API code 5010 seem related to entities, not entries, and specific to aplus, expensify, and
    # custom code. Not worrying about those for now.
    SUPPORTED_STATUS_CODES = [200]

    CACHING_ERROR_CODES = ['5010']  # Stale Object Error

    def __init__(self, qbs, response: requests.Response) -> None:
        # There is a property hierarchy set within this class. Setting any one low-level property will update any
        # dependent, higher-level property. The hierarchy, from low to high, is this:
        #   - response
        #   - json
        #   - faults
        #   - errors
        #   - error_messages
        #   - error_message_df
        self._qbs = qbs
        self.response = response
        super().__init__()

    @property
    def response(self) -> requests.Response:
        return self._response

    @response.setter
    def response(self, response: requests.Response) -> None:
        self._response = response
        self.json = response

    @property
    def json(self) -> dict:
        return self._json

    @json.setter
    def json(self, response: requests.Response) -> None:
        try:
            self._json = response.json()
        except Exception:
            # TODO: Catch more specific exception
            self._json = {}

        self.faults = self._json

    @property
    def faults(self) -> list:
        return self._faults

    @faults.setter
    def faults(self, response_json: dict) -> None:
        self._faults = []
        batch_item_response = response_json.get('BatchItemResponse', [])

        if batch_item_response:
            for item in batch_item_response:
                if item.get('Fault'):
                    self._faults.append(item)

        elif response_json.get('Fault'):
            self._faults.append(response_json)

        self.errors = self._faults

    @property
    def errors(self) -> list:
        return self._errors

    @errors.setter
    def errors(self, faults: list) -> None:
        self._errors = [fault.get('Fault') for fault in faults]
        self.error_messages = self._errors

    @property
    def error_messages(self) -> list:
        return self._error_messages

    @error_messages.setter
    def error_messages(self, errors: list) -> None:
        self._error_messages = []

        for error in errors:
            self._error_messages.extend(error.get('Error', []))

        self.error_message_df = self._error_messages

    @property
    def error_message_df(self) -> pd.DataFrame:
        return self._error_message_df

    @error_message_df.setter
    def error_message_df(self, error_messages: list) -> None:
        self._error_message_df = pd.DataFrame(error_messages)

    def has_caching_errors(self) -> bool:
        return (len(self.error_message_df) > 0 and
                'code' in self.error_message_df.columns and
                self.error_message_df.code.isin(self.CACHING_ERROR_CODES).any())

    def resolve(self) -> None:
        if self.has_caching_errors():
            self.info('')
            self.info('')
            self.info('===============================================================================================')
            self.info(f'CACHING ERROR DETECTED')
            self.info('===============================================================================================')
            from finoptimal.admin.helpers import restore_qbo_cache  # Fucking import errors

            error = self.error_message_df.loc[self.error_message_df.code.isin(self.CACHING_ERROR_CODES)].iloc[0]
            error_name = error.get('Message')
            error_code = error.get('code')
            error_detail = error.get('Detail')
            rollback_days = 2  # TODO: Maybe parameterize
            fix_message = f'Rolling back {self._qbs.client_code} cache {rollback_days} day(s) to resolve {error_name}'

            self.info(f'error_name:   {error_name}')
            self.info(f'error_code:   {error_code}')
            self.info(f'error_detail: {error_detail}')
            self.info(fix_message)
            self._qbs.qba.api_logger.info(fix_message)
            self.info('===============================================================================================')
            self.info('')
            self.info('')
            restore_qbo_cache(qbs=self._qbs, days_ago=rollback_days, ignore_cdc_load=True)
            raise CachingError(error_detail, name=error_name, code=error_code)


if __name__ == '__main__':
    batch1 = {
        'Fault':
            {'Error': [
                {
                    'Message': 'A business validation error has occurred while processing your request',
                    'Detail': 'Business Validation Error: Fill out at least two detail lines to continue.',
                    'code': '6000',
                    'element': ''
                }
            ],
                'type': 'ValidationFault'
            },
        'bId': 'JournalEntry||ICSWING_2024-02-29|ICS'
    }

    batch2 = {'BatchItemResponse': [
        {'Fault':
            {'Error': [
                {
                    'Message': 'Duplicate Document Number Error',
                    'Detail': 'Duplicate Document Number Error : You must specify a different number. This number has already been used. DocNumber=AA_2025-07_7676ce is assigned to TxnType=Journal Entry with TxnId=3632',
                    'code': '6140',
                    'element': ''
                },
                {
                    'Message': 'Required param missing, need to supply the required value for the API',
                    'Detail': 'Required parameter AccountRef is missing in the request',
                    'code': '2020',
                    'element': 'AccountRef'
                },
                {
                    'Message': 'Required param missing, need to supply the required value for the API',
                    'Detail': 'Required parameter AccountRef is missing in the request',
                    'code': '2020',
                    'element': 'AccountRef'
                },
                {
                    'Message': 'Required param missing, need to supply the required value for the API',
                    'Detail': 'Required parameter AccountRef is missing in the request',
                    'code': '2020',
                    'element': 'AccountRef'
                }
            ],
                'type': 'ValidationFault'
            },
            'bId': 'JournalEntry|51278|GClayton_COM_022024|Gopher-Hubspot-Commissions'
        },
        {'Fault':
            {'Error': [
                {
                    'Message': 'Required param missing, need to supply the required value for the API',
                    'Detail': 'Required parameter AccountRef is missing in the request',
                    'code': '2020',
                    'element': 'AccountRef'
                },
                {
                    'Message': 'Required param missing, need to supply the required value for the API',
                    'Detail': 'Required parameter AccountRef is missing in the request',
                    'code': '2020',
                    'element': 'AccountRef'
                },
                {
                    'Message': 'Required param missing, need to supply the required value for the API',
                    'Detail': 'Required parameter AccountRef is missing in the request',
                    'code': '2020',
                    'element': 'AccountRef'
                }
            ],
                'type': 'ValidationFault'
            },
            'bId': 'JournalEntry|51279|GClayton_COM_032024|Gopher-Hubspot-Commissions'
        }
    ]}

    from finoptimal.ledger.qbo2.qbosesh import QBOSesh
    sesh = QBOSesh('foco', verbosity=2)
    e = QBOErrorHandler(sesh.qbs, None)
    e.faults = batch2
    import ipdb;ipdb.set_trace()