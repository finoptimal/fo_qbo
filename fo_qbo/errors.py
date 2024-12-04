from typing import Optional, Union

import pandas as pd
import requests

from finoptimal.firstaid.qbo import create_disconnection_ticket
from finoptimal.exceptions import APIError
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


class BusinessValidationError(TechnicalError):
    pass


class SuspectedTransientError(TechnicalError):
    """
    Intermittent error we want to stuff and basically just retry:
      https://help.developer.intuit.com/s/question/0D54R00008xOl3zSAC/getting-error-6000-an-unexpected-error-occurred-while-accessing-or-saving-your-data-but-the-entity-saves-anyway
    """
    pass


class QBOErrorHandler(LoggedClass):
    """
    Aims to resolve QBO errors inplace.

    This class will determine if there are errors it can resolve, and if a resolution takes place, the appropriate
    error is raised so that callers can retry.

    Parameters
    ----------
    qbs : QBS
    response : requests.Response, optional
        The API response.
    response_data : list or dict, optional
        Data from the API response.
    """
    # 400 status_codes with API code 5010 seem related to entities, not entries, and specific to aplus, expensify, and
    # custom code. Not worrying about those for now.
    SUPPORTED_STATUS_CODES = [200, 400]

    # 5010 = Stale Object Error
    # 610 = Object Not Found (This CAN be related to NON-caching problems, so further inspection is required)
    CACHING_ERROR_CODES = ['5010', '610']
    MISSING_PARAMETER_ERROR_CODE = '2020'
    BUSINESS_VALIDATION_ERROR_CODES = ['6000']
    OTHER_SUSPECTED_TRANSIENT_ERROR_CODES = [] #['5020']

    def __init__(self,
                 qbs,
                 response: Optional[requests.Response] = None,
                 response_data: Optional[Union[list, dict]] = None) -> None:
        self._qbs = qbs
        self.reset_state()

        try:
            if response is not None:
                self.response = response

            elif response_data is not None:
                self.response_data = response_data
                self.faults = response_data

        except Exception as ex:
            # I'll probably remove this stuff after proving this out
            self.exception()
            self.reset_state()
            error_msg = f'An error occurred during initialization: {str(ex)}'
            self._qbs.qba.api_logger.info(error_msg[:200])

        super().__init__()

    def reset_state(self) -> None:
        del self.response
        del self.response_data
        del self.faults
        del self.error_df


    @property
    def status_code(self):
        if self.response is None:
            return None

        return self.response.status_code

    
    @property
    def has_seen_a_suspected_transient_error(self) -> bool:
        if not hasattr(self, "_has_seen_a_suspected_transient_error"):
            self.has_seen_a_suspected_transient_error = False
            
        return self._has_seen_a_suspected_transient_error
    
    
    @has_seen_a_suspected_transient_error.setter
    def has_seen_a_suspected_transient_error(self, value: bool) -> None:
        self._has_seen_a_suspected_transient_error = value
    

    @staticmethod
    def get_list_of_faults(data: Union[dict, list]) -> list:
        fault_key = 'Fault'
        batch_key = 'BatchItemResponse'
        list_of_faults = []

        if isinstance(data, dict) and fault_key in data:
            # "non-batch" format
            list_of_faults = [data]

        elif isinstance(data, dict) and batch_key in data:
            # "batch" format
            batch_data = data.get(batch_key)
            list_of_faults = [i for i in batch_data if fault_key in i]

        elif isinstance(data, list) and len(data) > 0 and fault_key in data[0]:
            # "error_dict" format
            list_of_faults = data

        return list_of_faults

    @staticmethod
    def get_error_df(faults: list) -> pd.DataFrame:
        dfs = []
        fault_df = pd.DataFrame(faults)
        cols = [i for i in fault_df.columns if i != 'Fault']

        # I know this isn't the most efficient way to access the data, but this data set should always be small and
        # the efficiency loss is negligible (for now).
        for index, row in fault_df.iterrows():
            df = pd.io.json.json_normalize(row.Fault, record_path=['Error'])

            for col in cols:
                if row[col]:
                    df[col] = row[col]

            dfs.append(df)

        return pd.concat(dfs) if len(dfs) > 0 else pd.DataFrame()

    @property
    def response(self) -> Union[requests.Response, None]:
        return self._response

    @response.setter
    def response(self, response: Union[requests.Response, None]) -> None:
        self._response = response
        response_data = {}

        if self._response is not None:

            try:
                response_data = self._response.json()

            except Exception:
                self.exception()

        self.response_data = response_data
        self.faults = response_data

    @response.deleter
    def response(self) -> None:
        self._response = None


    @property
    def url(self):
        if self.response is None:
            return None

        return self.response.request.url


    @property
    def request_body(self):
        if self.response is None:
            return None

        return self.response.request.body


    @property
    def response_data(self) -> Union[dict, list, None]:
        return self._response_data

    @response_data.setter
    def response_data(self, response_data: Union[dict, list]) -> None:
        self._response_data = response_data

    @response_data.deleter
    def response_data(self) -> None:
        self._response_data = None

    @property
    def faults(self) -> list:
        return self._faults

    @faults.setter
    def faults(self, data: Union[list, dict]) -> None:
        try:
            self._faults = self.get_list_of_faults(data)

        except Exception:
            self.exception()
            del self.faults

        self.error_df = self.faults

    @faults.deleter
    def faults(self) -> None:
        self._faults = []

    @property
    def error_df(self) -> pd.DataFrame:
        return self._error_df

    @error_df.setter
    def error_df(self, faults: list) -> None:
        try:
            self._error_df = self.get_error_df(faults)

        except Exception:
            self.exception()
            del self.error_df

    @error_df.deleter
    def error_df(self) -> None:
        self._error_df = pd.DataFrame()

    @property
    def has_caching_errors(self) -> bool:
        if (
            len(self.error_df) > 0 and
            'code' in self.error_df.columns and
            self.error_df.code.isin(self.CACHING_ERROR_CODES).any()
        ):
            if '5010' in self.error_df.code.unique().tolist():
                return True

            if 'Detail' in self.error_df.columns:
                return len(
                    self.error_df.loc[
                        self.error_df.Detail.fillna('').str.contains('Another user has deleted this transaction')
                    ].index
                ) > 0

        return False

    @property
    def has_suspected_transient_errors(self) -> bool:
        if len(self.error_df) < 1 or not "code" in self.error_df.columns:
            return False

        if self.error_df.code.isin(self.BUSINESS_VALIDATION_ERROR_CODES).any():
            if 'Detail' in self.error_df.columns:
                matching_errors = self.error_df.loc[
                    self.error_df.code.isin(self.BUSINESS_VALIDATION_ERROR_CODES) &
                    self.error_df.Detail.fillna('').str.contains('Please wait a few minutes and try again')
                ]
                
                return len(matching_errors) > 0

        if self.error_df.code.isin(self.OTHER_SUSPECTED_TRANSIENT_ERROR_CODES).any():
            return True

        return False


    @property
    def has_authorization_errors(self) -> bool:
        return isinstance(self.response_data, dict) and self.response_data.get('x_error_reason') == 'user_not_in_realm'


    @property
    def has_feature_permission_errors(self):
        if not self.status_code == 400:
            return False

        if not "code" in self.error_df.columns:
            return False

        return "5020" in self.error_df.code.values


    @property
    def error_slug(self):
        if self.has_feature_permission_errors:
            if "BudgetVsActuals" in self.url:
                return "qbo-api-error-permission-denied-bva"

            return "qbo-api-error-permission-denied-unspecified"

        return "qbo-api-error-unspecified"

    @property
    def error_text(self):
        if self.error_slug == "qbo-api-error-permission-denied-bva":
            budget_count = len(self._qbs.query("Budget"))
            return " ".join([
                f"You are trying to access a QuickBooks Online Budget-vs-Actuals report,",
                f"but it doesn't look like you have any budgets in your data file!",
            ])

        return None # which should let the default kick in

    def resolve(self) -> None:
        # Will lightly refactor for DRY soon

        if self.has_caching_errors:
            self.info('')
            self.info('')
            self.info('===============================================================================================')
            self.info(f'CACHING ERROR DETECTED')
            self.info('===============================================================================================')

            try:
                from finoptimal.admin.helpers import restore_qbo_cache  # Fucking import errors

                error = self.error_df.loc[self.error_df.code.isin(self.CACHING_ERROR_CODES)].iloc[0]
                error_name = error.get('Message')
                error_code = error.get('code')
                error_detail = error.get('Detail')
                # Try to cover workflows that only run monthly; approximate to be sure...better would be to look at the
                #  update stamp of the conflicted entries and roll back THAT far...
                rollback_days = 40
                fix_msg = f'Rolling back {self._qbs.client_code} cache {rollback_days} day(s) to resolve {error_name}'

                self.info(f'error_name:   {error_name}')
                self.info(f'error_code:   {error_code}')
                self.info(f'error_detail: {error_detail}')
                self.info(fix_msg)
                self._qbs.qba.api_logger.info(fix_msg)

                self.info('===========================================================================================')
                self.info('')
                self.info('')

            except Exception:
                self.exception()

            else:
                self.note(fix_msg, tracer_at=3)
                restore_qbo_cache(qbs=self._qbs, days_ago=rollback_days, ignore_cdc_load=True)
                raise CachingError(error_detail, name=error_name, code=error_code)

        elif self.has_suspected_transient_errors:
            self.info('')
            self.info('')
            self.info('===============================================================================================')
            self.info(f'BUSINESS VALIDATION ERROR DETECTED')
            self.info('===============================================================================================')

            try:
                error = self.error_df.loc[self.error_df.code.isin(self.BUSINESS_VALIDATION_ERROR_CODES)].iloc[0]
                error_name = error.get('Message')
                error_code = error.get('code')
                error_detail = error.get('Detail')
                fix_msg = f'Waiting and retrying call in response to temporary business validation error'

                self.info(f'error_name:   {error_name}')
                self.info(f'error_code:   {error_code}')
                self.info(f'error_detail: {error_detail}')
                self.info(fix_msg)
                self._qbs.qba.api_logger.info(fix_msg)

                self.info('===========================================================================================')
                self.info('')
                self.info('')

            except Exception:
                self.exception()

            else:
                self.has_seen_a_suspected_transient_error = True
                self.note("About to raise SuspectedTransientError!", tracer_at=3)
                raise SuspectedTransientError(error_detail, name=error_name, code=error_code)

        elif self.has_authorization_errors:
            create_disconnection_ticket(client_code=self._qbs.client_code, user_not_in_realm=True)

        elif self.has_feature_permission_errors:
            # self.note("Check subscription tier et al?", ta=3)
            raise APIError(dict(
                error_slug=self.error_slug,
                error_text=self.error_text,
                while_trying_to=f"Call {self.url} with body[:250]: {str(self.request_body)[:250]}",
            ))


if __name__ == '__main__':
    response = {
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

    batch = {'BatchItemResponse': [
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

    error_dict = [{
        'Fault': {
            'Error': [
                {
                    'Message': 'Required param missing, need to supply the required value for the API',
                    'Detail': 'Required parameter AccountRef is missing in the request',
                    'code': '2020',
                    'element': 'AccountRef'
                }
            ],
            'type': 'ValidationFault'
        },
        'entry_id': '',
        'entry_label': 'GoodEL_2024_04_30',
        'entry_magic': 'Booker-GoogleSpreadsheet',
        'entry_type': 'JournalEntry',
        'operation': 'create',
        'result': {}
    }]

    stale_object_error = {
        'BatchItemResponse': [
            {
                'Fault': {
                    'Error': [
                        {
                            'Message': 'Stale Object Error',
                            'Detail': 'Stale Object Error : You and Jesse Rubenfeld were working on this at the same time. Jesse Rubenfeld finished before you did, so your work was not saved.',
                            'code': '5010',
                            'element': ''
                        }
                    ],
                    'type': 'ValidationFault'
                },
                'bId': 'Invoice|52625|ST_FD_SIHP-2024-02|'
            }, {
                'Fault': {
                    'Error': [
                        {
                            'Message': 'Stale Object Error',
                            'Detail': 'Stale Object Error : You and Jesse Rubenfeld were working on this at the same time. Jesse Rubenfeld finished before you did, so your work was not saved.',
                            'code': '5010',
                            'element': ''
                        }
                    ],
                    'type': 'ValidationFault'
                },
                'bId': 'Invoice|52702|ST_FD_SIHP-2023-12|'
            }
        ],
        'time': '2024-04-08T04:12:18.814-07:00'
    }

    ar_customer = [
        {'Fault': {
            'Error': [
                {
                    'Detail': 'Business Validation Error: When you use Accounts Receivable, you must choose a customer in the Name field.',
                    'Message': 'A business validation error has occurred while processing your request',
                    'code': '6000',
                    'element': ''
                }
            ],
            'type': 'ValidationFault'
        },
            'entry_id': '8497',
            'entry_label': 'EOM_Mar24_2024_03_29',
            'entry_magic': 'Booker-GoogleSpreadsheet',
            'entry_type': 'JournalEntry',
            'operation': 'update',
            'result': {}
        }
    ]

    not_found = {
        'BatchItemResponse': [
            {
                'Fault': {
                    'Error': [
                        {
                            'Message': 'Object Not Found',
                            'Detail': 'Object Not Found : Another user has deleted this transaction.',
                            'code': '610',
                            'element': ''
                        }
                    ],
                    'type': 'ValidationFault'
                },
                'bId': 'JournalEntry|4351|AA_2024-02_1852ec|Accruer'
            },
            {
                'JournalEntry': {
                    'domain': 'QBO',
                    'status': 'Deleted',
                    'Id': '4352'
                },
                'bId': 'JournalEntry|4352|AA_2024-02_1f316c|Accruer'
            }
        ],
        'time': '2024-04-22T18:38:22.315-07:00'
    }

    bus_val = {
        'Fault': {
            'Error': [
                {
                    'Message': 'A business validation error has occurred while processing your request',
                    'Detail': 'Business Validation Error: An unexpected error occurred while accessing or saving '
                              'your data. Please wait a few minutes and try again. If the problem persists, contact '
                              'customer support.',
                    'code': '6000',
                    'element': ''
                }
            ],
            'type': 'ValidationFault'
        },
        'time': '2024-04-11T20:21:08.175-07:00'
    }

    user_not_in_realm = {
        'error_description': 'Unauthorized Request: User is not a member of the specified Realm',
        'x_error_reason': 'user_not_in_realm',
        'x_error_reason_detail': 'The user is not in the specified realm',
        'error': 'invalid_grant'
    }


    from finoptimal.ledger.qbo2.qbosesh import QBOSesh

    sesh = QBOSesh('foco', verbosity=2)

    for data in [user_not_in_realm, batch, stale_object_error, ar_customer, not_found, bus_val]:
        er = QBOErrorHandler(sesh.qbs, response_data=data)
        print(er.has_caching_errors)
        print(er.has_suspected_transient_errors)
        print(er.has_authorization_errors)

        # import ipdb;ipdb.set_trace()
        #
        # try:
        #     er.resolve()
        # except BusinessValidationError as e:
        #     print(dir(e))
        # else:
        #     print('Nothing to resolve')

