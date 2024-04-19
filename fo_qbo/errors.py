from typing import Optional, Union

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
    SUPPORTED_STATUS_CODES = [200]

    CACHING_ERROR_CODES = ['5010']  # Stale Object Error
    MISSING_PARAMETER_ERROR_CODE = '2020'

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
        del self.faults
        del self.error_df

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

        self.faults = response_data

    @response.deleter
    def response(self) -> None:
        self._response = None

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

    def has_caching_errors(self) -> bool:
        return (len(self.error_df) > 0 and
                'code' in self.error_df.columns and
                self.error_df.code.isin(self.CACHING_ERROR_CODES).any())

    def resolve(self) -> None:
        if self.has_caching_errors():
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
                rollback_days = 2
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
                restore_qbo_cache(qbs=self._qbs, days_ago=rollback_days, ignore_cdc_load=True)
                raise CachingError(error_detail, name=error_name, code=error_code)


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
        }]

    from finoptimal.ledger.qbo2.qbosesh import QBOSesh

    sesh = QBOSesh('foco', verbosity=2)
    er = QBOErrorHandler(sesh.qbs, None)

    for data in [response, batch, error_dict, stale_object_error, ar_customer]:
        er = QBOErrorHandler(sesh.qbs, response_data=data)
        print(er.faults)
        print(er.error_df)
        import ipdb

        ipdb.set_trace()
        # er.resolve()
