"""
Wrap the QBO v3 (and hopefully v4) REST API. The API supports json as well as
xml, but this wrapper ONLY supports json-formatted messages.

https://developer.intuit.com/docs/api/accounting
https://developer.intuit.com/docs/0100_accounting/0400_references/reports
https://developer.intuit.com/v2/apiexplorer

Please contact developer@finoptimal.com with questions or comments.

Copyright 2016-2024 FinOptimal, Inc. All rights reserved.
"""
import collections
import datetime
import json
import os
import textwrap
from base64 import b64encode
from typing import Union, Optional

import google.cloud.logging as logging_gcp

import requests
from django.conf import settings
from finoptimal import environment
from finoptimal.logging import LoggedClass, get_logger, void, returns
from finoptimal.utilities import retry
from fo_qbo.errors import RateLimitError, QBOErrorHandler
from .mime_types import MIME_TYPES
from .qba import QBAuth2
from .qbo import QBO

logger = get_logger(__name__)

client = logging_gcp.Client()
api_logger = client.logger('api-qbo')


class QBS(LoggedClass):
    """
    Basic wrapper, with auth functionality broken out into the QBA class
    """
    PRODUCTION_API_URL = "https://quickbooks.api.intuit.com/v3/company"
    SANDBOX_API_URL = "https://sandbox-quickbooks.api.intuit.com/v3/company"
    UNQUERIABLE_OBJECT_TYPES = ["TaxService"]
    ATTACHABLE_MIME_TYPES = MIME_TYPES

    OAUTH_VERSION = 2

    BAD_CHARS = {
        '\u2010': "-",
        '\u2013': "-",
        '\uff0c': ", ",
    }

    ALIASES = {
        "CreditCardPayment": "CreditCardPaymentTxn",
    }

    def __init__(self, client_code: str, verbosity: int = 0, modifier: Optional[str] = None):
        """
        This only works with a single company_id at a time.

        You must pass in a client_id and client_secret, or a refresh_token to bypass OOB authentication.
        """
        super().__init__()
        self.client_code = client_code
        self.vb = verbosity

        self.qbo_env = "sandbox" \
            if settings.configured and settings.DATABASES['default']['NAME'] != 'themagic' \
            else "production"

        self.api_base_url = self.SANDBOX_API_URL \
            if self.qbo_env == "sandbox" \
            else self.PRODUCTION_API_URL
        # self.api_base_url = self.PRODUCTION_API_URL  # Local override

        self.API_BASE_URL = self.api_base_url  # For backwards compatibility
        self.oauth_version = self.OAUTH_VERSION
        self.qba = QBAuth2(client_code=self.client_code, modifier=modifier, verbosity=self.vb, env=self.qbo_env)

        if self.qbo_env == "sandbox":
            self.info(f'API_BASE_URL = {self.API_BASE_URL}')

    @property
    def mav(self) -> int:
        return self.qba.minor_api_version

    @mav.setter
    def mav(self, minor_version_number):
        self.qba.minor_api_version = minor_version_number

    @property
    def at(self) -> Union[str, None]:
        return self.qba.access_token

    @property
    def rt(self) -> Union[str, None]:
        return self.qba.refresh_token

    @property
    def cid(self) -> Union[str, None]:
        return self.qba.realm_id

    @property
    def cli(self) -> Union[str, None]:
        return self.qba.client_id

    @property
    def cls(self) -> Union[str, None]:
        return self.qba.client_secret

    @property
    def exa(self) -> Union[str, None]:
        return self.qba.expires_at

    @property
    def rtaa(self) -> Union[str, None]:
        return self.qba.rt_acquired_at

    @property
    def cbu(self) -> Union[str, None]:
        return self.qba.callback_url

    @property
    def business_context(self) -> str:
        return self.qba.business_context

    @property
    def logged_in(self) -> bool:
        return self.qba.logged_in

    @retry(max_tries=2, delay_secs=0.2, drag_factor=1, exceptions=(ConnectionError,))
    @retry(max_tries=4, delay_secs=5, drag_factor=3, exceptions=(RateLimitError,))
    @logger.timeit(**returns)
    def _basic_call(self, request_type, url, data=None, **params):
        """
        params often get used for the Reports API, not for CRUD ops.
        """
        headers  = {"accept": "application/json"}
        original_params = params.copy()
        original_data   = None

        if isinstance(data, str):
            original_data = data + ""
            
        if "minorversion" not in params.get("params", {}) and self.mav is not None:
            if "params" not in params:
                params["params"] = {}

            params["params"]["minorversion"] = str(int(self.mav))

        if "download" in url:
            headers = {}
        elif "/pdf" == url[-4:]:
            headers = {"content-type": "application/pdf"}

        if request_type.lower() in ["post"]:
            if url[-4:] == "send":
                headers.update({"Content-Type": "application/octet-stream"})

            elif isinstance(data, dict):
                orig_data = data.copy()  # For Troubleshooting
                if "headers" in data:
                    # It should be a dict, then...
                    headers = data["headers"].copy()       # must be a dict
                    data    = data["request_body"] + ""    # should be text
                else:
                    headers["Content-Type"] = "application/json"
                    data = json.dumps(data)
            else:
                # (basically for queries only)
                headers["Content-Type"] = "application/text"

        if self.vb > 7:
            if isinstance(data, dict) or not data:
                self.print(json.dumps(data, indent=4))
            else:
                if len(data) > 1500:
                    self.print("First 500 characters of data:")
                    self.print(data[:750])
                    self.print("\n...\n")
                    self.print(data[-750:])
                else:
                    self.print(data)

            self.print("Above is the request body about to go here:")
            self.print(url)
            self.print("Below are the call's params and then headers:")
            self.print(json.dumps(params, indent=4))
            self.print(json.dumps(headers, indent=4))

            if self.vb > 19:
                self.print("inspect request_type, url, headers, data, and params:")
                import ipdb
                ipdb.set_trace()

        self.last_call = {
            "request_type": request_type.upper(),
            "url"         : url,
            "realm"       : self.cid,
            "header"      : headers,
            "data"        : data,
            "params"      : params}

        # Up to this point we are just building the request parameters and troubleshooting utilities

        response = self.qba.request(
            request_type.upper(),
            url,
            header_auth=True,
            realm=self.cid,
            verify=True,
            headers=headers,
            data=data,
            **params
        )

        self.last_response = response

        if not hasattr(self, "resps"):
            self.resps = collections.OrderedDict()

        intuit_tid = response.headers.get("intuit_tid")

        if intuit_tid:
            # For troubleshooting
            self.resps[intuit_tid] = (response.request.url, response.request.body, response)

        if self.vb > 7:
            self.print("The final URL (with params):")
            self.print(response.url)

            if self.vb > 15:
                self.print("inspect response:")
                import ipdb
                ipdb.set_trace()

        if response.status_code in QBOErrorHandler.SUPPORTED_STATUS_CODES:
            # Raises CachingError if problem is addressed. It is up to callers further up the stack to retry in a way
            # that's suitable.
            handler = QBOErrorHandler(self, response=response)
            handler.resolve()

        if response.status_code in [200]:
            if headers.get("accept") == "application/json":
                rj = response.json()

                if self.vb > 10:
                    self.print(json.dumps(rj, indent=4))

                    if self.vb > 14:
                        import ipdb
                        ipdb.set_trace()
                
                self.last_call_time = rj.get("time")
                return rj
            elif headers.get("content-type") == "application/pdf":
                return response
            else:
                return response.text

        try:
            error_message = response.json()
        except:
            error_message = response.text

        raise ConnectionError(error_message)

    @logger.timeit(**returns)
    def query(self,
              object_type: str,
              where_tail: Optional[str] = None,
              count_only: bool = False,
              start_position: Optional[int] = None,
              per_page: int = 1000,
              select_fields: Optional[str] = None) -> Union[list, int]:
        """Query the QuickBooks Online API.

        Parameters
        ----------
        object_type : str
            The object type to query.
        where_tail : str, optional
            The WHERE clause (e.g., "WHERE Active in (true, false) ..."). See Intuit documentation for more info.
        count_only : bool
            Perform a count query, which returns an `int`. Defaults to False.
        start_position : int, optional
            The starting point of the response for pagination.
        per_page : int
            The number of objects to return per page.
        select_fields : str, optional
            The field(s) to use in the select statement, separated by commas.

        Returns
        -------
        list or int
        """
        queried_all = False

        if not select_fields:
            select_what = "COUNT(*)" if count_only else "*"
        else:
            select_what = "COUNT(*)" if count_only else select_fields

        where_tail = " " + where_tail if where_tail else ""
        query = f"SELECT {select_what} FROM {object_type}{where_tail}"

        if not count_only:
            query += f" MAXRESULTS {per_page}"

        if start_position is not None:
            query += f" STARTPOSITION {start_position}"
            
        url = f"{self.API_BASE_URL}/{self.cid}/query"

        all_objs = []

        if object_type in self.UNQUERIABLE_OBJECT_TYPES:
            raise Exception(f"Can't query QB {object_type} objects!")

        base_len = len(query)

        while not queried_all:
            if self.vb > 6:
                self.print(query)

            resp = self._basic_call(request_type="POST", url=url, data=query)

            if self.vb > 9:
                self.print(json.dumps(resp, indent=4))

            if not resp or "QueryResponse" not in resp:
                if self.vb > 1:
                    self.print("Failed query was:")
                    self.print(query)

                    if resp:
                        if isinstance(resp, (str, dict)) and resp:
                            self.print(resp)
                        else:
                            self.print(resp.text)
                            self.print(resp.status_code)

                raise Exception("Failed QBO Query")

            if count_only:
                return resp["QueryResponse"]["totalCount"]

            alias          = self.ALIASES.get(object_type, object_type)
            objs           = resp["QueryResponse"].get(alias, [])
            start_position = resp["QueryResponse"].get("startPosition")

            if start_position is None:
                # We started seeing null responses for this attribute on
                #  2019-06-14 (instead of the attribute just not being
                #  present prior to that)
                start_position = 0

            max_results    = resp["QueryResponse"].get("maxResults")

            if max_results is None:
                # We started seeing null responses for this attribute on
                #  2019-06-14 (instead of the attribute just not being
                #  present prior to that)
                max_results = 0

            all_objs      += objs

            if self.vb > 4 and max_results > 0:
                self.print("Queried {:20s} objects {:>4} through {:4>}.".format(
                    object_type,
                    start_position,
                    start_position + max_results - 1))

            if max_results < per_page:
                queried_all = True

            # This will be the NEXT query:
            query = query[:base_len] + f" STARTPOSITION {start_position + per_page}"

        return all_objs

    @logger.timeit(**void)
    def create(self, object_type, object_dict, **params):
        """
        The object type isn't actually included in the object_dict, which is
         why you also have to pass that in (first).
        """
        url = f"{self.API_BASE_URL}/{self.cid}/{object_type.lower()}"
        self.touchless_test()
        return self._basic_call(request_type="POST", url=url, data=object_dict, **params)

    @logger.timeit(**returns)
    def read(self, object_type, object_id, **params):
        """
        Just returns a single object, no questions asked.
        """
        if len(params) > 0:
            raise NotImplementedError()
        url = f"{self.API_BASE_URL}/{self.cid}/{object_type.lower()}/{object_id}"

        return self._basic_call(request_type="GET", url=url)

    @logger.timeit(**returns)
    def update(self, object_type, object_dict, **params):
        """
        Unlike with the delete method, you really have to provide the update
         dict when making this call (otherwise the class won't know how you
         want to change the object in question).
        """
        if len(params) > 0:
            raise NotImplementedError()

        url = f"{self.API_BASE_URL}/{self.cid}/{object_type.lower()}"
        self.touchless_test()
        return self._basic_call(request_type="POST", url=url, data=object_dict)

    @logger.timeit(**returns)
    def delete(self, object_type, object_id=None, object_dict=None, **params):
        """
        Either provide an object_dict (which will simply be handed to the API
         for deletion) or an object_id. If ONLY an object_id is passed in,
         the method does a read call to GET the object_dict, then performs
         the same as if an object_dict had been passed in
        """
        if len(params) > 0:
            raise NotImplementedError()

        url = f"{self.API_BASE_URL}/{self.cid}/{object_type.lower()}"

        if object_id and not object_dict:
            object_dict = self.read(object_type, object_id)[object_type]

        # In the interest of sending as little as possible over the web...
        skinny_dict = {
            "Id"        : object_dict["Id"],
            "SyncToken" : object_dict["SyncToken"]}
        self.touchless_test()
        return self._basic_call(request_type="POST",
                                url=url,
                                data=skinny_dict,
                                params={"operation": "delete"})

    @logger.timeit(**returns)
    def batch(self, items):
        """
        https://developer.intuit.com/app/developer/qbo/docs/api/
         accounting/all-entities/batch
        """
        url = f"{self.API_BASE_URL}/{self.cid}/batch"
        self.touchless_test()
        return self._basic_call(request_type="POST",
                                url=url,
                                data={"BatchItemRequest": items})

    @logger.timeit(**returns)
    def change_data_capture(self, utc_since, object_types):
        """
        https://developer.intuit.com/docs/api/accounting/ChangeDataCapture

        Note that this only gets you changes from the last 30 days

        object_types should be a list, e.g.
         ["Purchase", "JournalEntry", "Vendor"]
        """
        url = f"{self.API_BASE_URL}/{self.cid}/cdc"

        if isinstance(utc_since, datetime.datetime):
            # Either pass in a UTC datetime or a string formatted like this:
            utc_since = utc_since.strftime("%Y-%m-%dT%H:%M:%S+00:00")

        params = {
            "changedSince" : utc_since,
            "entities"     : ",".join(object_types)}

        if self.vb > 7:
            self.print("CDC Params:")
            self.print(json.dumps(params, indent=4))

        # This will be a list of dictionaries, each of which relates to
        #  a specific response...
        return self._basic_call(request_type="GET", url=url, params=params)

    @logger.timeit(**returns)
    def report(self, report_name, **params):
        """
        Use the QBO reporting API, documented here:
        """
        url = f"{self.API_BASE_URL}/{self.cid}/reports/{report_name}"

        if self.vb > 7:
            self.print(json.dumps(params, indent=4))

        raw = self._basic_call(request_type="GET", url=url, **{"params" : params})

        if not raw:
            msg = "\n\n".join([
                "No json-formatted {} {} report to start with. rp_params:".\
                format(self.cid, report_name),
                json.dumps(params, indent=4)])
            raise Exception(msg)
        elif not "Header" in list(raw.keys()):
            msg = "\n\n".join([
                json.dumps(raw, indent=4),
                f"No Header item in {self.cid} {report_name} raw (above)!?"])
            raise Exception(msg)

        if self.vb > 7:
            self.print(json.dumps(raw["Header"], indent=4))
            self.print('(raw["Header"] is above)')

        return raw

    @logger.timeit(**returns)
    def upload(self, path, attach_to_object_type=None,
               attach_to_object_id=None, new_name=None,
               include_on_send=False):
        """
        https://developer.intuit.com/docs/api/accounting/attachable

        https://developer.intuit.com/v2/apiexplorer?apiname=V3QBO#?id=Attachable

        new_name is there in case you're uploading from one place, but you want
         the file's name to come from somewhere else.

        In theory you can attach to multiple objects, but you'd have to roll
         your own for that use case.

        Note that, as of this writing, the API ignored the json part of the
         request, but the upload does work. You simply have to do an Update
         of the attachable to get the name right (which will be all lower-case)
         and to achieve the attachment to one or more transaction entities.
        """
        url       = f"{self.API_BASE_URL}/{self.cid}/upload"
        loc, name = os.path.split(path)
        base, ext = os.path.splitext(name)
        mime_type = self.ATTACHABLE_MIME_TYPES.get(ext.lower())

        if not mime_type:
            raise Exception(f"MIME type for files with extension {ext}?")

        boundary  = "-------------PythonMultipartPost"
        headers   = {
            "Content-Type"    : f"multipart/form-data;boundary={boundary}",
            "accept"          : "application/json",
            "Connection"      : "close",
            #"Accept-Encoding" : "gzip;q=1.0,deflate;q=0.6,identity;q=0.3",
            #"User-Agent"      : "OAuth gem v0.4.7",
            "cache-control"   : "no-cache",
        }

        with open(path, "rb") as handle:
            binary_data = b64encode(handle.read())

        jd              = {
            "ContentType" : mime_type,
            "FileName"    : new_name if new_name else name,}

        if attach_to_object_type and attach_to_object_id:
            jd.update({
                "AttachableRef" : [
                    {"EntityRef": {
                        "type"  : attach_to_object_type,
                        "value" : attach_to_object_id,},
                     "IncludeOnSend" : include_on_send},],})

        request_body    = textwrap.dedent(
            """
            --{}
            Content-Disposition: form-data; name="file_metadata_1";filename="{}"
            Content-Type: application/json

            {}
            --{}
            Content-Disposition: form-data; name="file_content_1";filename="{}"
            Content-Type: {}
            Content-Length: {:d}
            Content-Transfer-Encoding: base64

            {}
            --{}--
            """
        ).format(boundary, "metadata.json", json.dumps(jd, indent=0),
                 boundary, name, mime_type, len(binary_data),
                 binary_data.decode('utf-8'), boundary)

        for ky, vl in self.BAD_CHARS.items():
            # See https://stackoverflow.com/questions/41030128/
            #  str-encoding-from-latin-1-to-utf-8-arbitrarily
            request_body = request_body.replace(ky, vl)
        
        data = {
            "headers"     : headers.copy(),
            "request_body": request_body
        }
        self.touchless_test()
        return self._basic_call(request_type="POST", url=url, data=data)

    @logger.timeit(**returns)
    def download(self, attachable_id, path):
        """
        https://developer.intuit.com/docs/api/accounting/attachable
        """
        '''
        url    = "{}/{}/download/{}".format(
            self.API_BASE_URL, self.cid, attachable_id)
        link   = self._basic_call("GET", url)
        '''
        # Don't even bother with the above...just do a Read to get the link
        #  in addition to important metadata that may be useful...
        att       = self.read("Attachable", attachable_id)["Attachable"]
        link      = att["TempDownloadUri"]
        fn        = att["FileName"]

        if os.path.isdir(path):
            path = os.path.join(path, fn)
            
        loc, name = os.path.split(path)

        if not name:
            name  = fn + ""

        path      = os.path.join(loc, name)

        handle    = open(path, "wb")

        resp = requests.get(link, timeout=60)
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

        for chunk in resp.iter_content(1024):
            handle.write(chunk)

        handle.close()

        return path  # Because this may have changed if a directory was passed in

    @logger.timeit(**returns)
    def get_pdf(self, object_type, object_id, path):
        """
        https://developer.intuit.com/docs/api/accounting/invoice
        """
        link   = f"{self.API_BASE_URL}/{self.cid}/{object_type.lower()}/{object_id}/pdf"

        if self.vb > 4:
            self.print(f"Downloading {object_type} {object_id} from {link}...")

        with open(path, "wb") as handle:
            for chunk in self._basic_call(request_type="GET", url=link).iter_content(1024):
                handle.write(chunk)

            handle.close()

        return link

    @logger.timeit(**returns)
    def send(self, object_type, object_id, recipient):
        """
        https://developer.intuit.com/docs/api/accounting/invoice
        """
        url   = f"{self.API_BASE_URL}/{self.cid}/{object_type.lower()}/{object_id}/send"

        if self.vb > 4:
            self.print(f"Emailing {object_type} {object_id} to {recipient}...")
        self.touchless_test()
        return self._basic_call(request_type="POST",
                                url=url,
                                **{"params": {"params": {"sendTo": recipient}}})

    @classmethod
    def get_where_in_query_string(cls, select_field: str, values: list, object_type: str) -> str:
        values_string = ','.join([f"'{i}'" for i in values])
        query = f'WHERE {select_field} IN ({values_string})'

        if object_type not in QBO.TRANSACTION_OBJECTS + QBO.OTHER_OBJECTS:
            query = f'{query} AND Active in (true,false)'

        return query

    def get_missing(self, object_type: str, values: list, select_field: str):
        """
        Returns the values missing from QuickBooks.
        """
        starting_index = 0
        max_count = 999
        values_count = len(values)
        missing_values = []

        # We query in increments so that we don't blow up on supersized value lists (e.g. cdc.py)

        while starting_index < values_count:
            values_subset = values[starting_index : starting_index + max_count]

            where_tail = self.get_where_in_query_string(
                select_field=select_field,
                values=values_subset,
                object_type=object_type
            )
            response = self.query(object_type=object_type, where_tail=where_tail, select_fields=select_field)
            existing = [i.get(select_field) for i in response]
            missing = list(set(values_subset).difference(existing))
            missing_values.extend(missing)

            starting_index += max_count

        return missing_values

    def __repr__(self):
        return f"<{self.cid} QBS (OAuth Version {self.oauth_version})>"

    @property
    def touchless_mode(self) -> bool:
        """bool: Touchless mode was activated from the command line."""
        if not hasattr(self, "_touchless_mode"):
            self._touchless_mode = True if self.job and self.job.args.get('touchless_mode') else False

        return self._touchless_mode

    def touchless_test(self) -> None:
        """Raise an Exception if `touchless_mode` is True."""
        if self.touchless_mode and not environment.is_production():
            error = 'Touchless Failure'
            print(error)
            import ipdb;ipdb.set_trace()
            raise Exception(error)

    @property
    def caller(self) -> str:
        return self.qba.caller

    @property
    def realm_id(self) -> str:
        return self.qba.realm_id
