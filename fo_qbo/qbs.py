# Wrap the QBO v3 (and hopefully v4) REST API. The API supports json as well as
#  xml, but this wrapper ONLY supports json-formatted messages.

# https://developer.intuit.com/docs/api/accounting
# https://developer.intuit.com/docs/0100_accounting/0400_references/reports
# https://developer.intuit.com/v2/apiexplorer

# Please contact developer@finoptimal.com with questions or comments.

from base64      import b64encode

import datetime, json, os, requests, textwrap, time, traceback

from .qba        import QBAuth2
from .mime_types import MIME_TYPES

IMMEDIATELY_RAISABLE_ERRORS = {}

def retry(max_tries=3, delay_secs=0.2):
    """
    Produces a decorator which tries effectively the function it decorates
     a given number of times. Because the QBO API has been known to respond
     erratically (and, e.g., return "Unauthorized" errors erroneously), this
     method takes a hammer-it approach to the problem (within reason).
    """
    def decorator(retriable_function):
        def inner(*args, **kwargs):
            """
            Retries retriable_function max_tries times, waiting delay_secs
             between tries (and increasing delay_secs geometrically by the
             drag_factor). escape can be set to true during a run to get out
             immediately if, e.g. ipdb is running.
            """
            tries  = kwargs.get("tries", max_tries)
            delay  = kwargs.get("delay", delay_secs)

            attempts = 0

            while True:
                try:
                    return retriable_function(*args, **kwargs)
                    break
                except Exception as exc:
                    tries    -= 1
                    attempts += 1
                    if tries <= 0:
                        raise
                    # back off as failures accumulate in case it's transient
                    time.sleep(delay * attempts)

        return inner
    return decorator

class QBS(object):
    """
    Basic wrapper, with auth functionality broken out into the QBA class
    """
    API_BASE_URL              = "https://quickbooks.api.intuit.com/v3/company"
    UNQUERIABLE_OBJECT_TYPES  = ["TaxService"]
    ATTACHABLE_MIME_TYPES     = MIME_TYPES

    def __init__(
            self,
            # oauth 1:
            consumer_key=None, consumer_secret=None, expires_on=None,
            # oauth 2
            client_id=None, client_secret=None, refresh_token=None, 
            access_token=None, access_token_secret=None, company_id=None,
            callback_url=None, expires_at=None,
            new_token_callback_function=None, minor_api_version=None,
            rt_acquired_at=None, reload_credentials_callback=None, verbosity=0):
        """
        This only works with a single company_id at a time.

        You must pass in a client_id and client_secret, or
        a refresh_token to bypass OOB authentication.
        """
        if not access_token_secret is None:
            # If there is no active OAuth1 access_tokens (and presumably, then,
            #  no access_token_secret), we use OAuth2. The deprecated Py2
            #  version of this wrapper is the only way to still get OAuth1
            #  access tokens (which will become impossible in December, 2019)
            self.oauth_version = 1
        elif not client_id is None  and not client_secret   is None:
            self.oauth_version = 2
        else:
            raise ValueError(
                "Could not create connection to QB API, not enough credentials")

        # Oauth 2
        self.cli   = client_id
        self.cls   = client_secret
        self.rt    = refresh_token
        self.exa   = expires_at
        self.rtaa  = rt_acquired_at
        self.ntcbf = new_token_callback_function
        self.at    = access_token
        self.cid   = company_id
        self.rlc   = reload_credentials_callback
        
        self.cbu   = callback_url
        
        self.mav   = minor_api_version
        self.vb    = verbosity

        self._setup()

    def _setup(self):
        """
        Make sure the access_token is fresh (otherwise reconnect). If there's NO
         access_token (and, of course, accompanying secret), go through the
         connect workflow.
        """
        if not self.exa is None:
            if self.exa < str(datetime.datetime.utcnow()):
                if self.vb > 2:
                    print(f"\n{self.cid}'s access_token has expired;",
                          "not passing to QBA.\n")
                self.at = None

        self.qba = QBAuth2(
            self.cli, self.cls, realm_id=self.cid,
            refresh_token=self.rt, access_token=self.at,
            callback_url=self.cbu, verbosity=self.vb)

        if self.cid is None:
            self.qba.establish_access()
            self.address_new_oauth2_token()
                
        if self.qba.session is None:
            if self.vb > 1:
                print("QBS has no working access token!")
                if self.vb > 8:
                    print("Inspect self.qba, the QBAuth object:")
                    import ipdb;ipdb.set_trace()

        if self.qba.new_token and self.oauth_version == 1:
            if self.cid is None:
                self.cid = self.qba.realm_id
            if self.vb > 1:
                print(f"New access token et al for company id {self.cid}.")
                print("Don't forget to store it!")

    def _reload_credentials(self):
        """
        Addresses a situation where another process refreshes while this
         process is still using these old creds. In such a situation, the 
         refresh operation will fail, and we need to get new creds from the
         QBS instantiator.
        """
        if self.vb > 2:
            print(f"Reloading {self} credentials!")

        # Reset this objects cred attrs...
        self._reload_credentials_callback()

        # Propagate the new creds to the QBA object...
        for attr in ["refresh_token", "access_token"]:
            setattr(self.qbs, attr, getattr(self, attr))

    @retry()
    def _basic_call(self, request_type, url, data=None, **params):
        """
        params often get used for the Reports API, not for CRUD ops.
        """
        headers  = {"accept" : "application/json"}

        """
        if not "minorversion" in url and not self.mav is None:
            url += "?minorversion={}".format(str(int(self.mav)))
        """
        original_params = params.copy()
        original_data   = None
        if isinstance(data, str):
            original_data = data + ""
            
        if not "minorversion" in params.get("params", {}) and \
           not self.mav is None:
            if not "params" in params:
                params["params"] = {}
            params["params"]["minorversion"] = str(int(self.mav))

        if "download" in url:
            headers = {}
        elif "/pdf" == url[-4:]:
            headers = {"content-type" : "application/pdf"}

        if request_type.lower() in ["post"]:
            if url[-4:] == "send":
                headers.update({"Content-Type" : "application/octet-stream"})

            elif isinstance(data, dict):
                orig_data = data.copy() # For Troubleshooting
                if "headers" in data:
                    # It should be a dict, then...
                    headers = data["headers"].copy()       # must be a dict
                    data    = data["request_body"] + ""    # should be text
                    #data    = data["request_body"].encode("utf-8")
                else:
                    headers["Content-Type"] = "application/json"
                    data = json.dumps(data)
            else:
                # (basically for queries only)
                headers["Content-Type"] = "application/text"

        if self.vb > 7:
            if isinstance(data, dict) or not data:
                print(json.dumps(data, indent=4))
            else:
                if len(data) > 1500:
                    print("First 500 characters of data:")
                    print(data[:750])
                    print("\n...\n")
                    print(data[-750:])
                else:
                    print(data)
            print("Above is the request body about to go here:")
            print(url)
            print("Below are the call's params and then headers:")
            print(json.dumps(params, indent=4))
            print(json.dumps(headers, indent=4))
            if self.vb > 19:
                print("inspect request_type, url, headers, data, and params:")
                import ipdb;ipdb.set_trace()

        self.last_call = {
            "request_type" : request_type.upper(),
            "url"          : url,
            "realm"        : self.cid,
            "header"       : headers,
            "data"         : data,
            "params"       : params}


        established_access = False
        tries_remaining    = 2
        while not established_access:
            # Handle a situation where one instance loads up credentials that
            #  an earlier instance is ABOUT to blow away (because of a token
            #  refresh).
            try:
                self.qba.establish_access()
                break
            
            except:
                if not hasattr(self.qba, "refresh_failure"):
                    raise
                
                if self.qba.refresh_failure:
                    tries -= 1
                    self._reload_credentials()

                if tries > 0:
                    continue

                raise   
                    
        response = self.qba.request(
                request_type.upper(), url, header_auth=True, realm=self.cid,
                verify=True, headers=headers, data=data, **params)
        
        self.last_response = response
        
        if self.vb > 7:
            print("The final URL (with params):")
            print(response.url)
            if self.vb > 15:
                print("inspect response:")
                import ipdb;ipdb.set_trace()

        if self.oauth_version == 2 and self.address_new_oauth2_token():
            return self._basic_call(
                request_type, url, data=original_data,
                **original_params)
                    
        if response.status_code in [200]:
            if headers.get("accept") == "application/json":
                rj = response.json()

                if self.vb > 10:
                    print(json.dumps(rj, indent=4))
                    if self.vb > 14:
                        import ipdb;ipdb.set_trace()
                
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

    def address_new_oauth2_token(self):
        if not self.qba.new_token:
            return False
        
        self.exa = str(
            datetime.datetime.utcnow() + datetime.timedelta(minutes=55))
        self.cid = self.qba.realm_id
        self.at  = self.qba.access_token
        self.rt  = self.qba.refresh_token

        updater  = {
            "access_token"  : self.at,
            "refresh_token" : self.rt,
            "expires_at"    : self.exa,
            "company_id"    : self.cid
        }

        if self.qba.new_refresh_token:
            # In case we actually need to re-authorize after a year,
            #  let's know when we got the refresh token
            updater["rt_acquired_at"] = str(datetime.datetime.utcnow())

        if not self.ntcbf is None:
            # Make the callback (if available)
            self.ntcbf(updater)

            self.qba.new_token = False

        else:
            if self.vb > 1:
                print("You're refresh token and access token are new.",
                      "Store the new credentials!")

        return True

        
                    
    def query(self, object_type, where_tail=None, count_only=False, **params):
        """
        where_tail example: WHERE Active IN (true,false) ... the syntax is
         SQLike and documented in Intuit's documentation.

        Handles pagination, because that's just no fun at all.
        """
        if len(params) > 0:
            raise NotImplementedError()

        queried_all = False

        select_what = "COUNT(*)" if count_only else "*"
        if where_tail:
            where_tail = " " + where_tail
        else:
            where_tail = ""
        query       = "SELECT {} FROM {}{} MAXRESULTS 1000".format(
            select_what, object_type, where_tail)
        url = "{}/{}/query".format(self.API_BASE_URL, self.cid)

        all_objs = []

        if object_type in self.UNQUERIABLE_OBJECT_TYPES:
            raise Exception("Can't query QB {} objects!".format(object_type))

        base_len = len(query)
        while not queried_all:
            if self.vb > 7:
                print(query)
            resp = self._basic_call("POST", url, data=query)
            if self.vb > 9:
                print(json.dumps(resp, indent=4))

            if not resp or not "QueryResponse" in resp:
                if self.vb > 1:
                    print("Failed query was:")
                    print(query)
                    if resp:
                        if isinstance(resp, (str, dict)) and resp:
                            print(resp)
                        else:
                            print(resp.text)
                            print(resp.status_code)
                raise Exception("Failed QBO Query")

            if count_only:
                return resp["QueryResponse"]["totalCount"]

            objs           = resp["QueryResponse"].get(object_type, [])
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

            if self.vb > 6 and max_results > 0:
                print("Queried {:20s} objects {:>4} through {:4>}.".format(
                    object_type, start_position,
                    start_position + max_results - 1))

            if max_results < 1000:
                queried_all = True

            # This will be the NEXT query:
            query = query[:base_len] + " STARTPOSITION {}".format(
                start_position + 1000)

        return all_objs

    def create(self, object_type, object_dict, **params):
        """
        The object type isn't actually included in the object_dict, which is
         why you also have to pass that in (first).
        """
        url = "{}/{}/{}".format(
            self.API_BASE_URL, self.cid, object_type.lower())

        return self._basic_call("POST", url, data=object_dict, **params)

    def read(self, object_type, object_id, **params):
        """
        Just returns a single object, no questions asked.
        """
        if len(params) > 0:
            raise NotImplementedError()
        url = "{}/{}/{}/{}".format(
            self.API_BASE_URL, self.cid, object_type.lower(), object_id)

        return self._basic_call("GET", url)

    def update(self, object_type, object_dict, **params):
        """
        Unlike with the delete method, you really have to provide the update
         dict when making this call (otherwise the class won't know how you
         want to change the object in question).
        """
        if len(params) > 0:
            raise NotImplementedError()

        url = "{}/{}/{}".format(
            self.API_BASE_URL, self.cid, object_type.lower())

        return self._basic_call("POST", url, data=object_dict)

    def delete(self, object_type, object_id=None, object_dict=None, **params):
        """
        Either provide an object_dict (which will simply be handed to the API
         for deletion) or an object_id. If ONLY an object_id is passed in,
         the method does a read call to GET the object_dict, then performs
         the same as if an object_dict had been passed in
        """
        if len(params) > 0:
            raise NotImplementedError()

        url = "{}/{}/{}".format(
            self.API_BASE_URL, self.cid, object_type.lower())

        if object_id and not object_dict:
            object_dict = self.read(object_type, object_id)[object_type]

        # In the interest of sending as little as possible over the web...
        skinny_dict = {
            "Id"        : object_dict["Id"],
            "SyncToken" : object_dict["SyncToken"]}

        return self._basic_call(
            "POST", url, data=skinny_dict, params={"operation" : "delete"})

    def change_data_capture(self, utc_since, object_types):
        """
        https://developer.intuit.com/docs/api/accounting/ChangeDataCapture

        Note that this only gets you changes from the last 30 days

        object_types should be a list, e.g.
         ["Purchase", "JournalEntry", "Vendor"]

        Watch out for the deletion bug on JournalEntry...it won't tell you about
         deleted JournalEntry objects! QBO-94274 is the bug number (though it's
         not listed in the API documentation's known issues).
        """
        url = "{}/{}/cdc".format(self.API_BASE_URL, self.cid)

        if isinstance(utc_since, datetime.datetime):
            # Either pass in a UTC datetime or a string formatted like this:
            utc_since = utc_since.strftime("%Y-%m-%dT%H:%M:%S+00:00")

        params = {
            "changedSince" : utc_since,
            "entities"     : ",".join(object_types)}

        if self.vb > 7:
            print("CDC Params:")
            print(json.dumps(params, indent=4))

        # This will be a list of dictionaries, each of which relates to
        #  a specific response...
        return self._basic_call("GET", url, params=params)

    def report(self, report_name, **params):
        """
        Use the QBO reporting API, documented here:
        """
        url = "{}/{}/reports/{}".format(
            self.API_BASE_URL, self.cid, report_name)

        if self.vb > 7:
            print(json.dumps(params, indent=4))

        raw = self._basic_call("GET", url, **{"params" : params})

        if not raw:
            print("No json-formatted {} {} report to start with. rp_params:".\
                format(self.cid, report_name))
            print(json.dumps(params, indent=4))
            raise Exception()
        elif not "Header" in list(raw.keys()):
            print(json.dumps(raw, indent=4))
            print("No Header item in raw (above)!?")
            raise Exception()

        if self.vb > 7:
            print(json.dumps(raw["Header"], indent=4))
            print('(raw["Header"] is above)')

        return raw

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
        url       = "{}/{}/upload".format(self.API_BASE_URL, self.cid)
        loc, name = os.path.split(path)
        #base, ext = name.rsplit(".", 1)
        base, ext = os.path.splitext(name)
        mime_type = self.ATTACHABLE_MIME_TYPES.get(ext.lower())
        if not mime_type:
            raise Exception(
                "MIME type for files with extension {}?".format(ext))
        boundary  = "-------------PythonMultipartPost"
        headers   = {
            "Content-Type"    : "multipart/form-data;boundary={}".format(
                boundary),
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
                    {"EntityRef" : {
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
        data = {
            "headers"      : headers.copy(),
            "request_body" : request_body
        }

        return self._basic_call("POST", url, data=data)

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
        loc, name = os.path.split(path)

        if not name:
            name  = fn + ""
        path      = os.path.join(loc, name)

        handle    = open(path, "wb")
        for chunk in requests.get(link).iter_content(1024):
            handle.write(chunk)

        handle.close()

        return path # Because this may have changed if a directory was passed in

    def get_pdf(self, object_type, object_id, path):
        """https://developer.intuit.com/docs/api/accounting/invoice"""
        link   = "{}/{}/{}/{}/pdf".format(
            self.API_BASE_URL, self.cid, object_type.lower(), object_id)

        if self.vb > 4:
            print("Downloading {} {} from {}...".format(
                object_type, object_id, link))

        with open(path, "wb") as handle:
            for chunk in self._basic_call("GET", link).iter_content(1024):
                handle.write(chunk)

            handle.close()

        return link

    def send(self, object_type, object_id, recipient):
        """https://developer.intuit.com/docs/api/accounting/invoice"""
        url   = "{}/{}/{}/{}/send".format(
            self.API_BASE_URL, self.cid, object_type.lower(), object_id)

        if self.vb > 4:
            print("Emailing {} {} to {}...".format(
                object_type, object_id, recipient))

        return self._basic_call(
            "POST", url, **{"params" : {"params" : {"sendTo" : recipient}}})

    def __repr__(self):
        return f"<{self.cid} QBS (OAuth Version {self.oauth_version})>"
