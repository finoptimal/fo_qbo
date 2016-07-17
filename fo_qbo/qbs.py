"""
Wrap the QBO v3 (and hopefully v4) REST API. The API supports json as well as 
 xml, but this wrapper ONLY supports json-formatted messages.

https://developer.intuit.com/docs/api/accounting
https://developer.intuit.com/docs/0100_accounting/0400_references/reports
https://developer.intuit.com/v2/apiexplorer

Please contact developer@finoptimal.com with questions or comments.
"""

from rauth import OAuth1Session
import json, time

def retry(max_tries=10, delay_secs=0.1):
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
            
            while True:
                try:
                    return retriable_function(*args, **kwargs)
                    break
                except:
                    tries -= 1
                    if tries <= 0:
                        raise
                    time.sleep(delay)
                    
        return inner
    return decorator

class QBS(object):
    """
    Basic unit of engagement, an rauth OAuth1Session wrapper.
    """
    API_BASE_URL = "https://quickbooks.api.intuit.com/v3"
    
    def __init__(self, consumer_key, consumer_secret,
                 access_token=None, access_token_secret=None, company_id=None,
                 connector_callback=None, reconnector_callback=None,
                 verbosity=0):
        """
        You must have (developer) consumer credentials (key + secret) to use
         this module.

        It only works with a single company_id at a time.
        """
        self.ck  = consumer_key
        self.cs  = consumer_secret

        self.at  = access_token
        self.ats = access_token_secret

        self.cid = company_id
        self.vb  = verbosity

        self.ccb = connector_callback
        self.rcb = reconnector_callback
        
        self._setup()

    def _setup(self):
        """
        Make sure the access_token is fresh (otherwise reconnect). If there's NO
         access_token (and, of course, accompanying secret), go through the
         connect workflow.
        """
        # To do: check token freshness, reconnecting if necessary
        # To do: initiate and process token request if no self.at yet

        # At this point, we're assumed to have a fresh, working access_token
        self.sess = OAuth1Session(self.ck, self.cs, self.at, self.ats)

    @retry(max_tries=1)
    def _basic_call(self, request_type, url, data=None, **params):
        """
        For query results, this handles pagination...no one wants to do that.

        params often get used for the Reports API, not for CRUD ops.
        """
        headers  = {"accept" : "application/json"}

        if request_type.lower() in ["post"]:
            if isinstance(data, dict):
                headers["Content-Type"] = "application/json"
                data = json.dumps(data)
            else:
                # (basically for queries only)
                headers["Content-Type"] = "application/text"

        if self.vb > 9:
            print json.dumps(data, indent=4)
            print "Above is the request body about to go here:"
            print url
            raw_input("<press any key to continue>")
                
        response = self.sess.request(
            request_type.upper(), url, header_auth=True, realm=self.cid,
            verify=True, headers=headers, data=data, **params)

        if response.status_code in [200]:
            return response.json()
        
        if self.vb > 4:
            try:
                print json.dumps(response.json(), indent=4)
            except:
                print response.text
        
    def query(self, object_type, where_tail=None, count_only=False):
        """
        where_tail example: WHERE Active IN (true,false) ... the syntax is
         SQLike and documented in Intuit's documentation.
        """
        select_what = "COUNT(*)" if count_only else "*"
        if not where_tail:
            where_tail = ""
        query       = "SELECT {} FROM {}{}".format(
            select_what, object_type, where_tail)
        url = "{}/company/{}/query".format(self.API_BASE_URL, self.cid)

        return self._basic_call("POST", url, data=query)
        
    def create(self, object_type, object_dict):
        """
        The object type isn't actually included in the object_dict, which is
         why you also have to pass that in (first).
        """
        url = "{}/company/{}/{}".format(
            self.API_BASE_URL, self.cid, object_type.lower())

        return self._basic_call("POST", url, data=object_dict)
        
    def read(self, object_type, object_id):
        """
        Just returns a single object, no questions asked.
        """
        url = "{}/company/{}/{}/{}".format(
            self.API_BASE_URL, self.cid, object_type.lower(), object_id)
        
        return self._basic_call("GET", url)
        
    def update(self, object_type, object_dict):
        """
        Unlike with the delete method, you really have to provide the update
         dict when making this call (otherwise the class won't know how you
         want to change the object in question).
        """
        url = "{}/company/{}/{}".format(
            self.API_BASE_URL, self.cid, object_type.lower())

        return self._basic_call("POST", url, data=object_dict)

    def delete(self, object_type, object_id=None, object_dict=None):
        """
        Either provide an object_dict (which will simply be handed to the API
         for deletion) or an object_id. If ONLY an object_id is passed in,
         the method does a read call to GET the object_dict, then performs
         the same as if an object_dict had been passed in
        """
        url = "{}/company/{}/{}".format(
            self.API_BASE_URL, self.cid, object_type.lower())

        if object_id and not object_dict:
            object_dict = self.read(object_type, object_id)[object_type]

        # In the interest of sending as little as possible over the web...
        skinny_dict = {
            "Id"        : object_dict["Id"],
            "SyncToken" : object_dict["SyncToken"]}
        
        return self._basic_call(
            "POST", url, data=skinny_dict, params={"operation" : "delete"})
