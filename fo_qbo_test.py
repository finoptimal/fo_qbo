#!/usr/bin/env python

import argparse, json, time
import fo_qbo

parser = argparse.ArgumentParser()

parser.add_argument("-at", "--access_token_creds", 
                    type=str,
                    nargs=2,
                    default=None,
                    help="token then secret")

parser.add_argument("-cc", "--consumer_creds", 
                    type=str,
                    nargs=2,
                    default=None,
                    help="key then secret")

parser.add_argument("-ci", "--company_id", 
                    type=str,
                    default=None,
                    help="Don't provide for a new connection")

parser.add_argument("-c", "--create", 
                    type=str,
                    default=None,
                    help="creates an Account by this name")

parser.add_argument("-d", "--delete", 
                    action="store_true",
                    default=False,
                    help="Create and THEN delete a purchase object")

parser.add_argument("-q", "--query",
                    type=str,
                    nargs="*",
                    default=None,
                    help="object_type, then optional where_tail, " + \
                    "then ANYTHING (which signals count only)")

parser.add_argument("-r", "--read", 
                    type=str,
                    nargs=2,
                    default=None,
                    help="object_type and object_id")

parser.add_argument("-u", "--update",
                    type=str,
                    default=None,
                    help="toggles active/inactive for Account with this name")

parser.add_argument("-v", "--verbosity", 
                    type=int,
                    default=1,
                    help="How loud to be")

if __name__=='__main__':
    start = time.time()
    args = parser.parse_args()

    sesh = fo_qbo.QBS(
        args.consumer_creds[0], args.consumer_creds[1],
        access_token=args.access_token_creds[0],
        access_token_secret=args.access_token_creds[1],
        company_id=args.company_id, verbosity=args.verbosity)

    if args.query:
        rd = sesh.query(*args.query)
        print json.dumps(rd, indent=4)        

    if args.create:
        print "This test creates a basic new account"
        object_dict = {"Name"           : args.create,
                       "AccountSubType" : "TrustAccounts"}
        rd = sesh.create("Account", object_dict)
        print json.dumps(rd, indent=4)
        
    if args.read:
        rd = sesh.read(*args.read)
        print json.dumps(rd, indent=4)

    if args.update:
        print "This just toggles active / inactive for the first found account."
        accts = sesh.query("Account")
        for acct in accts["QueryResponse"]["Account"]:
            if acct["Name"] == args.update:
                if acct["Active"]:
                    acct["Active"] = False
                else:
                    acct["Active"] = True
                break
        rd    = sesh.update("Account", acct)
        print json.dumps(rd, indent=4)
                    
    if args.delete:
        # Find or create a bank account called "FinOptimal Rocks Bank"
        accts = sesh.query("Account")
        found = False

        for acct in accts["QueryResponse"]["Account"]:
            if acct["Name"] == "FinOptimal Rocks Bank":
                av =  acct["Id"]
                found = True
                break
        if not found:
            av = sesh.create("Account", {
                "Name"           : "FinOptimal Rocks Bank",
                "AccountSubType" : "Checking"})
            import ipbd;ipdb.set_trace()
            
        print "Creating a Purchase object and then deleting it..."

        purch_dict =  {
            "AccountRef": {
                "value": av,
            },
            "PaymentType": "Cash",
            "TotalAmt": 1.23,
            "domain": "QBO",
            "sparse": False,
            "TxnDate": "2016-12-31",
            "CurrencyRef": {
                "value": "USD",
                "name": "United States Dollar"
            },
            "Line": [
                {
                    "Id": "1",
                    "Description": "Test Line",
                    "Amount": 1.23,
                    "DetailType": "AccountBasedExpenseLineDetail",
                    "AccountBasedExpenseLineDetail": {
                        "AccountRef": {
                            "value": av,
                        },
                        "BillableStatus": "NotBillable",
                        "TaxCodeRef": {
                            "value": "NON"
                        }
                    }
                }
            ]
        }

        deletable_purch_dict = sesh.create(
            "Purchase", purch_dict)["Purchase"]
        
        print json.dumps(deletable_purch_dict, indent=4)
        print "Now deleting the above newly-created Purchase object..."
        
        rd = sesh.delete("Purchase", deletable_purch_dict["Id"])
        print json.dumps(rd, indent=4)

    end = time.time()

    if args.verbosity > 0:
        print "Running time: {:.2f} seconds.".format(end-start)
