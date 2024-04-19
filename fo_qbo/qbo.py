import datetime
import pytz
import re

# TODO:
#  This already exists in the finoptimal package for the most part. Both classes were meant to phase out all the
#  constants stuffed inside QBOSesh alone (because other classes need that data, too).
#  THIS class should be the solve survivor, because we need to reference this in the fo_qbo package and I want to
#  avoid all the potential circular import problems.


class QBO:
    """A central resource for commonly used QBO attributes."""

    NAME = "QBO"
    LEDGER_TYPE = "General"
    MINOR_API_VERSION = 65

    EPOCH = "1980-01-01T00:00:00.000000"
    TIME_FORMATTER = "%Y-%m-%dT%H:%M:%S.%f"
    EPOCH_DT = pytz.utc.localize(datetime.datetime.strptime(EPOCH, TIME_FORMATTER))

    NAME_LIST_OBJECTS = [
        "Account",
        "Budget",
        "Class",
        "CompanyCurrency",
        "Customer",
        "Department",
        "Employee",
        "Item",
        "PaymentMethod",
        "TaxAgency",
        "TaxCode",
        "TaxRate",
        "TaxService",
        "Term",
        "Vendor",
    ]

    # entity is the table name used by QBO for all of these
    ENTITY_OBJECTS = ["Customer", "Employee", "Vendor"]

    # QBO refers to these as Entry objects and includes the NON_POSTING_TRANSACTION_OBJECTS below. QBO calls
    # TimeActivity a transaction object, but FO does not.
    TRANSACTION_OBJECTS = [
        "Bill",
        "BillPayment",
        "CreditMemo",
        "CreditCardPayment",
        "Deposit",
        "Invoice",
        "JournalEntry",
        "Payment",
        "Purchase",
        "RefundReceipt",
        "SalesReceipt",
        "Transfer",
        "VendorCredit",
    ]

    NON_POSTING_TRANSACTION_OBJECTS = ["Estimate", "ReimburseCharge", "PurchaseOrder"]

    ENTRY_OBJECTS = TRANSACTION_OBJECTS + NON_POSTING_TRANSACTION_OBJECTS
    POSTING_TRANSACTION_OBJECTS = list(set(TRANSACTION_OBJECTS).difference(NON_POSTING_TRANSACTION_OBJECTS))

    OTHER_OBJECTS = [
        # "CompanyInfo",   # query by Metadata.LastUpdatedTime doesn't work!
        "Attachable",
        "CompanyCurrency",
        "Estimate",        # Intuit calls this a txn, FO doesn't...
        "ReimburseCharge",
        "PurchaseOrder",   # Intuit calls this a txn, FO doesn't...
        "TimeActivity",    # Intuit calls this a txn, FO doesn't...
    ]

    OBJECT_TYPES = NAME_LIST_OBJECTS + TRANSACTION_OBJECTS + OTHER_OBJECTS
    CACHE_LIFESPAN = datetime.timedelta(hours=72)
    MAX_OBJS_IN_CDC = 300
    NOT_IMPLEMENTED_TYPES = ["TaxAgency", "TaxRate", "TaxService"]

    NATIVE_TYPES = {
        "Account"    : "Account",
        "Class"      : "Class",
        "Department" : "Department",
        "Item"       : "Item",
        "Term"       : "Term"
    }

    NATIVE_TYPES_R = {vl : ky for ky, vl in NATIVE_TYPES.items()}
    SPECIAL_ACCOUNTS = ["<QBO TAX ACCOUNT>"]

    # QBO does not support Change Data Capture operations for these API entities (also JournalCode)
    BROKEN_CDC_OBJECTS = ["TaxAgency", "TaxCode", "TaxRate", "TimeActivity"]

    UNCACHABLE_OBJECTS = ["ExchangeRate"]

    CDC_OBJECTS = list(set(OBJECT_TYPES)
                       .difference(BROKEN_CDC_OBJECTS)
                       .difference(NOT_IMPLEMENTED_TYPES)
                       .difference(UNCACHABLE_OBJECTS))

    CACHABLE_OBJECTS = list(
        set(CDC_OBJECTS + BROKEN_CDC_OBJECTS) - set(NOT_IMPLEMENTED_TYPES) - set(UNCACHABLE_OBJECTS)
    )

    MASK_NAMES = {
        "Account"       : "account",
        "Class"         : "class",
        "Customer"      : "name",
        "Department"    : "department",
        "Employee"      : "name",
        "Item"          : "item",
        "Vendor"        : "name",
        "Term"          : "term",
        "PaymentMethod" : "paymeth",
    }

    # Transaction mask attributes
    TXN_MASK_ATTRS = {
        "header_account"       : "account",
        "header_name"          : "name",
        "line_account"         : "account",
        "line_class"           : "class",
        "line_department"      : "department",
        "line_item"            : "item",
        "line_name"            : "name",
        "entry_ref_s"          : "term",
        "entry_payment_method" : "paymeth",
    }

    # Entity mask attributes
    ENT_MASK_ATTRS = {
        "Account": {
            "account_parent"         : "account",
        },
        "Class": {
            "class_parent"           : "class",
        },
        "Customer": {
            "entity_parent"          : "name",
            "entity_terms"           : "term",
            "entity_payment_method"  : "paymeth",
        },
        "Department": {
            "department_parent"      : "department",
        },
        "Item": {
            "item_parent"            : "item",
            "item_income_account"    : "account",
            "item_expense_account"   : "account",
            "item_preferred_vendor"  : "name",
        },
        "TimeActivity": {
            "timeactivity_by"        : "name",
            "timeactivity_for"       : "name",
            "timeactivity_item"      : "item",
            "timeactivity_class"     : "class",
            "timeactivity_department": "department",
        },
    }

    _OBJECT_TYPES = {
        "account"    : "Account",
        "class"      : "Class",
        "department" : "Department",
        "item"       : "Item",
        "term"       : "Term",
        "paymeth"    : "PaymentMethod",
    }

    OBJECT_ALIASES = {
        "CreditCardPayment": "CreditCardPaymentTxn",
    }

    # This maps the object types to QBO's primary key column, which is the column retrieved from QBO Change Data
    # Capture when the objects get "deleted".
    OBJECT_ID_MAP = {
        'Account'           : 'account_id',
        'Attachable'        : 'attachable_id',
        'Bill'              : 'entry_id',
        'BillPayment'       : 'entry_id',
        'Budget'            : 'budget_id',
        'Class'             : 'class_id',
        'CompanyCurrency'   : 'currency_id',
        'CreditCardPayment' : 'entry_id',
        'CreditMemo'        : 'entry_id',
        'Customer'          : 'entity_id',
        'Department'        : 'department_id',
        'Deposit'           : 'entry_id',
        'Employee'          : 'entity_id',
        'Estimate'          : 'entry_id',
        'Invoice'           : 'entry_id',
        'Item'              : 'item_id',
        'JournalEntry'      : 'entry_id',
        'Payment'           : 'entry_id',
        'PaymentMethod'     : 'paymeth_id',
        'Purchase'          : 'entry_id',
        'PurchaseOrder'     : 'entry_id',
        'RefundReceipt'     : 'entry_id',
        'ReimburseCharge'   : 'entry_id',
        'SalesReceipt'      : 'entry_id',
        'TaxCode'           : 'taxcode_id',
        'Term'              : 'term_id',
        'TimeActivity'      : 'timeactivity_id',
        'Transfer'          : 'entry_id',
        'Vendor'            : 'entity_id',
        'VendorCredit'      : 'entry_id'
    }

    MERGE_EVENT_COLUMN_MAP = {
        'Account': {
            'stale_columns': ['line_account', 'header_account', 'entry_deposit_account'],
            'name_column': 'account_name',
            'id_column': 'account_id'
        },
        'Department': {
            'stale_columns': ['line_department'],
            'name_column': 'department_name',
            'id_column': 'department_id'
        },
        'Class': {
            'stale_columns': ['line_class'],
            'name_column': 'class_name',
            'id_column': 'class_id'
        },
        'Item': {
            'stale_columns': ['line_item'],
            'name_column': 'item_name',
            'id_column': 'item_id'
        },
        'Vendor': {
            'stale_columns': ['header_name', 'line_name'],
            'name_column': 'entity_fqn',
            'id_column': 'entity_id'
        },
        'Employee': {
            'stale_columns': ['header_name', 'line_name'],
            'name_column': 'entity_fqn',
            'id_column': 'entity_id'
        },
        'Customer': {
            'stale_columns': ['header_name', 'line_name'],
            'name_column': 'entity_fqn',
            'id_column': 'entity_id'
        },
    }


    @classmethod
    def split_words(cls, cap_word: str) -> list:
        return re.findall(r'[A-Z][a-z]+', cap_word)

    @classmethod
    def get_table_name(cls, object_type: str) -> str:
        """Get the database table name for the QBO API object.

        Parameters
        ----------
        object_type : str

        Returns
        -------
        str
        """
        if cls.OBJECT_ID_MAP.get(object_type) == 'entry_id':
            # Entry-type (transaction) objects share a table
            table_name = f'{cls.NAME}_entry'

        elif cls.OBJECT_ID_MAP.get(object_type) == 'entity_id':
            # Entity-type objects share a table
            table_name = f'{cls.NAME}_entity'

        else:
            words = cls.split_words(object_type)
            words = [cls.NAME] + words

            table_name = '_'.join(words)

        return table_name.lower()

    @classmethod
    def get_model_name(cls, object_type: str) -> str:
        """Get the database model name for the QBO API object.

        Parameters
        ----------
        object_type : str

        Returns
        -------
        str
        """
        if object_type in cls.ENTRY_OBJECTS:
            return f'{cls.NAME}Entry'

        if object_type in cls.ENTITY_OBJECTS:
            return f'{cls.NAME}Entity'

        return f'{cls.NAME}{object_type}'

    @classmethod
    def get_object_id(cls, object_type: str) -> str:
        return cls.OBJECT_ID_MAP.get(object_type)
