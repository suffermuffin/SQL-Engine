
class SqlEngineError(Exception):
    """ Errors linked to sqlengine module """
    pass

class TransactionError(SqlEngineError):
    """ Errors within transactions """
    pass

class OutsideTransactionError(TransactionError):
    """ Errors of prohibited outside tranasctions operations """
    pass

class NestedTransactionError(TransactionError):
    """ Errors of nested transaction operations """
    pass

class TableDeclarationError(SqlEngineError):
    """ Errors of table declaration """
    pass