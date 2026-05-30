
class SqlEngineError(Exception):
    pass

class TransactionError(SqlEngineError):
    pass

class OutsideTransactionError(TransactionError):
    pass

class NestedTransactionError(TransactionError):
    pass

class TableDeclarationError(SqlEngineError):
    pass