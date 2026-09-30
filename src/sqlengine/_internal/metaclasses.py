from typing import get_origin, get_args

from ..exceptions import TableDeclarationError
from .types import Primary

class SqlTableMeta(type):

    def __init__(cls, name, bases, namespace, **kwargs):
        super().__init__(name, bases, namespace, **kwargs)

        if not bases or bases == (object,):
            return

        annotations = namespace.get('__annotations__', {})

        tablename = cls.__tablename__ if \
            hasattr(cls, "__tablename__") and cls.__tablename__ is not None\
            else cls.__name__

        columns = list(getattr(cls, "__columns__", []))
        types   = list(getattr(cls, "__types__",   []))
        primary = list(getattr(cls, "__primary__", []))

        for name, type_ in annotations.items():
            if name.startswith("_") or name.endswith("_"):
                continue
            
            if name in columns:
                raise TableDeclarationError(f"Annotated column `{name}` is already in __columns__")
            
            columns.append(name)
            
            if not get_origin(type_) == Primary:
                types.append(type_)
                continue

            if name in primary:
                raise TableDeclarationError(f"Annotated primary column `{name}` is already in __primary__")
            
            primary_type = get_args(type_)[0]

            if not primary_type:
                raise TableDeclarationError(("Primary type was declared without the type. "
                "Usage: `my_column : Primary[T]`, where T is desired type"))
            
            types.append(primary_type)
            primary.append(name)
            
        
        cls.__tablename__ = tablename
        cls.__columns__ = columns
        cls.__types__   = types
        cls.__primary__ = primary
        cls._validate_attributes()


    def _validate_attributes(self) -> None:

        missing_attrs = [
            attr for attr in 
            [ "__columns__", "__types__", "__primary__", "__tablename__"]
            if not hasattr(self, attr)
        ]

        if missing_attrs:
            raise TableDeclarationError(f'{self.__name__} is missing attributes: {missing_attrs}')
        
        n_types, n_cols = len(self.__types__), len(self.__columns__)

        if not n_types == n_cols:
            raise TableDeclarationError((f'`__types__` and `__columns__`: length mismatch: types = {n_types}, columns = {n_cols}'))
        
        if len(self.__primary__) < 1:
            raise TableDeclarationError(f'`__primary__`: Number of primary keys must be at least 1')
        
        wrong_primaries = [
            prim for prim in self.__primary__ if
            prim not in self.__columns__
        ]

        if wrong_primaries:
            raise TableDeclarationError(f'`__primary__`: Keys {wrong_primaries} can\'t be primaries as they are not declared in __columns__')
