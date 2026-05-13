from ...errors import (
    ColumnTypeMismatchException,
    StructPackException,
    TypeMismatchError,
    UnknownTypeError,
)
from ..streams.buffered import BufferedReader, BufferedWriter
from .arraycolumn import create_array_column
from .bfloat16column import BFloat16Column
from .boolcolumn import BoolColumn
from .datecolumn import Date32Column, DateColumn
from .datetimecolumn import create_datetime_column
from .decimalcolumn import create_decimal_column
from .enumcolumn import create_enum_column
from .floatcolumn import Float32, Float64
from .intcolumn import (
    Int8Column,
    Int16Column,
    Int32Column,
    Int64Column,
    Int128Column,
    Int256Column,
    UInt8Column,
    UInt16Column,
    UInt32Column,
    UInt64Column,
    UInt128Column,
    UInt256Column,
)
from .intervalcolumn import (
    IntervalDayColumn,
    IntervalHourColumn,
    IntervalMicrosecondColumn,
    IntervalMillisecondColumn,
    IntervalMinuteColumn,
    IntervalMonthColumn,
    IntervalNanosecondColumn,
    IntervalQuarterColumn,
    IntervalSecondColumn,
    IntervalWeekColumn,
    IntervalYearColumn,
)
from .ipcolumn import IPv4Column, IPv6Column
from .jsoncolumn import create_json_column
from .lowcardinalitycolumn import create_low_cardinality_column
from .mapcolumn import create_map_column
from .nestedcolumn import create_nested_column
from .nothingcolumn import NothingColumn
from .nullablecolumn import create_nullable_column
from .nullcolumn import NullColumn
from .simpleaggregatefunctioncolumn import create_simple_aggregate_function_column
from .stringcolumn import create_string_column
from .timecolumn import create_time_column
from .tuplecolumn import create_tuple_column
from .uuidcolumn import UUIDColumn

column_by_type = {
    c.ch_type: c
    for c in [
        DateColumn,
        Date32Column,
        BFloat16Column,
        Float32,
        Float64,
        BoolColumn,
        Int8Column,
        Int16Column,
        Int32Column,
        Int64Column,
        Int128Column,
        Int256Column,
        UInt8Column,
        UInt16Column,
        UInt32Column,
        UInt64Column,
        UInt128Column,
        UInt256Column,
        NothingColumn,
        NullColumn,
        UUIDColumn,
        IntervalNanosecondColumn,
        IntervalMicrosecondColumn,
        IntervalMillisecondColumn,
        IntervalYearColumn,
        IntervalMonthColumn,
        IntervalQuarterColumn,
        IntervalWeekColumn,
        IntervalDayColumn,
        IntervalHourColumn,
        IntervalMinuteColumn,
        IntervalSecondColumn,
        IPv4Column,
        IPv6Column,
    ]
}


type_aliases = {
    "TINYINT": "Int8",
    "INT1": "Int8",
    "BYTE": "Int8",
    "TINYINT SIGNED": "Int8",
    "INT1 SIGNED": "Int8",
    "SMALLINT": "Int16",
    "SMALLINT SIGNED": "Int16",
    "INT": "Int32",
    "INTEGER": "Int32",
    "MEDIUMINT": "Int32",
    "MEDIUMINT SIGNED": "Int32",
    "INT SIGNED": "Int32",
    "INTEGER SIGNED": "Int32",
    "BIGINT": "Int64",
    "SIGNED": "Int64",
    "BIGINT SIGNED": "Int64",
    "TINYINT UNSIGNED": "UInt8",
    "INT1 UNSIGNED": "UInt8",
    "SMALLINT UNSIGNED": "UInt16",
    "YEAR": "UInt16",
    "MEDIUMINT UNSIGNED": "UInt32",
    "INT UNSIGNED": "UInt32",
    "INTEGER UNSIGNED": "UInt32",
    "UNSIGNED": "UInt64",
    "BIGINT UNSIGNED": "UInt64",
    "BIT": "UInt64",
    "SET": "UInt64",
    "FLOAT": "Float32",
    "REAL": "Float32",
    "SINGLE": "Float32",
    "DOUBLE": "Float64",
    "DOUBLE PRECISION": "Float64",
    "BOOL": "Bool",
    "BOOLEAN": "Bool",
    "CHAR": "String",
    "NCHAR": "String",
    "CHARACTER": "String",
    "VARCHAR": "String",
    "NVARCHAR": "String",
    "VARCHAR2": "String",
    "TEXT": "String",
    "TINYTEXT": "String",
    "MEDIUMTEXT": "String",
    "LONGTEXT": "String",
    "BLOB": "String",
    "CLOB": "String",
    "TINYBLOB": "String",
    "MEDIUMBLOB": "String",
    "LONGBLOB": "String",
    "BYTEA": "String",
    "CHARACTER LARGE OBJECT": "String",
    "CHARACTER VARYING": "String",
    "CHAR LARGE OBJECT": "String",
    "CHAR VARYING": "String",
    "NATIONAL CHAR": "String",
    "NATIONAL CHARACTER": "String",
    "NATIONAL CHARACTER LARGE OBJECT": "String",
    "NATIONAL CHARACTER VARYING": "String",
    "NATIONAL CHAR VARYING": "String",
    "NCHAR VARYING": "String",
    "NCHAR LARGE OBJECT": "String",
    "BINARY LARGE OBJECT": "String",
    "BINARY VARYING": "String",
    "VARBINARY": "String",
    "BINARY": "FixedString",
    "DEC": "Decimal",
    "NUMERIC": "Decimal",
    "FIXED": "Decimal",
    "TIMESTAMP": "DateTime",
    "INET4": "IPv4",
    "INET6": "IPv6",
    "ENUM": "Enum",
}

aliases = [
    # Begin Geo types
    ("Point", "Tuple(Float64, Float64)"),
    ("LineString", "Array(Point)"),
    ("MultiLineString", "Array(LineString)"),
    ("Ring", "Array(Point)"),
    ("Polygon", "Array(Ring)"),
    ("MultiPolygon", "Array(Polygon)"),
    # End Geo types
]


def normalize_spec_alias(spec):
    alias = type_aliases.get(spec.upper())
    if alias:
        return alias

    for alias, primitive in type_aliases.items():
        prefix = alias + "("
        if spec.upper().startswith(prefix):
            if primitive == "String":
                return "String"
            return primitive + spec[len(alias) :]

    return spec


def get_column_by_spec(spec, column_options):
    def create_column_with_options(x):
        return get_column_by_spec(x, column_options)

    spec = normalize_spec_alias(spec)

    if spec == "String" or spec.startswith("FixedString"):
        return create_string_column(spec, column_options)

    elif spec.startswith("Enum"):
        return create_enum_column(spec, column_options)

    elif spec.startswith("DateTime"):
        return create_datetime_column(spec, column_options)

    elif spec == "Time" or spec.startswith("Time64"):
        return create_time_column(spec, column_options)

    elif spec.startswith("Decimal"):
        return create_decimal_column(spec, column_options)

    elif spec.startswith("Array"):
        return create_array_column(spec, create_column_with_options, column_options)

    elif spec.startswith("Tuple"):
        return create_tuple_column(spec, create_column_with_options, column_options)

    elif spec.startswith("Nested"):
        return create_nested_column(spec, create_column_with_options, column_options)

    elif spec.startswith("Nullable"):
        return create_nullable_column(spec, create_column_with_options)

    elif spec.startswith("LowCardinality"):
        return create_low_cardinality_column(spec, create_column_with_options, column_options)

    elif spec.startswith("SimpleAggregateFunction"):
        return create_simple_aggregate_function_column(spec, create_column_with_options)

    elif spec.startswith("Map"):
        return create_map_column(spec, create_column_with_options, column_options)
    elif spec.startswith("Object('json')"):
        return create_json_column(spec, create_column_with_options, column_options)
    else:
        for alias, primitive in aliases:
            if spec.startswith(alias):
                return create_column_with_options(primitive + spec[len(alias) :])  # noqa: E203

        try:
            cls = column_by_type[spec]
            return cls(**column_options)

        except KeyError as e:
            raise UnknownTypeError(f"Unknown type {e.args[0]}")


async def read_column(
    reader: BufferedReader,
    writer: BufferedWriter,
    context,
    column_spec,
    n_items,
    has_custom_serialization=False,
):
    column_options = {
        "context": context,
        "reader": reader,
        "writer": writer,
        "has_custom_serialization": has_custom_serialization,
    }
    column = get_column_by_spec(column_spec, column_options)
    await column.read_state_prefix()
    return await column.read_data(
        n_items,
    )


async def write_column(
    reader: BufferedReader,
    writer: BufferedWriter,
    context,
    column_name,
    column_spec,
    items,
    types_check=False,
):
    column_options = {
        "context": context,
        "types_check": types_check,
        "reader": reader,
        "writer": writer,
    }
    column = get_column_by_spec(column_spec, column_options)

    try:
        await column.write_state_prefix()
        await column.write_data(items)

    except ColumnTypeMismatchException as e:
        err_arg = e.args[0]
        raise TypeMismatchError(
            "Type mismatch in VALUES section. "
            f"Expected {column_spec} got {type(err_arg)}: "
            f'{err_arg} for column "{column_name}".'
        )

    except (StructPackException, OverflowError) as e:
        error = e.args[0]
        raise TypeMismatchError(
            "Type mismatch in VALUES section. "
            "Repeat query with types_check=True for detailed info. "
            "Column {}: {}".format(column_name, str(error))
        )
