from struct import Struct
from struct import error as struct_error

from ...errors import ColumnTypeMismatchException, StructPackException
from ..streams.buffered import BufferedReader, BufferedWriter


class CommonSerialization:
    def __init__(self, column):
        self.column = column

    async def read_sparse(self, n_items):
        return n_items

    def apply_sparse(self, items):
        return items


class SparseSerialization(CommonSerialization):
    def __init__(self, column):
        self.sparse_indexes = []
        self.items_total = None
        super().__init__(column)

    async def read_sparse(self, n_items):
        sparse_indexes = []
        items_total = 0
        non_default_items = 0

        end_of_granule_flag = 1 << 62
        end_of_granule = False

        while not end_of_granule:
            group_size = await self.column.reader.read_varint()
            end_of_granule = group_size & end_of_granule_flag
            group_size &= ~end_of_granule_flag

            items_total += group_size + 1
            if not end_of_granule:
                non_default_items += 1
                sparse_indexes.append(items_total)

        self.sparse_indexes = sparse_indexes
        self.items_total = items_total

        return non_default_items

    def apply_sparse(self, items):
        default = self.column.null_value
        if self.column.after_read_items:
            default = self.column.after_read_items([default])[0]

        rv = [default] * (self.items_total - 1)
        for item_number, i in enumerate(self.sparse_indexes):
            rv[i - 1] = items[item_number]

        return rv


class Column:
    ch_type = None
    py_types = None

    check_item = None
    after_read_items = None
    before_write_items = None

    types_check_enabled = False

    null_value = 0

    def __init__(
        self,
        reader: BufferedReader,
        writer: BufferedWriter,
        types_check=False,
        has_custom_serialization=False,
        **kwargs,
    ):
        self.writer = writer
        self.reader = reader
        self.nullable = False
        self.types_check_enabled = types_check
        self.has_custom_serialization = has_custom_serialization
        self.serialization = CommonSerialization(self)
        self.input_null_as_default = False
        if "context" in kwargs:
            settings = kwargs["context"].client_settings
            self.input_null_as_default = settings.get("input_format_null_as_default", False)

        super().__init__()

    def make_null_struct(self, n_items):
        return Struct(f"<{n_items}B")

    async def _read_nulls_map(self, n_items):
        s = self.make_null_struct(n_items)
        return s.unpack(await self.reader.read_bytes(s.size))

    async def _write_nulls_map(self, items):
        s = self.make_null_struct(len(items))
        items = [x is None for x in items]
        await self.writer.write_bytes(s.pack(*items))

    def check_item_type(self, value):
        if not isinstance(value, self.py_types):
            raise ColumnTypeMismatchException(value)

    def prepare_items(self, items):
        nullable = self.nullable
        null_value = self.null_value
        null_as_default = self.input_null_as_default

        check_item = self.check_item
        if self.types_check_enabled:
            check_item_type = self.check_item_type
        else:
            check_item_type = False

        if (
            not (self.nullable or null_as_default)
            and not check_item_type
            and not check_item
            and not self.before_write_items
        ):
            return items

        nulls_map = [False] * len(items) if self.nullable else None
        for i, x in enumerate(items):
            if x is None:
                if nullable:
                    nulls_map[i] = True
                    x = null_value
                elif null_as_default:
                    x = null_value

            else:
                if check_item_type:
                    check_item_type(x)

                if check_item:
                    check_item(x)

            items[i] = x

        if self.before_write_items:
            self.before_write_items(items, nulls_map=nulls_map)

        return items

    async def write_data(self, items):
        if self.nullable:
            await self._write_nulls_map(items)

        await self._write_data(items)

    async def _write_data(
        self,
        items,
    ):
        prepared = self.prepare_items(items)
        await self.write_items(prepared)

    async def write_items(self, items):
        raise NotImplementedError

    async def read_data(self, n_items):
        n_items = await self.serialization.read_sparse(n_items)

        if self.nullable:
            nulls_map = await self._read_nulls_map(n_items)
        else:
            nulls_map = None

        items = await self._read_data(n_items, nulls_map=nulls_map)
        return self.serialization.apply_sparse(items)

    async def _read_data(self, n_items, nulls_map=None):
        items = await self.read_items(
            n_items,
        )

        if self.after_read_items:
            return self.after_read_items(items, nulls_map)
        elif nulls_map is not None:
            return tuple((None if is_null else items[i]) for i, is_null in enumerate(nulls_map))
        return items

    async def read_items(self, n_items):
        raise NotImplementedError

    async def read_state_prefix(self):
        if self.has_custom_serialization:
            use_custom_serialization = await self.reader.read_varint()
            if use_custom_serialization:
                self.serialization = SparseSerialization(self)

    async def write_state_prefix(self):
        pass

    def prepare_state_prefix(self, items):
        pass


class FormatColumn(Column):
    """
    Uses struct.pack for bulk items writing.
    """

    format = None

    def make_struct(self, n_items):
        return Struct(f"<{n_items}{self.format}")

    async def write_items(
        self,
        items,
    ):
        s = self.make_struct(len(items))
        try:
            await self.writer.write_bytes(s.pack(*items))

        except struct_error as e:
            raise StructPackException(e)

    async def read_items(
        self,
        n_items,
    ):
        s = self.make_struct(n_items)
        unpack_data = s.unpack(await self.reader.read_bytes(s.size))
        return unpack_data


# How to write new column?
# - Check ClickHouse documentation for column
# - Wireshark and tcpdump are your friends.
# - Use `clickhouse-client --compression 0` to see what's going on data
#   transmission.
# - Check for similar existing columns and tests.
# - Use `FormatColumn` for columns that use "simple" types under the hood.
# - Some columns have before_write and after_read hooks.
#   Use them to convert items in column into "simple" types.
