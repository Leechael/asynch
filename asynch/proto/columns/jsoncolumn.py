import json

from .. import constants
from .base import Column
from .stringcolumn import String


class LegacyJsonColumn(Column):
    py_types = (dict,)

    # No NULL value actually
    null_value = {}

    def __init__(self, column_by_spec_getter, **kwargs):
        self.column_by_spec_getter = column_by_spec_getter
        self.string_column = String(**kwargs)
        super().__init__(**kwargs)

    async def write_state_prefix(self):
        await self.writer.write_uint8(1)

    async def read_items(self, n_items):
        await self.reader.read_uint8()
        spec = await self.reader.read_str()
        col = self.column_by_spec_getter(spec)
        await col.read_state_prefix()
        return await col.read_data(n_items)

    async def write_items(self, items):
        items = [x if isinstance(x, str) else json.dumps(x) for x in items]
        await self.string_column.write_items(items)


class JsonColumn(Column):
    py_types = (dict,)
    null_value = {}

    def __init__(self, column_by_spec_getter, **kwargs):
        self.column_by_spec_getter = column_by_spec_getter
        self.string_column = String(**kwargs)
        self.mode = None
        server_info = getattr(kwargs["context"], "server_info", None)
        server_revision = getattr(
            server_info,
            "revision",
            constants.DBMS_MIN_REVISION_WITH_V2_DYNAMIC_AND_JSON_SERIALIZATION,
        )
        self.write_mode = (
            0
            if server_revision < constants.DBMS_MIN_REVISION_WITH_V2_DYNAMIC_AND_JSON_SERIALIZATION
            else 1
        )
        self.dynamic_paths = []
        self.dynamic_columns = []
        self.shared_data_column = column_by_spec_getter("Map(String, String)")
        super().__init__(**kwargs)

    def prepare_state_prefix(self, items):
        if self.write_mode != 0:
            return

        flattened = [_flatten_json(item) for item in items]
        self.dynamic_paths = sorted({path for item in flattened for path in item})
        self.dynamic_columns = [self.column_by_spec_getter("Dynamic") for _ in self.dynamic_paths]
        for path, column in zip(self.dynamic_paths, self.dynamic_columns):
            column.prepare_state_prefix([item.get(path) for item in flattened])

    async def write_state_prefix(self):
        await self.writer.write_uint64(self.write_mode)
        if self.write_mode == 0:
            await self.writer.write_varint(1024)
            await self.writer.write_varint(len(self.dynamic_paths))
            for path in self.dynamic_paths:
                await self.writer.write_str(path)
            for column in self.dynamic_columns:
                await column.write_state_prefix()
            await self.shared_data_column.write_state_prefix()

    async def write_data(self, items):
        if self.write_mode == 1:
            return await super().write_data(items)

        flattened = [_flatten_json(item) for item in items]
        for path, column in zip(self.dynamic_paths, self.dynamic_columns):
            await column.write_data([item.get(path) for item in flattened])
        await self.shared_data_column.write_data([{} for _ in items])

    async def read_state_prefix(self):
        await super().read_state_prefix()
        serialization_version = await self.reader.read_uint64()
        self.mode = serialization_version

        if serialization_version == 1:
            return

        if serialization_version == 3:
            paths_size = await self.reader.read_varint()
            self.dynamic_paths = [
                await self.reader.read_str(as_bytes=False) for _ in range(paths_size)
            ]
            self.dynamic_columns = [
                self.column_by_spec_getter("Dynamic") for _ in self.dynamic_paths
            ]
            for column in self.dynamic_columns:
                await column.read_state_prefix()
            return

        if serialization_version not in (0, 2, 4):
            raise NotImplementedError(
                f"JSON serialization version {serialization_version} is not supported"
            )

        if serialization_version == 0:
            await self.reader.read_varint()

        dynamic_paths_size = await self.reader.read_varint()
        self.dynamic_paths = [
            await self.reader.read_str(as_bytes=False) for _ in range(dynamic_paths_size)
        ]

        if serialization_version == 4:
            shared_data_version = await self.reader.read_varint()
            if shared_data_version in (1, 2):
                await self.reader.read_varint()

        self.dynamic_columns = [self.column_by_spec_getter("Dynamic") for _ in self.dynamic_paths]
        for column in self.dynamic_columns:
            await column.read_state_prefix()
        await self.shared_data_column.read_state_prefix()

    async def read_data(self, n_items):
        if self.mode == 1:
            return tuple(json.loads(item) for item in await self.string_column.read_items(n_items))

        path_values = [await column.read_data(n_items) for column in self.dynamic_columns]
        result = [dict() for _ in range(n_items)]
        for path, values in zip(self.dynamic_paths, path_values):
            for row, value in enumerate(values):
                if value is not None:
                    _set_json_path(result[row], path, value)

        if self.mode in (0, 2, 4):
            shared_rows = await self.shared_data_column.read_data(n_items)
            for row, shared_values in enumerate(shared_rows):
                for path, value in shared_values.items():
                    try:
                        _set_json_path(result[row], path, json.loads(value))
                    except (TypeError, json.JSONDecodeError):
                        _set_json_path(result[row], path, value)

        return tuple(result)

    async def write_items(self, items):
        items = [x if isinstance(x, str) else json.dumps(x) for x in items]
        await self.string_column.write_items(items)

    async def read_items(self, n_items):
        return await self.read_data(n_items)


def create_json_column(spec, column_by_spec_getter, column_options):
    if spec.startswith("Object('json')"):
        return LegacyJsonColumn(column_by_spec_getter, **column_options)
    return JsonColumn(column_by_spec_getter, **column_options)


def _flatten_json(value, prefix=""):
    flattened = {}
    for key, item in value.items():
        path = f"{prefix}.{key}" if prefix else key
        if isinstance(item, dict):
            flattened.update(_flatten_json(item, path))
        else:
            flattened[path] = item
    return flattened


def _set_json_path(target, path, value):
    parts = path.split(".")
    for part in parts[:-1]:
        target = target.setdefault(part, {})
    target[parts[-1]] = value
