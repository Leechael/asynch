from .base import Column
from .stringcolumn import ByteString
from .variantcolumn import VariantColumn

DYNAMIC_SERIALIZATION_VERSION_V1 = 1
DYNAMIC_SERIALIZATION_VERSION_V2 = 2
SHARED_VARIANT = "SharedVariant"


class DynamicColumn(Column):
    py_types = (object,)
    null_value = None

    def __init__(self, column_by_spec_getter, **kwargs):
        self.column_by_spec_getter = column_by_spec_getter
        self.column_options = dict(kwargs)
        self.variant_column = None
        super().__init__(**kwargs)

    async def read_state_prefix(self):
        await super().read_state_prefix()
        version = await self.reader.read_uint64()
        if version not in (DYNAMIC_SERIALIZATION_VERSION_V1, DYNAMIC_SERIALIZATION_VERSION_V2):
            raise NotImplementedError(f"Dynamic serialization version {version} is not supported")

        if version == DYNAMIC_SERIALIZATION_VERSION_V1:
            await self.reader.read_varint()  # Historical max_dynamic_types parameter.
        num_dynamic_types = await self.reader.read_varint()
        dynamic_specs = [
            (await self.reader.read_str(as_bytes=False)).strip() for _ in range(num_dynamic_types)
        ]
        self.variant_column = self._make_variant_column(dynamic_specs)
        await self.variant_column.read_state_prefix()

    async def write_data(self, items):
        dynamic_specs, tagged_items = self._prepare_dynamic_items(items)
        await self.writer.write_uint64(DYNAMIC_SERIALIZATION_VERSION_V1)
        await self.writer.write_varint(len(dynamic_specs))
        await self.writer.write_varint(len(dynamic_specs))
        for spec in dynamic_specs:
            await self.writer.write_str(spec)

        self.variant_column = self._make_variant_column(dynamic_specs)
        await self.variant_column.write_state_prefix()
        await self.variant_column.write_data(tagged_items)

    async def read_data(self, n_items):
        items = await self.variant_column.read_data(n_items)
        return tuple(item if not isinstance(item, bytes) else item for item in items)

    async def write_items(self, items):
        return await self.write_data(items)

    async def read_items(self, n_items):
        return await self.read_data(n_items)

    def _make_variant_column(self, dynamic_specs):
        variant_specs = sorted([SHARED_VARIANT, *dynamic_specs])
        nested_columns = []
        for spec in variant_specs:
            if spec == SHARED_VARIANT:
                nested_columns.append(ByteString(**dict(self.column_options)))
            else:
                nested_columns.append(self.column_by_spec_getter(spec))
        return VariantColumn(
            variant_specs,
            nested_columns,
            **dict(self.column_options),
        )

    def _prepare_dynamic_items(self, items):
        tagged = []
        dynamic_specs = []

        for item in items:
            if item is None:
                tagged.append(None)
                continue

            spec, value = _explicit_item(item)
            if spec is None:
                spec, value = _infer_dynamic_spec(item), item

            if spec == SHARED_VARIANT:
                raise TypeError("SharedVariant cannot be written directly")

            if spec not in dynamic_specs:
                dynamic_specs.append(spec)
            tagged.append((spec, value))

        dynamic_specs.sort()
        return dynamic_specs, tagged


def _explicit_item(item):
    if isinstance(item, dict) and "type" in item and "value" in item:
        return item["type"], item["value"]

    if isinstance(item, (list, tuple)) and len(item) == 2 and isinstance(item[0], str):
        return item[0], item[1]

    return None, None


def _infer_dynamic_spec(item):
    if isinstance(item, bool):
        return "Bool"
    if isinstance(item, int):
        if 0 <= item <= 255:
            return "UInt8"
        if -(2**7) <= item < 2**7:
            return "Int8"
        if 0 <= item <= 2**16 - 1:
            return "UInt16"
        if -(2**15) <= item < 2**15:
            return "Int16"
        if 0 <= item <= 2**32 - 1:
            return "UInt32"
        if -(2**31) <= item < 2**31:
            return "Int32"
        if 0 <= item <= 2**64 - 1:
            return "UInt64"
        return "Int64"
    if isinstance(item, float):
        return "Float64"
    if isinstance(item, (bytes, str)):
        return "String"
    if isinstance(item, (list, tuple)):
        return "Array(Dynamic)"
    if isinstance(item, dict):
        return "JSON"

    raise TypeError(f"Cannot infer Dynamic type for value {item!r}")


def create_dynamic_column(column_by_spec_getter, column_options):
    return DynamicColumn(column_by_spec_getter, **column_options)
