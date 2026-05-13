from .base import Column
from .util import get_inner_columns, get_inner_spec

NULL_DISCRIMINATOR = 0xFF
VARIANT_BASIC_DISCRIMINATOR_MODE = 0


class VariantColumn(Column):
    py_types = (object,)
    null_value = None

    def __init__(self, variant_specs, nested_columns, **kwargs):
        self.variant_specs = list(variant_specs)
        self.nested_columns = list(nested_columns)
        self.spec_to_index = {spec: i for i, spec in enumerate(self.variant_specs)}
        super().__init__(**kwargs)

    async def read_state_prefix(self):
        await super().read_state_prefix()
        discriminator_mode = await self.reader.read_uint64()
        if discriminator_mode != VARIANT_BASIC_DISCRIMINATOR_MODE:
            raise NotImplementedError(
                f"Variant discriminator mode {discriminator_mode} is not supported"
            )
        for column in self.nested_columns:
            await column.read_state_prefix()

    async def write_state_prefix(self):
        await super().write_state_prefix()
        await self.writer.write_uint64(VARIANT_BASIC_DISCRIMINATOR_MODE)
        for column in self.nested_columns:
            await column.write_state_prefix()

    async def write_data(self, items):
        discriminators, nested_items = self._split_items(items)
        await self.writer.write_bytes(bytes(discriminators))
        for i, column in enumerate(self.nested_columns):
            await column.write_data(nested_items[i])

    async def read_data(self, n_items):
        discriminators = await self.reader.read_bytes(n_items)
        nested_counts = [0] * len(self.nested_columns)
        for discriminator in discriminators:
            if discriminator != NULL_DISCRIMINATOR:
                nested_counts[discriminator] += 1

        nested_values = []
        for count, column in zip(nested_counts, self.nested_columns):
            nested_values.append(list(await column.read_data(count)))

        positions = [0] * len(self.nested_columns)
        result = []
        for discriminator in discriminators:
            if discriminator == NULL_DISCRIMINATOR:
                result.append(None)
                continue

            position = positions[discriminator]
            result.append(nested_values[discriminator][position])
            positions[discriminator] += 1

        return tuple(result)

    async def write_items(self, items):
        return await self.write_data(items)

    async def read_items(self, n_items):
        return await self.read_data(n_items)

    def _split_items(self, items):
        nested_items = [[] for _ in self.nested_columns]
        discriminators = []

        for item in items:
            spec, value = self._resolve_item(item)
            if spec is None:
                discriminators.append(NULL_DISCRIMINATOR)
                continue

            discriminator = self.spec_to_index[spec]
            discriminators.append(discriminator)
            nested_items[discriminator].append(value)

        return discriminators, nested_items

    def _resolve_item(self, item):
        if item is None:
            return None, None

        explicit = self._explicit_item(item)
        if explicit is not None:
            return explicit

        for spec, column in zip(self.variant_specs, self.nested_columns):
            if _fits_spec(spec, column, item):
                return spec, item

        raise TypeError(f"Cannot infer Variant alternative for value {item!r}")

    def _explicit_item(self, item):
        if isinstance(item, dict) and "type" in item and "value" in item:
            spec = item["type"]
            if spec in self.spec_to_index:
                return spec, item["value"]

        if (
            isinstance(item, (list, tuple))
            and len(item) == 2
            and isinstance(item[0], str)
            and item[0] in self.spec_to_index
        ):
            return item[0], item[1]

        return None


def _fits_spec(spec, column, item):
    if spec == "Bool":
        return isinstance(item, bool)

    if spec.startswith(("Int", "UInt")) and isinstance(item, bool):
        return False

    py_types = getattr(column, "py_types", None)
    if py_types is not None and not isinstance(item, py_types):
        return False

    if spec.startswith("UInt") and isinstance(item, int):
        return item >= 0

    if spec == "Point":
        return _is_point(item)

    if spec in ("LineString", "Ring"):
        return _is_sequence_of(item, _is_point)

    if spec == "Polygon":
        return _is_sequence_of(item, lambda x: _is_sequence_of(x, _is_point))

    if spec == "MultiLineString":
        return _is_sequence_of(item, lambda x: _is_sequence_of(x, _is_point))

    if spec == "MultiPolygon":
        return _is_sequence_of(
            item,
            lambda x: _is_sequence_of(x, lambda y: _is_sequence_of(y, _is_point)),
        )

    return True


def _is_point(value):
    return isinstance(value, (list, tuple)) and len(value) == 2


def _is_sequence_of(value, predicate):
    return isinstance(value, (list, tuple)) and all(predicate(item) for item in value)


def create_variant_column(spec, column_by_spec_getter, column_options):
    inner = get_inner_spec("Variant", spec)
    variant_specs = sorted(
        column.strip()
        for column in get_inner_columns(inner)
        if column.strip() and column.strip() != "Nothing"
    )
    nested_columns = [column_by_spec_getter(column_spec) for column_spec in variant_specs]
    return VariantColumn(variant_specs, nested_columns, **column_options)
