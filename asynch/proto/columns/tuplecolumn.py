from .base import Column
from .util import get_inner_columns_with_types, get_inner_spec


class TupleColumn(Column):
    py_types = (list, tuple)

    def __init__(self, names, nested_columns, **kwargs):
        self.names = names
        self.nested_columns = nested_columns
        settings = kwargs["context"].settings
        client_settings = kwargs["context"].client_settings
        self.namedtuple_as_json = settings.get(
            "allow_experimental_object_type", False
        ) and client_settings.get("namedtuple_as_json", True)
        super().__init__(**kwargs)
        self.null_value = tuple(x.null_value for x in nested_columns)

    async def write_data(
        self,
        items,
    ):
        if not items:
            return
        items = self.prepare_items(items)
        items = list(zip(*items))

        for i, x in enumerate(self.nested_columns):
            await x.write_data(
                list(items[i]),
            )

    async def write_items(
        self,
        items,
    ):
        return await self.write_data(
            items,
        )

    async def read_data(
        self,
        n_items,
    ):
        rv = [
            await x.read_data(
                n_items,
            )
            for x in self.nested_columns
        ]
        rv = list(zip(*rv))
        if self.names[0] and self.namedtuple_as_json:
            return [dict(zip(self.names, x)) for x in rv]
        return rv

    async def read_items(
        self,
        n_items,
    ):
        return await self.read_data(
            n_items,
        )

    async def read_state_prefix(self):
        await super().read_state_prefix()
        for column in self.nested_columns:
            await column.read_state_prefix()

    async def write_state_prefix(self):
        await super().write_state_prefix()
        for column in self.nested_columns:
            await column.write_state_prefix()

    def prepare_state_prefix(self, items):
        prepared = [self.null_value if item is None else item for item in items]
        fields = list(zip(*prepared)) if prepared else [[] for _ in self.nested_columns]
        for column, values in zip(self.nested_columns, fields):
            column.prepare_state_prefix(list(values))


def create_tuple_column(spec, column_by_spec_getter, column_options):
    inner_spec = get_inner_spec("Tuple", spec)
    columns_with_types = get_inner_columns_with_types(inner_spec)
    names, types = zip(*columns_with_types)
    return TupleColumn(
        names, [column_by_spec_getter(column_type) for column_type in types], **column_options
    )
