# Translated from clickhouse_driver/columns/largeint.pyx at 49afa09.
"""Pure-Python mechanical translation of clickhouse-driver large integer helpers."""

from .. import writer

MAX_UINT64 = writer.MAX_UINT64
MAX_INT64 = writer.MAX_INT64


def int128_from_quads(self, quad_items, n_items):
    factor = 2
    # pyx:12-33 allocates a tuple with C reference operations. Appending then
    # freezing a list is Python's ownership-equivalent tuple construction.
    items = []

    for index in range(n_items):
        item_index = factor * index

        if quad_items[item_index + 1] > MAX_INT64:
            item = (
                -((MAX_UINT64 - quad_items[item_index + 1]) << 64)
                - (MAX_UINT64 - quad_items[item_index])
                - 1
            )
        else:
            item = (quad_items[item_index + 1] << 64) + quad_items[item_index]

        items.append(item)

    return tuple(items)


def int128_to_quads(self, items, n_items):
    # pyx:38-67 uses PyTuple_SET_ITEM; this list/tuple pair is the direct
    # Python ownership equivalent, retaining the same per-item arithmetic.
    quad_items = []

    for index in range(n_items):
        value = items[index]
        if value < 0:
            value = -value - 1
            quad_items.append(MAX_UINT64 - value & MAX_UINT64)
            quad_items.append(MAX_UINT64 - (value >> 64) & MAX_UINT64)
        else:
            quad_items.append(value & MAX_UINT64)
            quad_items.append((value >> 64) & MAX_UINT64)

    return tuple(quad_items)


def uint128_from_quads(self, quad_items, n_items):
    factor = 2
    items = []

    for index in range(n_items):
        item_index = factor * index
        items.append((quad_items[item_index + 1] << 64) + quad_items[item_index])

    return tuple(items)


def uint128_to_quads(self, items, n_items):
    quad_items = []

    for index in range(n_items):
        value = items[index]
        quad_items.append(value & MAX_UINT64)
        quad_items.append((value >> 64) & MAX_UINT64)

    return tuple(quad_items)


# 256 bits


def int256_from_quads(self, quad_items, n_items):
    factor = 4
    items = []

    for index in range(n_items):
        item_index = factor * index

        if quad_items[item_index + 3] > MAX_INT64:
            item = (
                -((MAX_UINT64 - quad_items[item_index + 3]) << 192)
                - ((MAX_UINT64 - quad_items[item_index + 2]) << 128)
                - ((MAX_UINT64 - quad_items[item_index + 1]) << 64)
                - (MAX_UINT64 - quad_items[item_index])
                - 1
            )
        else:
            item = (
                (quad_items[item_index + 3] << 192)
                + (quad_items[item_index + 2] << 128)
                + (quad_items[item_index + 1] << 64)
                + quad_items[item_index]
            )

        items.append(item)

    return tuple(items)


def int256_to_quads(self, items, n_items):
    quad_items = []

    for index in range(n_items):
        value = items[index]
        if value < 0:
            value = -value - 1
            quad_items.append(MAX_UINT64 - value & MAX_UINT64)
            quad_items.append(MAX_UINT64 - (value >> 64) & MAX_UINT64)
            quad_items.append(MAX_UINT64 - (value >> 128) & MAX_UINT64)
            quad_items.append(MAX_UINT64 - (value >> 192) & MAX_UINT64)
        else:
            quad_items.append(value & MAX_UINT64)
            quad_items.append((value >> 64) & MAX_UINT64)
            quad_items.append((value >> 128) & MAX_UINT64)
            quad_items.append((value >> 192) & MAX_UINT64)

    return tuple(quad_items)


def uint256_from_quads(self, quad_items, n_items):
    factor = 4
    items = []

    for index in range(n_items):
        item_index = factor * index
        items.append(
            (quad_items[item_index + 3] << 192)
            + (quad_items[item_index + 2] << 128)
            + (quad_items[item_index + 1] << 64)
            + quad_items[item_index]
        )

    return tuple(items)


def uint256_to_quads(self, items, n_items):
    quad_items = []

    for index in range(n_items):
        value = items[index]
        quad_items.append(value & MAX_UINT64)
        quad_items.append((value >> 64) & MAX_UINT64)
        quad_items.append((value >> 128) & MAX_UINT64)
        quad_items.append((value >> 192) & MAX_UINT64)

    return tuple(quad_items)
