# Translated from clickhouse_driver/varint.pyx at 49afa09.
"""Pure-Python mechanical translation of clickhouse-driver's LEB128 helpers."""


def make_varint(number):
    """Writes an integer of variable length using LEB128."""
    # pyx:8-12 uses an unsigned long long argument. Python has unbounded ints,
    # so retain that accepted domain before replacing the C array with bytearray.
    if not 0 <= number <= 0xFFFFFFFFFFFFFFFF:
        raise OverflowError("unsigned long long integer out of range")
    num_buf = bytearray()

    while True:
        to_write = number & 0x7F
        number >>= 7
        if number:
            num_buf.append(to_write | 0x80)
        else:
            num_buf.append(to_write)
            break

    return bytes(num_buf)


def write_varint(number, buf):
    """Writes an integer of variable length using LEB128."""
    # pyx:32-36 has the same C argument domain; see make_varint above.
    if not 0 <= number <= 0xFFFFFFFFFFFFFFFF:
        raise OverflowError("unsigned long long integer out of range")
    num_buf = bytearray()

    while True:
        to_write = number & 0x7F
        number >>= 7
        if number:
            num_buf.append(to_write | 0x80)
        else:
            num_buf.append(to_write)
            break

    buf.write(bytes(num_buf))


def read_varint(f):
    """Reads an integer of variable length using LEB128."""
    shift = 0
    result = 0

    read_one = f.read_one

    while True:
        item = read_one()
        result |= (item & 0x7F) << shift
        shift += 7
        if item < 0x80:
            break

    return result
