import pytest

from asynch.errors import UnknownTypeError
from asynch.proto.columns import get_column_by_spec


def test_get_unknown_column():
    with pytest.raises(UnknownTypeError) as exc:
        get_column_by_spec("Unicorn", {})

    assert "Unicorn" in str(exc.value)
