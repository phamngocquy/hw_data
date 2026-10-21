import os

from transformer.core import lakefile


def test_every_write_adds_a_version_and_moves_frontier(tmp_path):
    frontier = (
        tmp_path / "year=2026" / "month=10" / "day=20" / "20261020_0100.csv"
    )

    first, v1 = lakefile.write(frontier, "same\n")
    second, v2 = lakefile.write(frontier, "same\n")

    assert (v1, v2) == (1, 2)
    assert first.name == "20261020_0100.v1.csv"
    assert second.name == "20261020_0100.v2.csv"
    assert os.readlink(frontier) == "20261020_0100.v2.csv"
    assert first.read_text() == "same\n"
