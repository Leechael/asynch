#!/usr/bin/env python3
"""Build the pinned clickhouse-driver pure-Python comparison interpreter."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional

REVISION = "49afa09"
REPOSITORY = "https://github.com/mymarilyn/clickhouse-driver.git"
MODULES = (
    Path("clickhouse_driver/bufferedreader.py"),
    Path("clickhouse_driver/bufferedwriter.py"),
    Path("clickhouse_driver/columns/largeint.py"),
    Path("clickhouse_driver/varint.py"),
)


def command(*args: str, cwd: Optional[Path] = None) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(  # noqa: S603 -- fixed build commands are assembled by this script
        args, check=False, cwd=cwd, text=True, capture_output=True
    )
    if completed.returncode:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise RuntimeError(f"build command failed ({' '.join(args)}): {detail}")
    return completed


def probe(executable: Path, source: Path) -> dict[str, object]:
    code = r'''
import importlib.machinery
import json
import subprocess
import sys
from importlib.metadata import distribution
from pathlib import Path
from urllib.parse import unquote, urlparse

import clickhouse_driver.bufferedreader as bufferedreader
import clickhouse_driver.bufferedwriter as bufferedwriter
import clickhouse_driver.columns.largeint as largeint
import clickhouse_driver.varint as varint

modules = (bufferedreader, bufferedwriter, largeint, varint)
paths = [Path(module.__file__).resolve() for module in modules]
suffixes = importlib.machinery.EXTENSION_SUFFIXES
if any(path.name.endswith(tuple(suffixes)) for path in paths):
    raise SystemExit("not a pure-Python clickhouse-driver: " + ", ".join(map(str, paths)))
direct_url_text = distribution("clickhouse-driver").read_text("direct_url.json")
if not direct_url_text:
    raise SystemExit("pure-Python comparison must be installed from a local source checkout")
direct_url = json.loads(direct_url_text)
source_url = urlparse(direct_url["url"])
if source_url.scheme != "file":
    raise SystemExit("pure-Python comparison must be installed from a local source checkout")
source_path = Path(unquote(source_url.path)).resolve()
expected_source = Path(sys.argv[1]).resolve()
if source_path != expected_source:
    raise SystemExit(f"installed source {source_path} does not match expected {expected_source}")
revision = subprocess.run(
    ["git", "-C", str(source_path), "rev-parse", "HEAD"],
    check=True,
    capture_output=True,
    text=True,
).stdout.strip()
expected_revision = sys.argv[2]
if not revision.startswith(expected_revision):
    raise SystemExit(f"source revision {revision} does not match {expected_revision}")
if any(expected_source not in path.parents for path in paths):
    raise SystemExit("pure-Python modules were not imported from the pinned checkout")
print(json.dumps({"module_paths": [str(path) for path in paths], "source_revision": revision}))
'''
    completed = command(str(executable), "-c", code, str(source), REVISION)
    return json.loads(completed.stdout)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--python", type=Path, default=Path(sys.executable))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = args.output_dir.resolve()
    source = output_dir / "clickhouse-driver"
    environment = output_dir / "venv"
    templates = Path(__file__).parent / "clickhouse_driver"
    patch = Path(__file__).parent / "setup-no-ext-modules.patch"

    if output_dir.exists():
        raise SystemExit(f"output directory already exists: {output_dir}")
    output_dir.mkdir(parents=True)
    try:
        command("git", "clone", REPOSITORY, str(source))
        command("git", "checkout", "--detach", REVISION, cwd=source)
        revision = command("git", "rev-parse", "HEAD", cwd=source).stdout.strip()
        if not revision.startswith(REVISION):
            raise RuntimeError(f"checked out {revision}, expected {REVISION}")
        command("patch", "-p1", "-i", str(patch), cwd=source)
        for relative_path in MODULES:
            destination = source / relative_path
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(templates / relative_path.relative_to("clickhouse_driver"), destination)
        command(str(args.python), "-m", "venv", str(environment))
        executable = environment / "bin" / "python"
        command(str(executable), "-m", "pip", "install", "--upgrade", "pip")
        command(str(executable), "-m", "pip", "install", ".[lz4]", cwd=source)
        print(
            json.dumps(
                {
                    "pure_python_python": str(executable),
                    "pure_python_provenance": probe(executable, source),
                },
                sort_keys=True,
            )
        )
    except BaseException:
        shutil.rmtree(output_dir, ignore_errors=True)
        raise


if __name__ == "__main__":
    main()
