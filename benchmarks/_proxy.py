"""A deliberately small TCP proxy for injecting loopback latency.

The proxy is transport-only: it does not inspect ClickHouse packets. It delays
the first byte of each read burst in each direction, then copies that burst
unchanged. This makes request/response-sized ClickHouse traffic observe the
configured latency without protocol parsing.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from contextlib import suppress


def parse_target(value: str) -> tuple[str, int]:
    host, separator, port = value.rpartition(":")
    if not separator or not host or not port.isdecimal():
        raise argparse.ArgumentTypeError("target must use HOST:PORT")
    return host, int(port)


async def close_writer(writer: asyncio.StreamWriter) -> None:
    writer.close()
    with suppress(ConnectionError):
        await writer.wait_closed()


async def relay(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter, delay_seconds: float
) -> None:
    while data := await reader.read(64 * 1024):
        if delay_seconds:
            await asyncio.sleep(delay_seconds)
        writer.write(data)
        await writer.drain()
    await close_writer(writer)


async def handle_client(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
    *,
    target: tuple[str, int],
    delay_seconds: float,
) -> None:
    try:
        upstream_reader, upstream_writer = await asyncio.open_connection(*target)
    except OSError as error:
        print(  # noqa: T201
            f"proxy error: unable to connect to {target[0]}:{target[1]}: {error}", file=sys.stderr
        )
        await close_writer(client_writer)
        return

    tasks = [
        asyncio.create_task(relay(client_reader, upstream_writer, delay_seconds)),
        asyncio.create_task(relay(upstream_reader, client_writer, delay_seconds)),
    ]
    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        await close_writer(upstream_writer)
        await close_writer(client_writer)


async def run(args: argparse.Namespace) -> None:
    target = parse_target(args.target)
    try:
        probe_reader, probe_writer = await asyncio.open_connection(*target)
    except OSError as error:
        raise RuntimeError(f"target {args.target} is unreachable: {error}") from error
    del probe_reader
    await close_writer(probe_writer)

    server = await asyncio.start_server(
        lambda reader, writer: handle_client(
            reader, writer, target=target, delay_seconds=args.delay_ms / 1000
        ),
        host="127.0.0.1",
        port=args.listen,
    )
    sockets = ", ".join(str(socket.getsockname()) for socket in server.sockets or [])
    print(f"proxy listening on {sockets}; target={args.target}; delay_ms={args.delay_ms:g}")  # noqa: T201
    async with server:
        await server.serve_forever()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--listen", type=int, required=True, help="local TCP port to listen on")
    parser.add_argument("--target", required=True, help="upstream ClickHouse HOST:PORT")
    parser.add_argument(
        "--delay-ms",
        type=float,
        required=True,
        help="one-way delay before each relayed burst (must be non-negative)",
    )
    args = parser.parse_args()
    if not 0 <= args.listen <= 65535:
        parser.error("--listen must be a valid TCP port")
    if args.delay_ms < 0:
        parser.error("--delay-ms must be non-negative")
    parse_target(args.target)
    return args


def main() -> None:
    try:
        asyncio.run(run(parse_args()))
    except KeyboardInterrupt:
        pass
    except Exception as error:
        print(f"proxy error: {error}", file=sys.stderr)  # noqa: T201
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()
