# Pool lock refactor benchmark report

Source: GitHub Actions [run 29196450867](https://github.com/Leechael/asynch/actions/runs/29196450867), Ubuntu runner, ClickHouse latest, Python 3.13. The run used the unmodified B and D benchmark scripts with seed `20260712`, one warmup plus five measured rounds, and ran B before D.

The adjacent JSON files are the authoritative distributions. This report only
summarizes their medians; it does not discard rounds or substitute means.

| Injected RTT | B p50 QPS (workers 8 / 32) | D p50 QPS at workers 128 (maxsize 8 / 32) | D acquire_wait p50 ms (8 / 32) |
| --- | ---: | ---: | ---: |
| 0 ms | 1184.9 / 1329.7 | 1003.7 / 1166.9 | 116.6 / 84.9 |
| 1 ms | 818.5 / 1224.8 | 824.5 / 1119.5 | 143.6 / 86.6 |
| 5 ms | 371.9 / 906.2 | 394.0 / 995.6 | 298.9 / 96.0 |
| 20 ms | 116.6 / 458.1 | 122.6 / 490.3 | 965.2 / 190.0 |

The lock-removal acceptance ratios are met in this run: at 5 ms D is 1.06x
(maxsize 8) and 1.10x (maxsize 32) of the corresponding B reference; at 20 ms
it is 1.05x and 1.07x. The 0 ms D medians also exceed the fixed 870 ops/s
floor.

This hosted runner's 0 ms B medians differ materially from the historic B
reference, so this run is retained as raw evidence but is not yet the final
environment-comparability acceptance run. A further identical B-to-D run is
required by the plan's ±10% rule before treating the comparison as final.
