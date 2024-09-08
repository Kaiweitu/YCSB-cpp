#!/bin/bash
sudo perf record -F 99 -p  $(pgrep -x cachebench) -g -m 2048 --call-graph lbr -- sleep 30
sudo perf script > out.perf
./FlameGraph/stackcollapse-perf.pl out.perf > out.folded
./FlameGraph/flamegraph.pl out.folded > flamegraph.svg