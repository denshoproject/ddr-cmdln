sfood --internal --ignore-unused . | grep -v tests | sfood-graph | dot -Tsvg > ddr-cmdln_graph.svg
