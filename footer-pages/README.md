# Modular Footer V2

Modular Footer V2 stores metadata arrays as typed Parquet page streams, with a small Thrift directory describing their locations. Field IDs define each stream's type and nullability; the source-data schema is stored once in `FileInfo`.

| Start here | Contents |
|---|---|
| [Playground guide](PLAYGROUND.md) | V2 design → Raincloud setup → tested datasets → four size levels → codec implementations → decoding tests → layout/encoding experiments → result recording. |
| [Experiment results](EXPERIMENT-RESULTS.md) | Consolidated size/decoding results, design experiments, source-record lookup, and pending measurements. |
| [Thrift definition](ModularFooterV2.thrift) | Version-21 layout, page streams, statistics, and encryption contract. |
| [Layout visualization](data-location-visualization.html) | Interactive HTML; download and open in a browser. |

The guide includes runnable source-generation and baseline benchmark commands. The optimized V2 benchmark harness and Runtime codecs are separate dependencies and are not packaged in this directory.

Decoding requirements include explicit one-column and all-column cases, plus separate measurements of column-position effects.

The [synthetic placement harness](../pfb-harness/README.md) supports adding layouts today. Full V2 layout/encoding experiments still require the separate artifact generator, native adapters/readers, and benchmark runner described in the guide.
