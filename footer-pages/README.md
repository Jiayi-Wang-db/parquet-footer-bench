# Modular Footer V2

Modular Footer V2 stores metadata arrays as typed Parquet page streams, with a small Thrift directory describing their locations. Field IDs define each stream's type and nullability; the source-data schema is stored once in `FileInfo`.

| Start here | Contents |
|---|---|
| [Playground guide](PLAYGROUND.md) | Design, four comparison levels, tested datasets, Raincloud Java setup, verification, and size/decoding protocol. |
| [Thrift definition](ModularFooterV2.thrift) | Version-21 layout, page streams, statistics, and encryption contract. |
| [Layout visualization](data-location-visualization.html) | Interactive HTML; download and open in a browser. |

The guide includes runnable source-generation and baseline benchmark commands. The optimized V2 benchmark harness and Runtime codecs are separate dependencies and are not packaged in this directory.
