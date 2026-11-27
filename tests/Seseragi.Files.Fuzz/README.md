# Seseragi.Files.Fuzz

A mutation fuzzer for the managed Parquet reader (`Kaminari.Seseragi.Files`), with round trips through its writer, and the unsafe kernels under it
(Sazanami's Snappy and LZ4 decoders, the RLE / bit-packed decoder, bit unpacking, null expansion, the Arrow
builders). Not part of the test suite: run by hand.

## Contract

Any input either reads to the end or is refused with `ParquetFormatException` (codecs: `InvalidDataException`).
Anything else is a finding: another exception, a hang (no progress for 20 s), or the worker dying — a crash is
usually a guard page hit by an over-read or over-write.

## How it finds over-reads

With `GuardedMemory.Enabled` (internal, set by the worker only):

- every block of `NativeMemoryPool` (Arrow buffers, windows, scratch) gets its own mapping that ends at an
  inaccessible page (within the 64-byte alignment);
- the page reader copies each page's stored bytes and its decompressed body into blocks that end exactly at a
  guard page.

A decoder reading or writing past those ends faults at once instead of touching neighbouring data.

## Cases

Case `n` of seed `s` is the same on every run. Three in four take a file of
`tests/Seseragi.Files.Tests/TestData/parquet` or `TestData/parquet-pushdown` (paged files with page indexes and Bloom
filters; minus files that are legitimately too big for the memory cap), apply a few mutations (bit flips, interesting
bytes and 32-bit values, nudges, deletions, duplications, splices from other files, truncation with the footer kept),
aimed in part at page headers and level / run headers, at column and offset indexes and Bloom filters (offsets taken
from the original footer) and at the footer, and read the result with random options (views, parallelism, chunk
reading mode, batch size, level bits, checksums, column subsets, row selections, filters (pruning only, or filtering rows
densely or sparsely), row limits, fetch gaps). The fourth feeds Snappy or LZ4 blocks (valid ones, then
mutated) or random RLE data straight to the kernels, input and output guarded, and compares the decoders with each
other; or round-trips the writer: random values through the RLE / bit-packed encoder and back, or random columns of
every type the Parquet writer writes, flat or nested (structs, lists of every layout, maps, run-end encoded and
dictionary-encoded nested values, nulls at every level), in random slices and with random options (pages, row groups,
codecs, dictionaries, page versions, statistics, checksums), written and read back with every value compared.

## Arrow IPC cases

Passing the IPC corpus (`tests/Seseragi.Files.Tests/TestData/ipc`) as the corpus directory runs the Arrow IPC cases
instead (`IpcCases.cs`); the contract is the same with `IpcFormatException`. Three in four take a corpus file or
stream (pyarrow, Polars, the Arrow integration gold files and fuzz reproducers), mutate it as above (aimed in part at
message starts and the footer) and read it with random options: threads (batches decoded ahead, buffers decompressed
side by side, a stream read ahead), memory mapping (one case in four through a temporary file: mapped bodies are not
guarded), columns, row limits, row filters, UTF-8 checks, batches read at random, consumers that stop early, streams
that cannot seek. The fourth writes random columns (the Parquet writer cases' flat and nested generators) in random
slices with random options (format, compression, threads) and reads them back, every value compared; or writes a
dataset (`DatasetWriteCases.cs`: random columns and partition keys, in batches of random sizes) as Parquet, IPC files,
IPC streams or JSON with random options (keys in the files or not, files cut by rows and by size, threads, a memory
budget that holds next to nothing, codecs, JSON layouts, piece sizes) and reads it back: every row must come back, and
a JSON dataset's objects (its lines, or its documents' elements), decompressed, must be those one `JsonWriter` writes
for the rows. Saved files end in `.arrow`.

## Running

Run it inside a memory- and CPU-capped scope, so that nothing it does reaches the rest of the machine:

```sh
dotnet build tests/Seseragi.Files.Fuzz -c Release
systemd-run --user --scope -p MemoryMax=3G -p MemorySwapMax=0 -p OOMPolicy=continue -p CPUQuota=200% \
  nice -n 19 dotnet tests/Seseragi.Files.Fuzz/bin/Release/net11.0/Seseragi.Files.Fuzz.dll run <seed> <minutes> <out dir>
```

Also with `--setenv=DOTNET_EnableAVX2=0` for the scalar paths. For the IPC cases, pass
`tests/Seseragi.Files.Tests/TestData/ipc` as the fourth argument (`run <seed> <minutes> <out dir> <corpus dir>`). The parent runs cases in batches in worker processes
(core dumps off, managed heap capped at 1.5 GiB), restarts after a crash or hang, pauses while the CPU package is
above 80 °C, and writes `log.txt`, `summary-<seed>.txt` and up to three cases per finding kind (`<seed>-<n>.txt`, and
the mutated `.parquet` for file cases) to the out dir.

Other modes:

| command | does |
|---|---|
| `case <seed> <n>` | runs one case in this process (guarded) and prints its outcome and stack |
| `save <seed> <n> <file>` | writes the mutated file of a file case |
| `selftest` | reads one byte past a guarded block: must die with an access violation |

Fixed findings go to `tests/Seseragi.Files.Tests/TestData/parquet-fuzz/` with a test in
`ParquetFuzzRegressionTests`.

## Encrypted Parquet cases

With `tests/Seseragi.Files.Tests/TestData/parquet/encrypted` as the corpus (`EncryptedCases.cs`): three in four take a
fixture of Parquet modular encryption (pyarrow and Polars, every mode; the keys are known), damage it as above (aimed in
part at its modules: lengths, nonces, tags, ciphertext, crypto metadata, footer module) and read it with its keys (one
in eight with wrong ones) and random options (threads: pages read ahead, columns, rows, filters, checksums). AES-GCM
modules refuse any damage; AES-CTR pages are not authenticated, so their damage reaches the decoders as garbage. The
fourth writes the plain twin's rows with random encryption (algorithm, key size, column keys, plaintext footer, AAD
stored or supplied) and write options, reads them back with the keys (every row compared) and with a wrong footer key
(refused). Contract as above, with `ParquetFormatException`.

## JSON cases

With `tests/Seseragi.Files.Tests/TestData/json` as the corpus directory, each case takes a JSON file of it
(newline-delimited `.ndjson`, or a document `.json`, read in that layout), applies a few mutations (aimed in part at line
and element starts), gzips one case in eight, and reads it twice: a reference
read (one thread, the defaults) and a read with random options (unit size down to one byte, threads, the source kind:
memory, a stream that cannot seek, ranged reads; a row limit, a batch size, a given schema, ignored errors, a row index column). Contract:
each read ends or is refused with `JsonFormatException` / `JsonValueException`, and the two agree: the same rows (the
first ones under a row limit, numbered from 0 with a row index) or the same refusal. Half the inputs read are written back with random options (layout, threads, piece size, in-flight
budget, compression): the bytes, decompressed, must be those of a one-thread write, and must read back as the rows read.
One case in four instead compares the mutated input's tokens, read by Sazanami's `JsonTokenReader` from guarded memory,
with `Utf8JsonReader`'s (`JsonTokenDiff.cs`): lines record by record (the oracle line by line, blank lines passed over, a
line that is not UTF-8 refused), a document as its first value; kinds, positions, lengths, escapes, depths, numbers,
unescaped strings, skips and the failing line must agree.

