# Seseragi.Files.Fuzz

A mutation fuzzer for the managed Parquet reader (`Kaminari.Seseragi.Files`) and the unsafe kernels under it
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
reading mode, batch size, level bits, checksums, column subsets, row selections, filters, row limits, fetch gaps). The fourth feeds Snappy or LZ4 blocks (valid ones, then
mutated) or random RLE data straight to the kernels, input and output guarded, and compares the decoders with each
other.

## Running

Run it inside a memory- and CPU-capped scope, so that nothing it does reaches the rest of the machine:

```sh
dotnet build tests/Seseragi.Files.Fuzz -c Release
systemd-run --user --scope -p MemoryMax=3G -p MemorySwapMax=0 -p OOMPolicy=continue -p CPUQuota=200% \
  nice -n 19 dotnet tests/Seseragi.Files.Fuzz/bin/Release/net10.0/Seseragi.Files.Fuzz.dll run <seed> <minutes> <out dir>
```

Also with `--setenv=DOTNET_EnableAVX2=0` for the scalar paths. The parent runs cases in batches in worker processes
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
