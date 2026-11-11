#!/usr/bin/env bash
# Regression runs in tiers, one test command after another, after a single build.
#
#   scripts/regress.sh quick [filter]   unit tests of the Delta kernel and the Delta tests of the C# and F# APIs (minutes);
#                                       [filter] adds an integration test run, e.g. "FullyQualifiedName~SparkMapColumnTests"
#   scripts/regress.sh delta [filter]   quick, plus the Delta integration tests: Spark interop, kernel on object stores,
#                                       DeltaLakeTests, Unity Catalog, file system contract (starts containers)
#   scripts/regress.sh full             every test project of the library, plus the delta tier's integration tests
#                                       (not: real-cloud, ML, MSSQL, ADBC, LINQ integration, the net11 LINQ suite)
#
# Each step's output goes to $REGRESS_LOG_DIR (default: $TMPDIR/polars-regress); a summary table is printed at the
# end. The exit code is non-zero when a build or a test run failed.
set -u
tier=${1:-quick}
extra=${2:-}
case "$tier" in quick|delta|full) ;; *) echo "usage: $0 quick|delta|full [integration filter]" >&2; exit 2 ;; esac

root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root" || exit 2
logs=${REGRESS_LOG_DIR:-${TMPDIR:-/tmp}/polars-regress}
mkdir -p "$logs"
rm -f "$logs"/*.log "$logs"/failures.txt

summary=()
failed=0

# step NAME COMMAND...: runs the command, logs it, records "name | result | seconds".
step() {
    local name=$1; shift
    local log="$logs/$(echo "$name" | tr ' /=~|' '_____').log"
    local start=$SECONDS
    echo "== $name"
    "$@" >"$log" 2>&1
    local rc=$?
    local result
    result=$(grep -E "^(Passed|Failed)!|Build succeeded|error" "$log" | grep -E "Passed!|Failed!|Build succeeded|[0-9]+ Error" | tail -1 | sed -E 's/ - [^ ]+\.dll.*//; s/^ +//')
    [ -z "$result" ] && result="exit $rc (see $log)"
    if [ $rc -ne 0 ]; then
        failed=1
        result="FAILED: $result"
        grep -E "^\s+Failed [A-Za-z]" "$log" | sed -E 's/^\s+/    /' | head -20 >>"$logs/failures.txt"
    fi
    summary+=("$(printf '%-60s %5ss  %s' "$name" $((SECONDS - start)) "$result")")
}

test_run() { # test_run NAME PROJECT [FILTER]
    local name=$1 project=$2 filter=${3:-}
    if [ -n "$filter" ]; then
        step "$name" dotnet test "$project" --no-build --filter "$filter"
    else
        step "$name" dotnet test "$project" --no-build
    fi
}

integration=tests/Polars.Integration.Tests

# One build of everything the tier runs; every test run after it uses --no-build.
projects=(tests/Seseragi.Lakehouse.Tests tests/Polars.FSharp.Tests tests/Polars.CSharp.Tests)
[ "$tier" != quick ] || [ -n "$extra" ] && projects+=("$integration")
[ "$tier" != quick ] && projects+=(tests/Seseragi.Catalog.Tests)
[ "$tier" = full ] && projects+=(tests/Sazanami.Tests tests/Polars.Config.Tests)
for p in "${projects[@]}"; do step "build $(basename "$p")" dotnet build "$p"; done
if [ $failed -ne 0 ]; then
    printf '%s\n' "${summary[@]}"
    echo "Build failed; no tests run. Logs: $logs"
    exit 1
fi

test_run "Lakehouse" tests/Seseragi.Lakehouse.Tests
if [ "$tier" = full ]; then
    test_run "F# (all)" tests/Polars.FSharp.Tests
    test_run "C# (all)" tests/Polars.CSharp.Tests
    test_run "Sazanami" tests/Sazanami.Tests
    test_run "Config (serial)" tests/Polars.Config.Tests
else
    test_run "F# Delta" tests/Polars.FSharp.Tests "FullyQualifiedName~Seseragi|FullyQualifiedName~DeltaTests"
    test_run "C# Delta" tests/Polars.CSharp.Tests "FullyQualifiedName~Delta"
fi
[ -n "$extra" ] && test_run "integration: $extra" "$integration" "$extra"

if [ "$tier" != quick ]; then
    test_run "Seseragi.Catalog" tests/Seseragi.Catalog.Tests
    test_run "Spark interop" "$integration" "DeltaKernel=Spark"
    test_run "kernel on object stores" "$integration" "DeltaKernel=Cloud"
    test_run "DeltaLakeTests" "$integration" "FullyQualifiedName~DeltaLakeTests"
    test_run "Unity Catalog" "$integration" "FullyQualifiedName~CatalogIntegrationTests"
    test_run "file system contract" "$integration" "FileSystemContract=Local|FileSystemContract=Cloud"
fi

echo
echo "Regression ($tier) — logs in $logs"
printf '%s\n' "${summary[@]}"
if [ -s "$logs/failures.txt" ]; then
    echo "Failed tests:"
    cat "$logs/failures.txt"
fi
exit $failed
