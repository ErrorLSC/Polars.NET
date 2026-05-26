#!/bin/bash
set -e

echo "=== 1. Building Projects (Release) ==="
dotnet build Polars.FSharp/Polars.FSharp.fsproj -c Release

echo "=== 2. Generating F# Markdown (Custom Script) ==="
dotnet fsi GenerateFSharpDocs.fsx

echo "=== 3. Organizing Docs ==="

rm -rf docs/api/csharp
rm -rf docs/api/fsharp
rm -rf docs/_site

mkdir -p docs/api/fsharp

cp -r docs/api/fsharp_generated/* docs/api/fsharp/

echo "=== 4. Building Site (DocFX) ==="
export PATH="$PATH:/home/qinglei/.dotnet/tools"
docfx metadata docs/docfx.json
docfx build docs/docfx.json
rm -rf docs/api/fsharp_generated

echo "=== 5. Deploying to gh-pages ==="
REPO_URL=$(git config --get remote.origin.url)

if [ -z "$REPO_URL" ]; then
    echo "Error: Could not find remote origin URL. Make sure you are in a git repository."
    exit 1
fi

cd docs/_site

git init

git checkout -b gh-pages

git add .
git commit -m "docs: auto-deploy updated documentation"

echo "Pushing to $REPO_URL (gh-pages branch)..."
git push -f "$REPO_URL" gh-pages

echo "=== Done! Successfully deployed to gh-pages ==="