#!/usr/bin/env bash
set -euo pipefail

# Ensure script is executed from the repository root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=========================================================="
echo "  Polars.NET Documentation Pipeline (VitePress)"
echo "=========================================================="

echo "=== 1. Building .NET Projects (Debug configuration) ==="
# NOTE: Never use --release per workspace engineering rules
dotnet build Polars.CSharp/Polars.CSharp.csproj -c Debug
dotnet build Polars.FSharp/Polars.FSharp.fsproj -c Debug

echo "=== 2. Extracting XML Docs & Generating API Markdown ==="
dotnet fsi tools/GenerateCSharpDocs.fsx Debug
dotnet fsi tools/GenerateFSharpDocs.fsx Debug

echo "=== 3. Generating Live Execution Output Previews (.ToHtml) ==="
dotnet fsi tools/GenerateDocPreviews.fsx

echo "=== 4. Building VitePress Documentation Site ==="
# Ensure dependencies are installed in docs directory
if [ ! -d "docs/node_modules" ]; then
    echo "Installing documentation npm dependencies..."
    npm --prefix docs install
fi

npm --prefix docs run docs:build

echo "=== Documentation build complete! ==="
echo "Static site output directory: docs/.vitepress/dist"

# ==========================================================
# 5. Deployment to GitHub Pages (gh-pages)
# [Temporarily commented out - will be enabled after package publishing]
# ==========================================================
# echo "=== 5. Deploying to gh-pages ==="
# REPO_URL=$(git config --get remote.origin.url)
# 
# if [ -z "$REPO_URL" ]; then
#     echo "Error: Could not find remote origin URL. Make sure you are in a git repository."
#     exit 1
# fi
# 
# cd docs/.vitepress/dist
# 
# git init
# git checkout -b gh-pages
# git add .
# git commit -m "docs: auto-deploy updated documentation [skip ci]"
# 
# echo "Pushing to $REPO_URL (gh-pages branch)..."
# git push -f "$REPO_URL" gh-pages
# 
# echo "=== Done! Successfully deployed to gh-pages ==="
