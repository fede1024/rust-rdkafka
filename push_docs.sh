#!/bin/bash

set -e

echo "Compile docs"
cargo doc
echo '<meta http-equiv=refresh content=0;url=rdkafka/index.html>' > target/doc/index.html

echo "Run ghp-import"
ghp-import -n target/doc

echo "Push to github"
git push origin gh-pages
