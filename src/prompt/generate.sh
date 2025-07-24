#!/bin/bash

set -euo pipefail


cp gendata.xml.tpl gendata.xml.new

echo "Generating prompts ..."
which sd || echo "please install https://github.com/chmln/sd first"

# Replace gendata.xml
readme=$(cat ../../README.md)
introduction=$(cat ../../introduction.md | grep -A10000 '### Default Generation Rules' | grep -B10000 '### AI Generation' | grep -v '### AI Generation')
example=$(cat ../../example/gendata.yaml)
example_tables=$(cat ../../example/ddl/*.table.sql)
example_stats=$(cat ../../example/ddl/*.stats.yaml)
format_tags=$(cat ../generator/README.md | grep -A10000 '## Format tags' | grep -v '^[^\|].*')

sd '^「readme」$' "$readme" gendata.xml.new
sd '^「introduction」$' "$introduction" gendata.xml.new
sd '^「example」$' "$example" gendata.xml.new
sd '^「tables」$' "$example_tables" gendata.xml.new
sd '^「column-stats」$' "$example_stats" gendata.xml.new
sd '^「format-tags」$' "$format_tags" gendata.xml.new
sd '\]\(.*\)' ']' gendata.xml.new # remove Markdown links
mv gendata.xml gendata.xml.old || true
mv gendata.xml.new gendata.xml
