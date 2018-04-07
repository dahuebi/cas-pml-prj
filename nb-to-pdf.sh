#!/bin/bash

. venv/bin/activate

OUT="`pwd`/pdf"
mkdir -p $OUT
cd nb
for nb in ??_*.ipynb; do
  pdf="${nb%.*}"
  echo "Convert $nb --> $pdf"
  pdf="$OUT/$pdf"
  jupyter nbconvert --to pdf $nb --output $pdf
done
