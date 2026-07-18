#!/bin/sh
# Demo: a fake long task that exercises all three primitives.
#   sitrep run --title "demo task" -- skills/examples/demo-task.sh

echo "::sitrep task.start 'demo: pretend training run'"
i=0
while [ "$i" -le 10 ]; do
  pct=$((i * 10))
  echo "::sitrep task.progress $pct epoch $i/10"
  echo "::sitrep metric.update demo_loss 0.$((100 - pct))"
  sleep 1
  i=$((i + 1))
done
echo "::sitrep message.send 'demo task finished'"
echo "::sitrep task.done all epochs complete"
