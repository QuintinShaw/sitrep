#!/bin/sh
# Server health → gauges. Register with `sitrep automation add`.
LIMIT=${CPU_LIMIT:-90}

CPU=$(ps -A -o %cpu | awk '{s+=$1} END {printf "%.0f", s}')
DISK=$(df -h / | awk 'NR==2 {gsub("%",""); print $5}')
echo "::sitrep metric.update --icon=cpu --tint=orange --template=gauge --max=100 --alert-above=$LIMIT cpu_pct $CPU \"CPU %\""
echo "::sitrep metric.update --icon=internaldrive --tint=teal --template=gauge --max=100 disk_pct $DISK \"Disk %\""
