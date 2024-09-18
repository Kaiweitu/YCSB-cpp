#!/bin/bash

# Define the top-level directories
workloads=("workloada" "workloadb" "workloadc" "workloadd" "workloade" "workloadf")

# Define the second-level subdirectories
subdirs=("optane_nvme" "nvme_sata")

# Define the third-level subdirectories
third_level_subdirs=("striping" "caching" "most" "tiering")

# Create the directory structure
for workload in "${workloads[@]}"; do
  for subdir in "${subdirs[@]}"; do
    for third_subdir in "${third_level_subdirs[@]}"; do
      mkdir -p "$workload/$subdir/$third_subdir"
    done
  done
done

echo "Directory structure created successfully."