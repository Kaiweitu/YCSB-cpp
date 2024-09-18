#!/bin/bash

# Define the top-level workload directories
workloads=("workloada" "workloadb" "workloadc" "workloadd" "workloade" "workloadf")

# Define the mapping for nvme_sata and optane_nvme
suffix_map=("nvme_sata" "optane_nvme")

# Loop through each workload
for workload in "${workloads[@]}"; do
  # Loop through each suffix
  for suffix in "${suffix_map[@]}"; do
    # Find and move the corresponding files
    for file in ${workload}_cachelib_${suffix}; do
      if [[ -f "$file" ]]; then
        mkdir -p "$workload/$suffix"  # Ensure the target directory exists
        mv "$file" "$workload/$suffix/"  # Move the file
        echo "Moved $file to $workload/$suffix/"
      fi
    done
  done
done

echo "File organization completed."