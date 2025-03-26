#!/bin/bash

# Check if device argument is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <block-device>"
    echo "Example: $0 /dev/sdb"
    exit 1
fi

DEVICE=$1

# Ensure the device exists
if [ ! -b "$DEVICE" ]; then
    echo "Error: $DEVICE is not a valid block device."
    exit 1
fi

# Confirm before proceeding
echo "This will erase all data on $DEVICE and create two equal partitions."
read -p "Are you sure you want to continue? [y/N] " answer
if [ "$answer" != "y" ] && [ "$answer" != "Y" ]; then
    echo "Operation cancelled."
    exit 1
fi

# Create partition table and two equal partitions
sudo parted --script "$DEVICE" \
    mklabel gpt \
    mkpart primary 0% 50% \
    mkpart primary 50% 100%

# Inform kernel of changes
sudo partprobe "$DEVICE"

echo "Partitioning completed successfully."
lsblk "$DEVICE"