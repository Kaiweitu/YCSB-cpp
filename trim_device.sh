#!/bin/bash
# nvme0
fio --name=trim --filename=/dev/disk/by-id/nvme-Samsung_SSD_960_EVO_1TB_S3ETNX0J308011P-part1  --rw=trim --bs=3G
fio --name=trim --filename=/dev/disk/by-id/nvme-Samsung_SSD_960_EVO_1TB_S3ETNX0J308011P-part2  --rw=trim --bs=3G
# optane0
fio --name=trim --filename=/dev/disk/by-id/nvme-INTEL_SSDPED1K750GA_PHKS92550031750BGN-part1  --rw=trim --bs=3G
fio --name=trim --filename=/dev/disk/by-id/nvme-INTEL_SSDPED1K750GA_PHKS92550031750BGN-part2  --rw=trim --bs=3G
# SATA0
fio --name=trim --filename=/dev/disk/by-id/ata-Samsung_SSD_870_EVO_1TB_S75BNS0W637147X-part1 --rw=trim --bs=3G
fio --name=trim --filename=/dev/disk/by-id/ata-Samsung_SSD_870_EVO_1TB_S75BNS0W637147X-part2 --rw=trim --bs=3G