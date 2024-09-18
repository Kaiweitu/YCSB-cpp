import subprocess
import argparse
import os

ops_conf = {
  "workloada" : {
    "optane_nvme": 300000000,
    "nvme_sata": 200000000,
  },
  "workloadb" : {
    "optane_nvme": 1200000000,
    "nvme_sata": 600000000
  },
  "workloadc" : {
    "optane_nvme": 1200000000,
    "nvme_sata": 600000000
  },
  "workloadd" : {
    "optane_nvme": 2000000000,
    "nvme_sata": 1000000000
  },
  "workloade" : {
    "optane_nvme": 10000000,
    "nvme_sata": 6000000
  },
  "workloadf" : {
    "optane_nvme": 300000000,
    "nvme_sata": 200000000
  },  
}

read_write_theader_conf = {
  "workloada" : {
    "reader": 128,
    "writer": 128,
  },
  "workloadb" : {
    "reader": 128,
    "writer": 32
  },
  "workloadc" : {
    "reader": 128,
    "writer": 32
  },
  "workloadd" : {
    "reader": 128,
    "writer": 32
  },
  "workloade" : {
    "reader": 128,
    "writer": 32
  },
  "workloadf" : {
    "reader": 128,
    "writer": 128
  }, 
}


def run_command(command, stdout_file, stderr_file):
    print(f"Running command: {command}")
    with open(stdout_file, 'w') as out, open(stderr_file, 'w') as err:
        result = subprocess.run(command, shell=True, stdout=out, stderr=err)
    if result.returncode != 0:
        print(f"Error running command: {command}")
    return result.returncode

def run_workload(workload, config, device, dir, type):
    if type == "thread":
      thread_nums = [8, 16, 32, 64, 128, 256]
    else:
      thread_nums = [256]
    

    if type == "hit_ratio":
      if config == "caching":
        record_nums =  [10000000,  20000000,  30000000] 
      else:
        record_nums = [10000000,  20000000,  30000000, 40000000, 50000000]
    else:
      record_nums = [10000000]

      
    for r_num in record_nums:
      for thread in thread_nums:
        # thread_adjustment = 2 if thread < 32
        base_command = "sudo -E ./build/ycsb -load -run -db cachelib -P"
        workload_path = f"workloads/{workload}_cachelib_{device}"
        config_path = f"cachelib/cachelib_{config}_{device}.properties"
        result_dir = f"{dir}/{device}/{workload}"
        os.makedirs(result_dir, exist_ok=True)

        stdout_file = f"{result_dir}/{workload}_{config}_{device}_{thread}_{r_num}.result"
        stderr_file = f"{result_dir}/{workload}_{config}_{device}_{thread}_{r_num}.error"
        iostat_path = f"{result_dir}/{workload}_{config}_{device}_{thread}_{r_num}.iostat"
        conf_log_path = f"{result_dir}/{workload}_{config}_{device}_{thread}_{r_num}.config"
        reader_thread_num = read_write_theader_conf[workload]['reader']
        writer_thread_num = read_write_theader_conf[workload]['writer']

          
        # if config == "tiering":
        #   if workload != "workloadd" and type != "hit_ratio":
        #     total_ops = max(100000000, int(ops_conf[workload][device] / 2))
        #   else:
        #     total_ops = ops_conf[workload][device] 
        # elif config == "striping" :
        #   if workload != "workloadd" and type != "hit_ratio":
        #     total_ops = max(100000000, int(ops_conf[workload][device] / 3))
        #   else:
        #     total_ops = ops_conf[workload][device]  
        # else:
        total_ops = ops_conf[workload][device] 
        

        if workload == "workloadc" and config == "caching":
          warmed_ops = 80000000
        else:
            if workload == "workloadd":
              warmed_ops = 200000000
            elif workload == "workloade":
              warmed_ops = int(80000000 / 50)
            else: 
              warmed_ops = 80000000
        command = f"{base_command} {workload_path} -warm {warmed_ops} -P {config_path} -p operationcount={total_ops} -p status.interval=60 -p cachelib.reader_thread={reader_thread_num} -p cachelib.writer_thread={writer_thread_num} -p threadcount={thread} -p recordcount={r_num} -s"
        
        with open(conf_log_path, 'w') as conf_log:
          for file_path in [config_path, workload_path]:
            with open(file_path, 'r') as f:
              data = f.read()
              conf_log.write(data)
          conf_log.write(f"Command: {command}")
        with open(iostat_path, 'w') as f:
          iostat = subprocess.Popen(['iostat', '-x', '1', '/dev/nvme0n1', '/dev/nvme2n1', '/dev/sdc'], stdout=f)
          rnt = run_command(command, stdout_file, stderr_file)
          iostat.terminate()
      
      

def trim_device():
    trim_command = "./trim_device.sh"
    result = subprocess.run(trim_command, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if result.returncode != 0:
        print("Error running trim_device.sh")
    return result.returncode

def main(workload, configs, device, dir, type):
    for conf in configs:
        run_workload(workload, conf, device, dir, type)
        trim_device()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run YCSB with specified workload, configuration(s), and device type.")
    parser.add_argument("workload", choices=['workloada', 'workloadb', 'workloadc', 'workloadd', 'workloade', 'workloadf'], help="The workload to run (e.g., workloada, workloadb, workloadc).")
    parser.add_argument("config", help="The configuration(s) to use, e.g., caching, striping, most, tiering, or a combination separated by ',', e.g., caching,striping.")
    parser.add_argument("device", choices=['nvme_sata', 'optane_nvme'], help="The device configuration to use (e.g., nvme_sata, optane_nvme).")
    parser.add_argument("environment", help="The directory to the store the result.")
    parser.add_argument("type", choices=['thread', 'normal', 'hit_ratio'], help="The type of experiment we run")

    args = parser.parse_args()

    # Split the configuration argument by ',' to support multiple configurations
    if args.config == 'all':
        configs = ['most', 'tiering', 'caching', 'striping']
    else:
        configs = args.config.split(',')

    # Validate configurations
    valid_configs = ['most', 'tiering', 'caching', 'striping']
    for conf in configs:
        if conf not in valid_configs:
            print(f"Invalid configuration: {conf}. Choose from 'caching', 'striping', 'most', 'tiering'.")
            exit(1)
            
    if args.environment == 'formal':
      dir = 'result'
    else:
      dir = args.environment

    main(args.workload, configs, args.device, dir, args.type)
