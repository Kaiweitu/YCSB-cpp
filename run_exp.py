import subprocess
import argparse

def run_command(command, stdout_file, stderr_file):
    print(f"Running command: {command}")
    with open(stdout_file, 'w') as out, open(stderr_file, 'w') as err:
        result = subprocess.run(command, shell=True, stdout=out, stderr=err)
    if result.returncode != 0:
        print(f"Error running command: {command}")
    return result.returncode

def run_workload(workload, config):
    base_command = "sudo -E ./build/ycsb -load -run -db cachelib -P"
    workload_path = f"workloads/{workload}_cachelib"
    config_path = f"cachelib/cachelib_{config}.properties"
    stdout_file = f"result/{workload}_{config}_nvme_sata.result"
    stderr_file = f"result/{workload}_{config}_nvme_sata.error"

    command = f"{base_command} {workload_path} -P {config_path} -s"
    return run_command(command, stdout_file, stderr_file)

def trim_device():
    trim_command = "./trim_device.sh"
    run_command(trim_command, '/dev/null', '/dev/null')  # No need to capture output

def main(workload, configs):
    for conf in configs:
        run_workload(workload, conf)
        trim_device()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run YCSB with specified workload and configuration(s).")
    parser.add_argument("workload", choices=['workloada', 'workloadb', 'workloadc', 'workloadd', 'workloade', 'workloadf'], help="The workload to run (e.g., workloada, workloadb, workloadc).")
    parser.add_argument("config", help="The configuration(s) to use, e.g., caching, striping, most, tiering, or a combination separated by ',', e.g., caching,striping.")

    args = parser.parse_args()

    # Split the configuration argument by ',' to support multiple configurations
    if args.config == 'all':
        configs = [ 'most', 'tiering', 'caching', 'striping']
    else:
        configs = args.config.split(',')

    # Validate configurations
    valid_configs = ['most', 'tiering', 'caching', 'striping']
    for conf in configs:
        if conf not in valid_configs:
            print(f"Invalid configuration: {conf}. Choose from 'caching', 'striping', 'most', 'tiering'.")
            exit(1)

    main(args.workload, configs)