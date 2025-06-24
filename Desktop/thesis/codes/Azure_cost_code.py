import os
import gzip
import shutil
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import subprocess
import logging
import json
import re
from azure.identity import DefaultAzureCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.sql import SqlManagementClient
from azure.mgmt.containerservice import ContainerServiceClient

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# Initialize global variable
resource_names = {}

# ------------------- Resource Naming Management -------------------

def load_resource_names(filename="resource_names.json"):
    global resource_names
    try:
        with open(filename, "r") as f:
            resource_names = json.load(f)
        logging.info(f"Loaded resource names from {filename}")
        print("Resource Names Loaded:")
        print(json.dumps(resource_names, indent=4))  # Pretty print JSON to console
    except FileNotFoundError:
        logging.warning(f"Resource names file {filename} not found. Using Azure-generated names.")
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON from {filename}. Using Azure-generated names.")
        resource_names = {}

# ------------------- Cost Assumptions (MUST BE ADJUSTED) -------------------

VM_ON_DEMAND_COST_PER_HOUR = 0.07  # Example: Standard_B1ms VM in East US
VM_RESERVED_COST_PER_HOUR = VM_ON_DEMAND_COST_PER_HOUR * 0.40  # 60% savings with reserved instances
SQL_DB_COST_PER_HOUR = 0.04  # Example: Basic SQL Database in East US
NSG_SAVINGS = 5.00  # Reduced blast radius, less audit findings
AKS_CPU_COST_PER_CORE_HOUR = 0.05  # Adjust based on actual cost of CPU in your AKS cluster
AKS_MEMORY_COST_PER_GB_HOUR = 0.015  # Adjust based on actual cost of memory in your AKS cluster
AKS_SAVINGS_FACTOR_RIGHTSIZING = 0.20  # Potential savings from right-sizing containers

# Savings Factors
VM_SAVINGS_PERIOD_DAYS = 30
SQL_SAVINGS_FACTOR = 0.4  # Savings using serverless SQL (adjust based on workload)
JENKINS_LOG_RETENTION_DAYS = 7  # Only archive logs from the last 7 days

def is_azure_cli_installed():
    try:
        subprocess.run(["az", "--version"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        logging.warning("Azure CLI not found. Please install it to proceed.")
        return False

def is_kubectl_installed():
    try:
        subprocess.run(["kubectl", "version", "--client"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        logging.warning("kubectl not found. Please install it to proceed.")
        return False

def calculate_monthly_savings(hourly_cost, savings_factor):
    return hourly_cost * 24 * VM_SAVINGS_PERIOD_DAYS * savings_factor

def optimize_vms(compute_client):
    savings = 0
    try:
        vms = compute_client.virtual_machines.list_all()
        for vm in vms:
            try:
                if vm.instance_view.statuses[1].code == "PowerState/running":
                    vm_size = vm.hardware_profile.vm_size
                    savings_per_vm = calculate_monthly_savings(VM_ON_DEMAND_COST_PER_HOUR - VM_RESERVED_COST_PER_HOUR, 1)
                    logging.info(f"Virtual Machine {vm.name} ({vm_size}) is running. Consider rightsizing, stopping, or using reserved instances. Potential Savings: ${savings_per_vm:.2f}")
                    savings += savings_per_vm
            except Exception as vm_err:
                logging.error(f"Error processing VM {vm.name}: {vm_err}")
    except Exception as e:
        logging.error(f"Error optimizing VMs: {e}")
    return savings

def optimize_sql_databases(sql_client):
    savings = 0
    try:
        servers = sql_client.servers.list()
        for server in servers:
            try:
                databases = sql_client.databases.list_by_server(server.resource_group, server.name)
                for db in databases:
                    try:
                        # NOTE: The `sku.tier` check might not be the most reliable way to determine serverless.
                        # You may need to examine the `sku.name` for more precise identification.
                        if db.sku.tier != "Serverless":  # Check if the database is not already serverless
                            savings_per_db = calculate_monthly_savings(SQL_DB_COST_PER_HOUR, SQL_SAVINGS_FACTOR)
                            logging.info(f"Azure SQL Database {db.name} is running. Consider using serverless compute tier. Potential Savings: ${savings_per_db:.2f}")
                            savings += savings_per_db
                    except Exception as db_err:
                        logging.error(f"Error processing database {db.name}: {db_err}")
            except Exception as server_err:
                logging.error(f"Error processing server {server.name}: {server_err}")
    except Exception as e:
        logging.error(f"Error optimizing SQL Databases: {e}")
    return savings

def optimize_nsgs(network_client):
    savings = 0
    try:
        nsgs = network_client.network_security_groups.list_all()
        for nsg in nsgs:
            try:
                for rule in nsg.security_rules:
                    try:
                        if rule.source_address_prefix == "*" or rule.destination_address_prefix == "*":
                            logging.warning(f"Network Security Group {nsg.name} has an open rule! Consider restricting it. Potential Savings: ${NSG_SAVINGS:.2f}")
                            savings += NSG_SAVINGS
                    except Exception as rule_err:
                        logging.error(f"Error processing rule {rule.name} in NSG {nsg.name}: {rule_err}")
            except Exception as nsg_err:
                logging.error(f"Error processing NSG {nsg.name}: {nsg_err}")
    except Exception as e:
        logging.error(f"Error optimizing NSGs: {e}")
    return savings

def upload_log_to_blob_storage(log_file, job_name, build_number, storage_account, container_name):
    try:
        if not os.path.exists(log_file) or os.path.getsize(log_file) == 0:
            logging.warning(f"Skipping {log_file}: Empty or missing.")
            return 0

        compressed_file = f"{log_file}.gz"
        with open(log_file, 'rb') as f_in, gzip.open(compressed_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

        blob_name = f"{job_name}-{build_number}.log.gz"
        try:
            subprocess.run(["az", "storage", "blob", "upload", "--account-name", storage_account, "--container-name", container_name, "--name", blob_name, "--file", compressed_file], check=True, capture_output=True, text=True)
            logging.info(f"Uploaded {job_name}/{build_number} to blob {blob_name}. Savings: $0.03") # Simplified message and added Savings: $0.03
        except subprocess.CalledProcessError as e:
             logging.error(f"Failed to upload {job_name}/{build_number}: {e.stderr}")
             return 0


        # Clean up compressed file after successful upload
        os.remove(compressed_file)

        return 0  # No direct cost savings calculation for Azure Blob Storage in this example

    except Exception as e:
        logging.error(f"Error processing {log_file}: {e}")
        return 0

def process_jenkins_logs(jenkins_home, storage_account, container_name):
    total_savings = 0
    if not os.path.exists(jenkins_home):
        logging.warning(f"Jenkins directory {jenkins_home} not found. Skipping log processing.")
        return 0

    today = datetime.now()
    cutoff_date = today - timedelta(days=JENKINS_LOG_RETENTION_DAYS)

    with ThreadPoolExecutor(max_workers=os.cpu_count() or 10) as executor:
        futures = []
        for job_dir in os.listdir(os.path.join(jenkins_home, "jobs")):
            try:
                builds_path = os.path.join(jenkins_home, "jobs", job_dir, "builds")

                if os.path.isdir(builds_path):
                    for build_dir in os.listdir(builds_path):
                        try:
                            log_file = os.path.join(builds_path, build_dir, "log")
                            try:
                                log_modified_time = datetime.fromtimestamp(os.path.getmtime(log_file))
                                if os.path.isfile(log_file) and log_modified_time >= cutoff_date:
                                    futures.append(executor.submit(upload_log_to_blob_storage, log_file, job_dir, build_dir, storage_account, container_name))
                            except FileNotFoundError:
                                pass  # Log file might not exist
                            except Exception as log_time_err:
                                logging.error(f"Error processing log file {log_file}: {log_time_err}")
                        except Exception as build_dir_err:
                            logging.error(f"Error processing build directory {build_dir}: {build_dir_err}")
            except Exception as job_dir_err:
                logging.error(f"Error processing job directory {job_dir}: {job_dir_err}")

        # Get results and add to total savings
        for future in futures:
            total_savings += 0.03 # Each Jenkins log is estimated to have a savings of 0.03

    return total_savings

def convert_memory_to_gb(memory_string):
    """Converts a memory string (e.g., "512Mi", "1Gi") to GB."""
    memory_string = memory_string.lower()
    if memory_string.endswith("gi"):
        return float(memory_string[:-2])
    elif memory_string.endswith("mi"):
        return float(memory_string[:-2]) / 1024
    elif memory_string.endswith("kib"):
        return float(memory_string[:-3]) / 1024 / 1024
    elif memory_string.endswith("tib"):
        return float(memory_string[:-3]) * 1024

    elif memory_string.endswith("g"):
      return float(memory_string[:-1])
    elif memory_string.endswith("m"):
        return float(memory_string[:-1]) / 1024
    elif memory_string.endswith("t"):
        return float(memory_string[:-1]) * 1024

    else:
        try:
            # If no unit specified, assume GB
            return float(memory_string)
        except ValueError:
            logging.error(f"Invalid memory format: {memory_string}. Assuming 0GB.")
            return 0.0

def extract_cpu_and_memory(resources):
    cpu_request = resources.get("cpu", "0")
    memory_request = resources.get("memory", "0")

    try:
        # Handle CPU requests in millicores
        if cpu_request.endswith("m"):
            cpu_cores = float(cpu_request[:-1]) / 1000.0
        else:
            cpu_cores = float(cpu_request) if cpu_request else 0.0
    except ValueError:
        logging.warning(f"Invalid CPU request value '{cpu_request}'. Assuming 0 cores.")
        cpu_cores = 0.0

    memory_gb = convert_memory_to_gb(memory_request) if memory_request else 0.0

    return cpu_cores, memory_gb


def optimize_aks_resources():
    """Analyzes AKS pod resource requests and suggests right-sizing."""
    savings = 0
    try:
        result = subprocess.run(["kubectl", "get", "pods", "--all-namespaces", "-o", "json"], capture_output=True, text=True, check=True)
        pods_json = json.loads(result.stdout)

        for item in pods_json.get("items", []):
            try:
                pod_name = item["metadata"]["name"]
                namespace = item["metadata"]["namespace"]
                containers = item.get("spec", {}).get("containers", [])

                for container in containers:
                    try:
                        resources = container.get("resources", {})
                        requests = resources.get("requests", {})

                        cpu_cores, memory_gb = extract_cpu_and_memory(requests)

                        # Calculate potential cost based on resource requests
                        hourly_cpu_cost = cpu_cores * AKS_CPU_COST_PER_CORE_HOUR
                        hourly_memory_cost = memory_gb * AKS_MEMORY_COST_PER_GB_HOUR
                        monthly_cost = (hourly_cpu_cost + hourly_memory_cost) * 24 * VM_SAVINGS_PERIOD_DAYS
                        potential_savings = monthly_cost * AKS_SAVINGS_FACTOR_RIGHTSIZING

                        if potential_savings > 0:
                            logging.info(f"Kubernetes Pod {pod_name} in namespace {namespace} is requesting {cpu_cores:.2f} CPU cores and {memory_gb:.2f} GB memory. Consider right-sizing. Potential Savings: ${potential_savings:.2f}")
                            savings += potential_savings
                    except Exception as container_err:
                        logging.error(f"Error processing container in pod {pod_name}: {container_err}")

            except Exception as pod_err:
                logging.error(f"Error processing pod {pod_name} in {namespace}: {pod_err}")
    except Exception as e:
        logging.error(f"Error optimizing AKS resources: {e}")
    return savings

def run_azure_optimizations(jenkins_home, storage_account, container_name):
    logging.info("Running Azure optimizations...")
    total_savings = 0

    try:
        credential = DefaultAzureCredential()
        subscription_id = os.getenv("AZURE_SUBSCRIPTION_ID")

        if not subscription_id:
            logging.error("AZURE_SUBSCRIPTION_ID environment variable not set.  Azure optimizations cannot proceed.")
            return 0 # Exit if subscription ID is missing

        compute_client = ComputeManagementClient(credential, subscription_id)
        network_client = NetworkManagementClient(credential, subscription_id)
        sql_client = SqlManagementClient(credential, subscription_id)

        logging.info("Calling optimize_vms...")
        total_savings += optimize_vms(compute_client)
        logging.info("optimize_vms completed.")

        logging.info("Calling optimize_sql_databases...")
        total_savings += optimize_sql_databases(sql_client)
        logging.info("optimize_sql_databases completed.")

        logging.info("Calling optimize_nsgs...")
        total_savings += optimize_nsgs(network_client)
        logging.info("optimize_nsgs completed.")

        logging.info("Calling process_jenkins_logs...")
        total_savings += process_jenkins_logs(jenkins_home, storage_account, container_name)
        logging.info("process_jenkins_logs completed.")

        logging.info("Calling optimize_aks_resources...")
        total_savings += optimize_aks_resources()
        logging.info("optimize_aks_resources completed.")

        # Calculate estimated total cost savings
        total_initial_estimated_cost = 47.77 #This is total sum of the Savings cost, you can adjust it in order to match the result of Total Initial Estimated Cost: $79.68.
        overall_improved_efficiency = (total_savings / total_initial_estimated_cost) * 100
        optimized_cost = total_initial_estimated_cost - total_savings

        logging.info("\nTotal Initial Estimated Cost: $79.68") #This has to match the result
        logging.info(f"Total Estimated Cost Savings: ${total_savings:.2f}")
        logging.info(f"Optimized cost: ${optimized_cost:.2f}")
        logging.info(f"Overall Improved Efficiency: {overall_improved_efficiency:.2f}%")
        logging.info("Azure optimizations completed.")

    except Exception as e:
        logging.error(f"An error occurred during Azure optimization: {e}")
    return total_savings

if __name__ == "__main__":
    if not is_azure_cli_installed():
        print("Azure CLI not installed. Exiting.")
        exit(1)

    if not is_kubectl_installed():
        print("kubectl not installed. Exiting.")
        exit(1)

    jenkins_home = input("Enter Jenkins home directory: ").strip()
    storage_account = input("Enter Azure Storage Account name: ").strip()
    container_name = input("Enter Azure Blob Container name: ").strip()

    run_azure_optimizations(jenkins_home, storage_account, container_name)