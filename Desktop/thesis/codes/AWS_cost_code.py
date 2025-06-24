import os
import gzip
import shutil
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import boto3
import subprocess
import logging
import json  

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
        logging.info(f"Load resources name in {filename}")
        print("Resource Names Loaded:")
        print(json.dumps(resource_names, indent=4))  # Pretty print JSON to console
    except FileNotFoundError:
        logging.warning(f"Resource names file {filename} not found. Using AWS-generated names.")
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON from {filename}. Using AWS-generated names.")
        resource_names = {}

# AWS Cost Assumptions
EC2_ON_DEMAND_COST_PER_HOUR = 0.0468  # t3.micro in us-east-1, change based on your instance types and region
EC2_RESERVED_COST_PER_HOUR = 0.0468 * 0.40  # Adjust savings based on your commitment
RDS_COST_PER_HOUR = 0.041  # db.t3.micro in us-east-1, change based on your instance types and region
NAT_GATEWAY_COST_PER_HOUR = 0.045  # US East (N. Virginia)
S3_STANDARD_COST_PER_GB = 0.023  # $ per GB (Standard, US East (N. Virginia))
S3_INTELLIGENT_TIERING_COST_PER_GB = 0.0195  # Intelligent-Tiering, US East (N. Virginia))

# Savings Factors
SECURITY_GROUP_SAVINGS = 6.00  # Reduced blast radius, less audit findings
EC2_SAVINGS_PERIOD_DAYS = 30
RDS_SAVINGS_FACTOR = 0.4  # Aurora Serverless savings (adjust based on workload)
NAT_GATEWAY_SAVINGS_FACTOR = 0.25  # Savings using Transit Gateway (adjust based on network)
JENKINS_LOG_RETENTION_DAYS = 7  # Only archive logs from the last 7 days

# Kubernetes Cost Assumptions (Adjust based on your cluster)
K8S_CPU_COST_PER_CORE_HOUR = 0.04  # Adjust based on actual cost of CPU in your cluster
K8S_MEMORY_COST_PER_GB_HOUR = 0.01  # Adjust based on actual cost of memory in your cluster
K8S_SAVINGS_FACTOR_RIGHTSIZING = 0.20  # Potential savings from right-sizing containers

def is_aws_cli_installed():
    try:
        subprocess.run(["aws", "--version"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        logging.warning("AWS CLI not found. Please install it to proceed.")
        return False

def is_kubectl_installed():
    try:
        subprocess.run(["kubectl", "version", "--client"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        logging.warning("kubectl not found. Please install it to proceed.")
        return False

def calculate_monthly_savings(hourly_cost, savings_factor):
    return hourly_cost * 24 * EC2_SAVINGS_PERIOD_DAYS * savings_factor

def describe_aws_resources(client, describe_method, resource_key):
    try:
        paginator = client.get_paginator(describe_method.__name__.replace('describe_', ''))
        response_iterator = paginator.paginate()
        resources = []
        for response in response_iterator:
            resources.extend(response[resource_key])
        return resources
    except Exception as e:
        logging.error(f"Error describing {resource_key}: {e}")
        return []

def optimize_security_groups(ec2):
    savings = 0
    initial_cost = 0
    try:
        security_groups = describe_aws_resources(ec2, ec2.describe_security_groups, 'SecurityGroups')
        for sg in security_groups:
            sg_cost = 0 # Calc the number of open rule and add
            for rule in sg.get('IpPermissions', []):
                for ip_range in rule.get('IpRanges', []):
                    if ip_range['CidrIp'] == "0.0.0.0/0" or ip_range['CidrIp'] == "::/0":  # also check for ipv6 open rules
                        logging.warning(f"Security Group {sg['GroupId']} has an open rule! Consider restricting it. Potential Savings: ${SECURITY_GROUP_SAVINGS:.2f}")
                        savings += SECURITY_GROUP_SAVINGS
                        sg_cost = sg_cost + SECURITY_GROUP_SAVINGS

            initial_cost = initial_cost + sg_cost
    except Exception as e:
        logging.error(f"Error optimizing Security Groups: {e}")
    return savings, initial_cost

def optimize_ec2_instances(ec2):
    savings = 0
    initial_cost = 0
    try:
        reservations = describe_aws_resources(ec2, ec2.describe_instances, 'Reservations')
        for res in reservations:
            for inst in res['Instances']:
                if inst['State']['Name'] == "running":
                    instance_type = inst.get('InstanceType', 'Unknown')
                    savings_per_instance = calculate_monthly_savings(EC2_ON_DEMAND_COST_PER_HOUR - EC2_RESERVED_COST_PER_HOUR, 1)
                    logging.info(f"EC2 Instance {inst['InstanceId']} ({instance_type}) is running. Consider rightsizing, stopping, or using reserved instances. Potential Savings: ${savings_per_instance:.2f}")
                    savings += savings_per_instance
                    initial_cost += calculate_monthly_savings(EC2_ON_DEMAND_COST_PER_HOUR, 1) # calc all ec2 costs
    except Exception as e:
        logging.error(f"Error optimizing EC2 Instances: {e}")
    return savings, initial_cost

def optimize_rds(rds):
    savings = 0
    initial_cost = 0
    try:
        dbs = describe_aws_resources(rds, rds.describe_db_instances, 'DBInstances')
        for db in dbs:
            if db['DBInstanceStatus'] == "available":
                engine = db.get('Engine', 'Unknown')
                if engine != 'aurora-serverless':  # check if the engine is already Aurora Serverless
                    savings_per_db = calculate_monthly_savings(RDS_COST_PER_HOUR, RDS_SAVINGS_FACTOR)
                    logging.info(f"RDS Instance {db['DBInstanceIdentifier']} ({engine}) is running. By using Aurora Serverless (if applicable). Potential Savings: ${savings_per_db:.2f}")
                    savings += savings_per_db
                    initial_cost += calculate_monthly_savings(RDS_COST_PER_HOUR, 1) # Base RDS costs
    except Exception as e:
        logging.error(f"Error optimizing RDS Instances: {e}")
    return savings, initial_cost

def check_nat_gateway_costs(ec2):
    savings = 0
    initial_cost = 0
    try:
        nat_gateways = describe_aws_resources(ec2, ec2.describe_nat_gateways, 'NatGateways')
        for nat in nat_gateways:
            savings_per_nat = calculate_monthly_savings(NAT_GATEWAY_COST_PER_HOUR, NAT_GATEWAY_SAVINGS_FACTOR)
            logging.info(f"NAT Gateway {nat['NatGatewayId']} might be costing you money! Use Transit Gateway. Potential Savings: ${savings_per_nat:.2f}")
            savings += savings_per_nat
            initial_cost += calculate_monthly_savings(NAT_GATEWAY_COST_PER_HOUR, 1) # base Nat costs.
    except Exception as e:
        logging.error(f"Error checking NAT Gateway costs: {e}")
    return savings, initial_cost

def upload_log_to_s3(log_file, job_name, build_number, s3_bucket):
    try:
        if not os.path.exists(log_file) or os.path.getsize(log_file) == 0:
            logging.warning(f"Skipping {log_file}: Empty or missing.")
            return 0

        original_size = os.path.getsize(log_file) / (1024 ** 3)  # Convert bytes to GB
        compressed_file = f"{log_file}.gz"

        with open(log_file, 'rb') as f_in, gzip.open(compressed_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

        compressed_size = os.path.getsize(compressed_file) / (1024 ** 3)  # Convert bytes to GB
        cost_savings = (original_size * S3_STANDARD_COST_PER_GB) - (compressed_size * S3_INTELLIGENT_TIERING_COST_PER_GB)

        s3_path = f"{s3_bucket}/{job_name}-{build_number}.log.gz"
        subprocess.run(["aws", "s3", "cp", compressed_file, s3_path, "--storage-class", "INTELLIGENT_TIERING"], check=True)
        logging.info(f"Uploaded {job_name}/{build_name} to {s3_path}. Savings: ${cost_savings:.2f}")
        return cost_savings

    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to upload {job_name}/{build_number}: {e}")
        return 0
    except Exception as e:
        logging.error(f"Error processing {log_file}: {e}")
        return 0

def process_jenkins_logs(jenkins_home, s3_bucket):
    total_savings = 0
    total_initial_cost = 0 # New metric
    if not os.path.exists(jenkins_home):
        logging.warning(f"Jenkins directory {jenkins_home} not found. Skipping log processing.")
        return 0, 0

    today = datetime.now()
    cutoff_date = today - timedelta(days=JENKINS_LOG_RETENTION_DAYS)

    with ThreadPoolExecutor(max_workers=os.cpu_count() or 10) as executor:
        futures = []
        for job_dir in os.listdir(os.path.join(jenkins_home, "jobs")):
            builds_path = os.path.join(jenkins_home, "jobs", job_dir, "builds")

            if os.path.isdir(builds_path):
                for build_dir in os.listdir(builds_path):
                    log_file = os.path.join(builds_path, build_dir, "log")
                    try:
                        log_modified_time = datetime.fromtimestamp(os.path.getmtime(log_file))
                        if os.path.isfile(log_file) and log_modified_time >= cutoff_date:
                           original_size = os.path.getsize(log_file) / (1024 ** 3)  # Convert bytes to GB
                           total_initial_cost += original_size * S3_STANDARD_COST_PER_GB
                           futures.append(executor.submit(upload_log_to_s3, log_file, job_dir, build_dir, s3_bucket))
                    except FileNotFoundError:
                        pass  # Log file might not exist

        for future in futures:
            total_savings += future.result()

    return total_savings, total_initial_cost

def get_kubernetes_pod_resource_requests():
    """
    Retrieves CPU and memory resource requests for all pods in all namespaces.
    Returns a list of dictionaries, each containing pod name, namespace, CPU request, and memory request.
    """
    try:
        result = subprocess.run(["kubectl", "get", "pods", "--all-namespaces", "-o", "json"], capture_output=True, text=True, check=True)
        pods_json = json.loads(result.stdout)

        pod_resources = []
        for item in pods_json.get("items", []):
            pod_name = item["metadata"]["name"]
            namespace = item["metadata"]["namespace"]
            containers = item.get("spec", {}).get("containers", [])

            for container in containers:
                resources = container.get("resources", {})
                requests = resources.get("requests", {})
                cpu_request = requests.get("cpu", "0")  # Default to "0" if not specified
                memory_request = requests.get("memory", "0")  # Default to "0" if not specified

                pod_resources.append({
                    "pod_name": pod_name,
                    "namespace": namespace,
                    "cpu_request": cpu_request,
                    "memory_request": memory_request
                })
        return pod_resources

    except subprocess.CalledProcessError as e:
        logging.error(f"Error getting pod resource requests: {e}")
        return []

def convert_memory_to_gb(memory_str):
    """Converts memory string (e.g., "2Gi", "512Mi") to GB."""
    memory_str = memory_str.lower()
    if memory_str.endswith("gi"):
        return float(memory_str[:-2])
    elif memory_str.endswith("mi"):
        return float(memory_str[:-2]) / 1024
    elif memory_str.endswith("ki"):
        return float(memory_str[:-2]) / (1024 * 1024)
    elif memory_str.endswith("bytes") or memory_str.endswith("b"):
        return float(memory_str[:-1]) / (1024 * 1024 * 1024)
    else:
        try:
            return float(memory_str)  # Assume it's already in GB or can be converted directly
        except ValueError:
            logging.warning(f"Could not parse memory string: {memory_str}. Assuming 0GB.")
            return 0.0

def optimize_kubernetes_resources():
    """Analyzes Kubernetes pod resource requests and suggests right-sizing."""
    savings = 0
    initial_cost = 0 # This had to be added
    pod_resources = get_kubernetes_pod_resource_requests()

    for resource in pod_resources:
        pod_name = resource["pod_name"]
        namespace = resource["namespace"]
        cpu_request = resource["cpu_request"]
        memory_request = resource["memory_request"]

        try:
            cpu_cores = float(cpu_request) if cpu_request else 0.0
        except ValueError:
            logging.warning(f"Invalid CPU request value '{cpu_request}' for pod {pod_name} in namespace {namespace}. Assuming 0 cores.")
            cpu_cores = 0.0

        memory_gb = convert_memory_to_gb(memory_request) if memory_request else 0.0

        # Calculate potential cost based on resource requests
        hourly_cpu_cost = cpu_cores * K8S_CPU_COST_PER_CORE_HOUR
        hourly_memory_cost = memory_gb * K8S_MEMORY_COST_PER_GB_HOUR
        monthly_cost = (hourly_cpu_cost + hourly_memory_cost) * 24 * EC2_SAVINGS_PERIOD_DAYS # Using EC2_SAVINGS_PERIOD_DAYS for consistency
        potential_savings = monthly_cost * K8S_SAVINGS_FACTOR_RIGHTSIZING

        if potential_savings > 0:
            logging.info(f"Pod {pod_name} in namespace {namespace} is requesting {cpu_cores:.2f} CPU cores and {memory_gb:.2f} GB memory. Consider right-sizing. Potential Savings: ${potential_savings:.2f}")
            savings += potential_savings
        initial_cost = initial_cost + monthly_cost
    return savings, initial_cost

def run_aws_optimizations(jenkins_home, s3_bucket, ec2, rds):
    logging.info("Running AWS optimizations...")
    total_savings = 0
    total_initial_cost = 0  # Track initial estimated cost

    try:

        # Security Groups
        sg_savings, sg_initial_cost = optimize_security_groups(ec2)
        total_savings += sg_savings
        total_initial_cost += sg_initial_cost

        # EC2 Instances
        ec2_savings, ec2_initial_cost = optimize_ec2_instances(ec2)
        total_savings += ec2_savings
        total_initial_cost += ec2_initial_cost

        # RDS Instances
        rds_savings, rds_initial_cost = optimize_rds(rds)
        total_savings += rds_savings
        total_initial_cost += rds_initial_cost

        # NAT Gateways
        nat_savings, nat_initial_cost = check_nat_gateway_costs(ec2)
        total_savings += nat_savings
        total_initial_cost += nat_initial_cost

        # Jenkins Logs
        jenkins_savings, jenkins_initial_cost = process_jenkins_logs(jenkins_home, s3_bucket)
        total_savings += jenkins_savings
        total_initial_cost += jenkins_initial_cost

        # Kubernetes Resources
        k8s_savings, k8s_initial_cost = optimize_kubernetes_resources()
        total_savings += k8s_savings
        total_initial_cost += k8s_initial_cost

        # Calculate Efficiency Factor
        optimized_cost = total_initial_cost - total_savings
        if total_initial_cost > 0:
            efficiency_factor = (total_savings / total_initial_cost) * 100
        else:
            efficiency_factor = 0  # Avoid division by zero

        logging.info(f"\nTotal Initial Estimated Cost: ${total_initial_cost:.2f}")
        logging.info(f"Total Estimated Cost Savings: ${total_savings:.2f}")
        logging.info(f"Optimized cost: ${optimized_cost:.2f}")
        logging.info(f"Overall Improved Efficiency: {efficiency_factor:.2f}%")

        logging.info("AWS optimizations completed.")

    except Exception as e:
        logging.error(f"An error occurred during AWS optimization: {e}")

if __name__ == "__main__":
    if not is_aws_cli_installed():
        print("AWS CLI not installed. Exiting.")
        exit(1)

    if not is_kubectl_installed():
        print("kubectl not installed. Exiting.")
        exit(1)

    # Load resource names from file (if it exists)
    load_resource_names()

    jenkins_home = input("Enter Jenkins home directory: ").strip()
    s3_bucket = input("Enter S3 bucket name (e.g., s3://my-bucket): ").strip()

    if not s3_bucket.startswith("s3://"):
        print("Invalid S3 bucket format. Example: s3://my-bucket")
        exit(1)

    ec2 = boto3.client('ec2')
    rds = boto3.client('rds')
    run_aws_optimizations(jenkins_home, s3_bucket, ec2, rds)