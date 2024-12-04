import os
import sys
import libp2p
import json
import argparse
import logging
import shutil
import multiprocessing
import subprocess

def get_args():
    parser = argparse.ArgumentParser(description="Calf test launcher")
    parser.add_argument(
        "--validators",
        type=int,
        required=True,
        help="Validators number (<=> primaries number)"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Workers number per validator"
    )
    parser.add_argument(
        "--test-id",
        type=str,
        default="test",
        help="test name"
    )
    parser.add_argument(
        "--calf",
        type=str,
        default="target/release/calf",
        help="tested executable path"
    )
    parser.add_argument(
        "--build",
        action="store_true",
        help="Build in release mode before running"
    )

    return parser.parse_args()

def generate_keypair(path):
    keypair = libp2p.crypto.ed25519.create_new_key_pair();
    export = {
        "public": keypair.public_key.to_bytes().hex(),
        "secret": keypair.private_key.to_bytes().hex(),
        "peer_id": libp2p.peer.id.ID.from_pubkey(keypair.public_key).to_base58()
    }
    with open(path, "w") as file:
        json.dump(export, file, indent=4)

def create_validator_env(path, workers_number, executable_path, committee_path):
    os.makedirs(path, exist_ok=True)
    generate_keypair(f"{path}/validator-keypair.json")
    for i in range(workers_number):
        os.makedirs(f"{path}/worker_{i}", exist_ok=True)
        shutil.copy(f"{executable_path}", f"{path}/worker_{i}")
        shutil.copy(f"{path}/validator-keypair.json", f"{path}/worker_{i}")
        shutil.copy(f"{committee_path}", f"{path}/worker_{i}/committee.json")
        generate_keypair(f"{path}/worker_{i}/keypair.json")
    os.makedirs(f"{path}/primary", exist_ok=True)
    os.makedirs(f"{path}/primary/dag_log", exist_ok=True)
    shutil.copy(f"{path}/validator-keypair.json", f"{path}/primary")
    shutil.copy(f"{executable_path}", f"{path}/primary")
    shutil.copy(f"{committee_path}", f"{path}/primary/committee.json")
    generate_keypair(f"{path}/primary/keypair.json")

def create_env(validators_number, workers_number, test_id, executable_path, committee_path):
    logging.info(f"Creating environment for {validators_number} validators, {workers_number} workers / validator...")
    os.makedirs(test_id, exist_ok=True)
    for i in range(validators_number):
        os.makedirs(f"{test_id}/validator_{i}", exist_ok=True)
        create_validator_env(f"{test_id}/validator_{i}", workers_number, executable_path, committee_path)
        logging.info(f"Validator {i} environment created")
    logging.info("test environment created")

def primaries_processes_output(n_validators, base_path):
    return [f"{base_path}/validator_{i}/primary/output.log" for i in range(n_validators)]

def workers_processes_output(n_validators, n_workers, base_path):
    return [f"{base_path}/validator_{i}/worker_{j}/output.log" for i in range(n_validators) for j in range(n_workers)]

def run_worker_cmd(id, validator_keypair_path, keypair_path, db_path, exec_path):
    dir_path = os.path.dirname(exec_path)
    exec_name = os.path.basename(exec_path)
    return ['bash', '-c', f'cd {dir_path} && ./{exec_name} run primary --db-path {db_path} --keypair-path {keypair_path} --validator-keypair-path {validator_keypair_path} --worker-id {str(id)}']

def run_primary_cmd(validator_keypair_path, keypair_path, db_path, exec_path):
    dir_path = os.path.dirname(exec_path)
    exec_name = os.path.basename(exec_path)
    return ['bash', '-c', f'cd {dir_path} && ./{exec_name} run primary --db-path {db_path} --keypair-path {keypair_path} --validator-keypair-path {validator_keypair_path}']

def worker_processes_commands(n_validators, n_workers, base_path, exec_name):
    return [run_worker_cmd(0, "validator-keypair.json", "keypair.json", "db", f"{base_path}/validator_{i}/primary/{exec_name}") for i in range(n_validators) for j in range(n_workers)]

def primary_processes_commands(n_validators, base_path, exec_name):
    return [run_primary_cmd("validator-keypair.json", "keypair.json", "db", f"{base_path}/validator_{i}/primary/{exec_name}") for i in range(n_validators)]

def run_command(command, output_file):
    with open(output_file, "w") as outfile:
        subprocess.run(command, stdout=outfile, stderr=outfile)

def config():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

def generate_authority_info():
    return {
        "authority_id": libp2p.peer.id.ID.from_pubkey(libp2p.crypto.ed25519.create_new_key_pair().public_key).to_base58(),
        "authority_pubkey": "0" * 32,
        "primary_address": ["0.0.0.0", "0"],
        "stake": 0,
        "workers_addresses": [
            ["0.0.0.0", "0"]
        ]
    }

def generate_dummy_committee(num_authorities, path):
    committee = {
        "authorities": [generate_authority_info() for _ in range(num_authorities)]
    }
    with open(path, "w", encoding="utf-8") as file:
        json.dump(committee, file, indent=4)

if __name__ == '__main__':
    config()

    args = get_args()
    n_validators = args.validators
    n_workers = args.workers
    test_id = args.test_id
    calf = args.calf
    committee_path = "committee.json"

    if args.build:
        logging.info("Building in release mode...")
        subprocess.run(["cargo", "build", "--release"], check=True)

    exec_name = os.path.basename(calf)

    generate_dummy_committee(n_validators, committee_path)
    create_env(n_validators, n_workers, test_id, calf, committee_path)

    commands = worker_processes_commands(n_validators, n_workers, test_id, exec_name) + primary_processes_commands(n_validators, test_id, exec_name)
    output_files = workers_processes_output(n_validators, n_workers, test_id) + primaries_processes_output(n_validators, test_id)

    commands[0].append('--txs-producer')
    with multiprocessing.Pool() as pool:
        pool.starmap(run_command, zip(commands, output_files))