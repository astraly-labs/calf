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
        "--committee-path",
        type=str,
        default="committee.json",
        help="committee file path"
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
    return [exec_path, 'run', 'worker', '--db-path', db_path, '--keypair-path', keypair_path, '--validator-keypair-path', validator_keypair_path, '--id', str(id)]

def run_primary_cmd(validator_keypair_path, keypair_path, db_path, exec_path):
    return [exec_path, 'run', 'primary', '--db-path', db_path, '--keypair-path', keypair_path, '--validator-keypair-path', validator_keypair_path]

def worker_processes_commands(n_validators, n_workers, base_path, exec_name):
    return [run_worker_cmd(j, f"{base_path}/validator_{i}/worker_{j}/validator-keypair.json", f"{base_path}/validator_{i}/worker_{j}/keypair.json", f"{base_path}/validator_{i}/worker_{j}/db", f"{base_path}/validator_{i}/worker_{j}/{exec_name}") for i in range(n_validators) for j in range(n_workers)]

def primary_processes_commands(n_validators, base_path, exec_name):
    return [run_primary_cmd(f"{base_path}/validator_{i}/primary/validator-keypair.json", f"{base_path}/validator_{i}/primary/keypair.json", f"{base_path}/validator_{i}/primary/db", f"{base_path}/validator_{i}/primary/{exec_name}") for i in range(n_validators)]

def run_command(command, output_file):
    with open(output_file, "w") as outfile:
        subprocess.run(command, stdout=outfile, stderr=outfile)

def config():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

config()

n_validators = get_args().validators
n_workers = get_args().workers
test_id = get_args().test_id
calf = get_args().calf
committee_path = get_args().committee_path
exec_name = os.path.basename(calf)

create_env(n_validators, n_workers, test_id, calf, committee_path)

commands = worker_processes_commands(n_validators, n_workers, test_id, exec_name) + primary_processes_commands(n_validators, test_id, exec_name)
output_files = workers_processes_output(n_validators, n_workers, test_id) + primaries_processes_output(n_validators, test_id)

with multiprocessing.Pool() as pool:
    pool.starmap(run_command, zip(commands, output_files))