import os
import subprocess
import json
from typing import List, Tuple

def list_files_in_dataset(dataset: str) -> List[Tuple[str, int]]:
    """
    List files in a dataset using dasgoclient.

    Args:
        dataset (str): The dataset name to query.

    Returns:
        List[Tuple[str, int, int]]: A list of tuples (file_path, file_size, nevents) for files in the dataset.
    """
    try:
        command = f'dasgoclient -query="file dataset={dataset}" -json'
        result = subprocess.check_output(command, shell=True, text=True)

        files = json.loads(result)
        file_list = []
        for entry in files:
            for file_info in entry.get('file', []):
                file_list.append((file_info['name'], file_info['size'], file_info['nevents']))
        # Sort by filename for reproducible ordering
        file_list.sort(key=lambda x: x[0])
        return file_list

    except subprocess.CalledProcessError as e:
        print(f"Error querying dataset {dataset}: {e}")
        return []
    except json.JSONDecodeError as e:
        print(f"Error parsing dasgoclient output: {e}")
        return []

def copy_file(file_path: str, destination_dir: str, destination_filename: str = None) -> bool:
    """
    Copy a file using xrdcp.

    Args:
        file_path (str): The full path of the file to copy.
        destination_dir (str): The directory to copy the file to.
        destination_filename (str, optional): The new name for the copied file. Defaults to None in which case the original name is used.

    Returns:
        bool: True if the copy was successful, False otherwise.
    """
    try:
        os.makedirs(destination_dir, exist_ok=True)

        destination = os.path.join(destination_dir, os.path.basename(file_path))
        if destination_filename:
            destination = os.path.join(destination_dir, destination_filename)
        #command = f"xrdcp root://cms-xrd-global.cern.ch/{file_path} {destination}"
        command = f"xrdcp root://cmsxrootd.fnal.gov/{file_path} {destination}" #Change redirector based on server where analysis is running
        subprocess.check_call(command, shell=True)
        print(f"Successfully copied {file_path} to {destination}")
        return True

    except subprocess.CalledProcessError as e:
        print(f"Error copying file {file_path}: {e}")
        return False

if __name__ == "__main__":
    from Nano_v15 import mc_bkg, mc_sig, jetmet
    example_datasets = {}
    example_datasets["TTTo4Q"] = mc_bkg["2024"]["TTbar"]["TTto4Q"]
    example_datasets["XToYHto4b_MX1800_MY100"] = mc_sig["2024"]["XtoYHto4b"]["MX1800_MY100"]
    example_datasets["JetMET_Run2024C"] = jetmet["2024"]["JetMET"]["Run2024C"][0] 


    test_dir = "test_files"

    for name, dataset in example_datasets.items():
        print(f"Processing dataset: {name}")
        files = list_files_in_dataset(dataset)
        if not files:
            print(f"No files found for dataset {name}")
            continue

        # Find the smallest file
        smallest_file = min(files, key=lambda x: x[1])
        print(f"Smallest file in {name}: {smallest_file[0]} ({smallest_file[1] / (1024 ** 2):.2f} MB)")
        # Copy the smallest file
        success = copy_file(smallest_file[0], test_dir, destination_filename=f"{name}_testfile.root")
        if not success:
            print(f"Failed to copy the smallest file for dataset {name}")