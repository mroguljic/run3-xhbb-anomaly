from condor.template_job_batch import stage_input_files, create_filelist_txt
import json
import tempfile
from pathlib import Path
from tagger_studies.templating_for_tagger import run_working_point_scan
from tagger_studies.config import MANIFEST_PATH, PROCESS_MAPS, TEMPLATE_STORE_DIR, YEAR
from tagger_studies.analyze_yield import optimize_tagger


print("=== Running WP optimization for these settings ===\n")
print(f"Manifest path: {MANIFEST_PATH}")
print(f"Process maps: {PROCESS_MAPS}\n")
print(f"Template store dir: {TEMPLATE_STORE_DIR}\n")
print(f"Year: {YEAR}\n")

def check_output(output_dir, process_maps):
    for process, subprocesses in process_maps.items():
        for subprocess in subprocesses:
            output_file = Path(output_dir) / f"{subprocess}_templates.root"
            if not output_file.exists():
                return False
    return True

def run_templating(manifest_path, process_maps, template_store_dir, year):
    manifest = json.load(open(manifest_path, "r"))
    manifest = manifest["datasets"]

    print(manifest.keys())
    for process, subprocesses in process_maps.items():
        for subprocess in subprocesses:
            print(subprocess)
            ds_info = manifest[subprocess]
            batch_id = f"{subprocess}_tpl_chunk_0"
            skim_paths = ds_info["batches"][batch_id]["skim_paths"]
            with tempfile.TemporaryDirectory(prefix=f"tagger_studies_{subprocess}_") as staging_dir:
                print("=== Staging Input Skims ===\n")
                filelist_path = str(Path(staging_dir) / f"filelist_{subprocess}.txt")
                local_files = stage_input_files(skim_paths, staging_dir)
                create_filelist_txt(local_files, filelist_path)
                run_working_point_scan(filelist_path, f"{template_store_dir}/{subprocess}_templates.root", year)


if __name__ == "__main__":
    templates_exist = check_output(TEMPLATE_STORE_DIR, PROCESS_MAPS)
    if templates_exist:
        print(f"Templates already exist in {TEMPLATE_STORE_DIR}.")
        choice = input("Do you wish to rerun the templating? (y/N)")  
        if choice.lower() == "y":
            run_templating(MANIFEST_PATH, PROCESS_MAPS, TEMPLATE_STORE_DIR, YEAR)
        else:
            print("Skipping templating.")
    else:
        run_templating(MANIFEST_PATH, PROCESS_MAPS, TEMPLATE_STORE_DIR, YEAR)

    optimize_tagger("xbb")
    optimize_tagger("antiqcd")