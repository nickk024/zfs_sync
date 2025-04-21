import subprocess
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

def list_datasets(dataset_type: str = "filesystem") -> List[str]:
    """
    Lists ZFS datasets of a specific type (e.g., filesystem, snapshot, volume).

    Args:
        dataset_type: The type of dataset to list ('filesystem', 'snapshot', 'volume', 'all'). Defaults to 'filesystem'.

    Returns:
        A list of dataset names. Returns an empty list if an error occurs or no datasets are found.
    """
    command = ["zfs", "list", "-H", "-o", "name", "-t", dataset_type]
    try:
        logger.debug(f"Running command: {' '.join(command)}")
        result = subprocess.run(command, capture_output=True, text=True, check=True, encoding='utf-8')
        datasets = result.stdout.strip().split('\n')
        # Filter out empty strings that might result from split
        datasets = [ds for ds in datasets if ds]
        logger.debug(f"Found datasets: {datasets}")
        return datasets
    except FileNotFoundError:
        logger.error("ZFS command not found. Is ZFS installed and in the system's PATH?")
        return []
    except subprocess.CalledProcessError as e:
        logger.error(f"Error listing ZFS {dataset_type}s: {e}")
        logger.error(f"Stderr: {e.stderr}")
        return []
    except Exception as e:
        logger.error(f"An unexpected error occurred while listing datasets: {e}")
        return []

def get_dataset_properties(dataset_name: str, properties: Optional[List[str]] = None) -> Optional[Dict[str, str]]:
    """
    Gets specific properties for a ZFS dataset.

    Args:
        dataset_name: The name of the dataset (e.g., 'pool/mydata').
        properties: A list of property names to retrieve (e.g., ['mountpoint', 'used']).
                    If None or empty, retrieves all properties. Defaults to None.

    Returns:
        A dictionary where keys are property names and values are property values.
        Returns None if the dataset is not found or an error occurs.
    """
    prop_list = ",".join(properties) if properties else "all"
    command = ["zfs", "get", "-H", "-p", "-o", "property,value", prop_list, dataset_name]
    try:
        logger.debug(f"Running command: {' '.join(command)}")
        result = subprocess.run(command, capture_output=True, text=True, check=True, encoding='utf-8')
        output_lines = result.stdout.strip().split('\n')
        dataset_properties = {}
        for line in output_lines:
            if not line:
                continue
            try:
                prop, value = line.split('\t', 1)
                dataset_properties[prop] = value
            except ValueError:
                logger.warning(f"Could not parse property line for {dataset_name}: {line}")
        logger.debug(f"Properties for {dataset_name}: {dataset_properties}")
        return dataset_properties
    except FileNotFoundError:
        logger.error("ZFS command not found. Is ZFS installed and in the system's PATH?")
        return None
    except subprocess.CalledProcessError as e:
        # Handle cases where the dataset might not exist
        if "dataset does not exist" in e.stderr:
            logger.warning(f"Dataset '{dataset_name}' not found.")
        else:
            logger.error(f"Error getting properties for {dataset_name}: {e}")
            logger.error(f"Stderr: {e.stderr}")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred while getting properties for {dataset_name}: {e}")
        return None


if __name__ == '__main__':
    # Example usage (for testing purposes)
    logging.basicConfig(level=logging.DEBUG)
    print("Listing filesystems:")
    filesystems = list_datasets("filesystem")
    if filesystems:
        for fs in filesystems:
            print(f"- {fs}")
            # Get specific properties for the first filesystem found
            if fs == filesystems[0]:
                 props_to_get = ["mountpoint", "used", "available", "compression"]
                 print(f"  Getting properties ({', '.join(props_to_get)}) for {fs}:")
                 props = get_dataset_properties(fs, props_to_get)
                 if props:
                     for key, value in props.items():
                         print(f"    {key}: {value}")
                 else:
                     print(f"    Could not retrieve properties for {fs}.")
                 print(f"  Getting all properties for {fs}:")
                 all_props = get_dataset_properties(fs)
                 if all_props:
                      # Print only a few for brevity in example
                      count = 0
                      for key, value in all_props.items():
                          print(f"    {key}: {value}")
                          count += 1
                          if count >= 5:
                              print("    ...")
                              break
                 else:
                     print(f"    Could not retrieve all properties for {fs}.")

    else:
        print("No filesystems found or error occurred.")

    print("\nListing snapshots:")
    snapshots = list_datasets("snapshot")
    if snapshots:
        for snap in snapshots:
            print(f"- {snap}")
    else:
        print("No snapshots found or error occurred.")