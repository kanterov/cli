import json
import sys
import argparse

if __name__ == "__main__":
    config = json.load(sys.stdin)

    parser = argparse.ArgumentParser()
    parser.add_argument("--phase")
    args, _ = parser.parse_known_args()

    pre_initialize = {
        "my_job": {
            "tasks": [
                {
                    "existing_cluster_id": "abc",
                    "notebook_task": {
                        "notebook_path": "/Workspace/foo",
                    },
                    "task_key": "print_hello",
                }
            ]
        }
    }

    initialize = {
        "my_job": {
            "tasks": [
                {
                    # mutator can change job settings
                    "existing_cluster_id": "cde",
                    "notebook_task": {
                        "notebook_path": "/Workspace/foo",
                    },
                    "task_key": "print_hello",
                }
            ]
        },
        # resource_generator can add more jobs
        "my_job_2": {
            "tasks": [
                {
                    # mutator can change job settings
                    "existing_cluster_id": "cde",
                    "notebook_task": {
                        "notebook_path": "/Workspace/foo",
                    },
                    "task_key": "print_hello",
                }
            ]
        },
    }

    if args.phase == "pre-initialize":
        jobs = pre_initialize
    else:
        # at this stage we can access variables
        assert config["variables"]

        jobs = initialize

    config["resources"] = config.get("resources", {})
    config["resources"]["jobs"] = config["resources"].get("jobs", {})

    config["resources"]["jobs"] = {**config["resources"]["jobs"], **jobs}

    print(json.dumps(config, indent=2), file=sys.stderr)

    print(json.dumps(config))
