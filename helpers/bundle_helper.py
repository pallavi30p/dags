from pathlib import Path

import yaml


CONFIG_PATH = Path(__file__).parent.parent / "config" / "bundle_validation.yaml"


def get_bundle_validation_message() -> str:
    with CONFIG_PATH.open() as f:
        config = yaml.safe_load(f)

    return config["message"]
