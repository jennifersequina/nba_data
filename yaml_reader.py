import yaml


def read_config(config_path: str) -> dict:
    with open(config_path) as file:
        config = yaml.load(file, Loader=yaml.FullLoader)
    return config
