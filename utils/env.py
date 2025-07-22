import os

from dotenv import load_dotenv


def check_is_prod():
    is_prod = os.getenv("PROD") == "true"
    if is_prod:
        load_dotenv('.env.prod')
        return is_prod

    load_dotenv('.env.dev')
    is_in_docker = os.getenv("IN_DOCKER") == "true"
    if is_in_docker:
        os.environ["SERVER"] = "host.docker.internal"

    return is_prod