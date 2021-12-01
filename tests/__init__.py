import os


API_KEY = os.environ.get("NTROPY_API_KEY")


if not API_KEY:
    raise RuntimeError("Environment variable NTROPY_API_KEY is not defined")
