"""
Contains user-defined functions to work with PySpark
"""
from os import environ
from enum import Enum

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


class Environment(str, Enum):
    """To decide if I save/load data Online or Offline (local files)"""

    LOCAL = "LOCAL"
    CLOUD = "CLOUD"


def translate_state_name_to_code():
    """
    Convert state name to state code, only use with PySpark
    e.g: Ohio -> OH
         South Carolina -> SC
    """

    states = {}
    with open(f"{environ['INPUT_DIR']}/us-states.txt", mode="r", encoding="utf8") as f:
        for line in f.read().splitlines():
            _id, name = line.split("=")
            states[name.strip()] = _id.strip()

    def translate(value: str):
        for name, _id in states.items():
            if name == value.upper():
                return _id
        return None

    return udf(translate, StringType())


def translate_visa():
    """
    Convert visa type number to string, only use with PySpark
    e.g: 1 -> Business
    """
    types = {}
    with open(f"{environ['INPUT_DIR']}/visa.txt", mode="r", encoding="utf8") as f:
        for line in f.read().splitlines():
            _id, name = line.split("=")
            types[int(_id)] = name.strip()

    def translate(value: int):
        for _id, name in types.items():
            if _id == value:
                return name
        return None

    return udf(translate, StringType())


def translate_travel_method():
    """
    Convert travel method number to string, only use with PySpark
    e.g: 1 -> Air
    """
    methods = {}
    with open(
        f"{environ['INPUT_DIR']}/travel-methods.txt", mode="r", encoding="utf8"
    ) as f:
        for line in f.read().splitlines():
            _id, name = line.split("=")
            methods[int(_id)] = name.strip()

    def translate(value: int):
        for _id, name in methods.items():
            if _id == value:
                return name
        return None

    return udf(translate, StringType())
