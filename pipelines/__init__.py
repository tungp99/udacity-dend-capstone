"""Abstract Pipeline class to force implementation on derived pipelines"""
import logging
from abc import ABC, abstractmethod
from configparser import ConfigParser
from os import environ
from typing import Literal

from pyspark import SparkConf
from pyspark.sql import DataFrame, SparkSession
from udacity_capstone.pipelines.helpers import Environment

jdbc_properties = {
    "driver": "com.amazon.redshift.jdbc42.Driver",
    "tempdir": None,
}

logger = logging.getLogger(__name__)


def get_spark():
    """Initialize Spark session, create if not exists"""
    config = ConfigParser()
    config.read("config.ini")

    # this will help Spark acknowledging the AWS credentials
    environ["AWS_ACCESS_KEY_ID"] = config["CLOUD"]["AWS_ACCESS_KEY_ID"]
    environ["AWS_SECRET_ACCESS_KEY"] = config["CLOUD"]["AWS_SECRET_ACCESS_KEY"]
    environ["INPUT_DIR"] = config["GENERAL"]["INPUT_DIR"]
    environ["OUTPUT_DIR"] = config["GENERAL"]["OUTPUT_DIR"]

    jdbc_properties["tempdir"] = config["GENERAL"]["INPUT_DIR"]

    environ[
        "REDSHIFT_URI"
    ] = f"jdbc:redshift://{config['REDSHIFT']['HOST']}:{config['REDSHIFT']['PORT']}/{config['REDSHIFT']['DB']}?user={config['REDSHIFT']['USER']}&password={config['REDSHIFT']['PASSWORD']}"

    conf = (
        SparkConf().setAppName("thelastone")
        # .set("spark.jars.packages", "com.amazon.redshift:redshift-jdbc42:2.1.0.8")
    )

    logging.basicConfig(level=logging.INFO)
    return SparkSession.builder.config(conf=conf).getOrCreate()


class Pipeline(ABC):
    """Abstract Pipeline class to force implementation on derived pipelines"""

    def __init__(
        self,
        table: str,
        write_mode: Literal["append", "overwrite", "error", "ignore"],
        filepath: str = "",
        env: Environment = Environment.LOCAL,
    ):
        self.spark = get_spark()
        self.df: DataFrame
        self.table = table
        self.write_mode = write_mode
        self.filepath: str = filepath
        self.env = env

    @abstractmethod
    def extract(self) -> DataFrame:
        """Use current Spark session to read CSV file"""
        raise NotImplementedError

    @abstractmethod
    def transform(self) -> DataFrame:
        """Use current Spark session to transform data to a specific model"""
        raise NotImplementedError

    def load(self):
        """Save data to Amz Redshift or Local file of my choice"""
        logger.info("saving data...")

        if self.env == Environment.CLOUD:
            self.df.write.jdbc(
                url=environ["REDSHIFT_URI"],
                table=self.table,
                mode=self.write_mode,
                properties=jdbc_properties,
            )
            return

        self.df.write.csv(
            path=f"{environ['OUTPUT_DIR']}/{self.table}",
            mode=self.write_mode,
            header=True,
        )

    def run(self):
        """The executor"""
        self.extract()
        self.transform()
        self.load()


class TestPipeline(ABC):
    """Abstract TestPipeline class to force implementation on derived test pipelines"""

    def __init__(self, pipeline: Pipeline):
        self.pipeline = pipeline

    @abstractmethod
    def transform(self) -> DataFrame:
        """Test to make sure data was transformed to a specific model"""
        raise NotImplementedError

    def load(self):
        """Test to make sure Redshift table has data"""

        if self.pipeline.env == Environment.CLOUD:
            assert (
                self.pipeline.spark.read.jdbc(
                    url=environ["REDSHIFT_URI"],
                    table=self.pipeline.table,
                    properties={
                        **jdbc_properties,
                        "query": f"select count(*) from {self.pipeline.table}",
                    },
                )
                .load()
                .collect()[0][0]
                > 0
            ), f"[TestPipeline][{self.pipeline.__class__.__name__}] table {self.pipeline.table} has no data"

            return

    def run(self):
        """Check quality of tables from Redshift"""
        self.transform()
        self.load()
