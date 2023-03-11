"""This ETL Pipeline is for dim_immigrations"""
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

from udacity_capstone.pipelines import Pipeline
from udacity_capstone.pipelines.helpers import translate_travel_method, translate_visa


class DimImmigrationsPipeline(Pipeline):
    """This ETL Pipeline is for dim_immigrations"""

    def __init__(self, filepath: str = ""):
        super().__init__("dim_immigrations", "overwrite", filepath)

    def extract(self):
        """Use current Spark session to read CSV file"""

        self.df = self.spark.read.csv(
            path=self.filepath,
            header=True,
            inferSchema=True,
        ).dropna(subset=["i94mode"])

        return self.df

    def transform(self):
        """
        Use current Spark session to transform data to this model:
        |-- cicid: integer
        |-- year: integer
        |-- month: integer
        |-- travel_method: string
        |-- origin_country: integer
        |-- origin_port: string
        |-- terminus_state_id: string
        |-- migrant_yob: integer
        |-- visa_type: string
        |-- visa_class: string
        """
        self.df = self.df.select(
            "cicid",
            col("i94yr").cast(IntegerType()).alias("year"),
            col("i94mon").cast(IntegerType()).alias("month"),
            translate_travel_method()(col("i94mode").cast(IntegerType())).alias(
                "travel_method"
            ),
            col("i94cit").cast(IntegerType()).alias("origin_country"),
            col("i94port").alias("origin_port"),
            col("i94addr").alias("terminus_state_id"),
            col("biryear").cast(IntegerType()).alias("migrant_yob"),
            translate_visa()(col("i94visa").cast(IntegerType())).alias("visa_type"),
            col("visatype").alias("visa_class"),
        )
        self.df.createOrReplaceTempView("dim_immigrations")

        return self.df
