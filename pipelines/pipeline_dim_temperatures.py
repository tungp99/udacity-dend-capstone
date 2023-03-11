"""This ETL Pipeline is for dim_temperatures"""
from pyspark.sql.functions import year, month, dayofmonth, col, avg

from udacity_capstone.pipelines import Pipeline
from udacity_capstone.pipelines.helpers import translate_state_name_to_code


class DimTemperaturesPipeline(Pipeline):
    """This ETL pipeline is for dim_temperatures"""

    def __init__(self, filepath: str = ""):
        super().__init__("dim_temperatures", "overwrite", filepath)

    def extract(self):
        """Use current Spark session to read CSV file"""
        self.df = (
            self.spark.read.csv(
                path=self.filepath,
                header=True,
                inferSchema=True,
            )
            .where(col("Country") == "United States")
            .dropna()
        )

        return self.df

    def transform(self):
        """
        Use current Spark session to transform data to this model:
        |-- state_id: string (nullable = true)
        |-- year: integer (nullable = true)
        |-- month: integer (nullable = true)
        |-- avg_tmp: double (nullable = true)
        """
        self.df = (
            self.df.select(
                translate_state_name_to_code()("State").alias("state_id"),
                year("dt").alias("year"),
                month("dt").alias("month"),
                dayofmonth("dt").alias("day"),
                col("AverageTemperature").alias("avg_tmp"),
            )
            .groupBy("state_id", "year", "month")
            .agg(avg("avg_tmp").alias("avg_tmp"))
            .dropna(subset=["state_id"])
        )
        self.df.createOrReplaceTempView("dim_temperatures")

        return self.df
