"""This ETL Pipeline is for dim_demographics"""
from pyspark.sql.functions import avg, col
from pyspark.sql.functions import sum as spark_sum

from udacity_capstone.pipelines import Pipeline


class DimDemographicsPipeline(Pipeline):
    """This ETL Pipeline is for dim_demographics"""

    def __init__(self, filepath: str = ""):
        super().__init__("dim_demographics", "overwrite", filepath)

    def extract(self):
        """Use current Spark session to read CSV file"""
        self.df = self.spark.read.csv(
            path=self.filepath,
            header=True,
            inferSchema=True,
            sep=";",
        ).dropna()

        return self.df

    def transform(self):
        """
        Use current Spark session to transform data to this model:
        |-- id: string
        |-- name: string
        |-- male_population: long
        |-- female_population: long
        |-- veterans_count: long
        |-- foreigners_count: long
        |-- avg_household_size: double
        """
        self.df = (
            self.df.select(
                col("State Code").alias("id"),
                col("State").alias("name"),
                col("City").alias("city"),
                col("Male Population").alias("male_population"),
                col("Female Population").alias("female_population"),
                col("Number of Veterans").alias("veterans_count"),
                col("Foreign-born").alias("foreigners_count"),
                col("Average Household Size").alias("avg_household_size"),
            )
            .groupBy("id", "name")
            .agg(
                spark_sum("male_population").alias("male_population"),
                spark_sum("female_population").alias("female_population"),
                spark_sum("veterans_count").alias("veterans_count"),
                spark_sum("foreigners_count").alias("foreigners_count"),
                avg("avg_household_size").alias("avg_household_size"),
            )
        )
        self.df.createOrReplaceTempView("dim_demographics")

        return self.df
