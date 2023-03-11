"""This ETL pipeline is for fact_immigrations"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from udacity_capstone.pipelines import Pipeline


class FactImmigrationsPipeline(Pipeline):
    """This EL pipeline is for fact_immigrations"""

    def __init__(self):
        super().__init__("fact_immigrations", "overwrite")

        self.df_t: DataFrame
        self.df_d: DataFrame
        self.df_i: DataFrame

    def extract(self):
        """Use current Spark session to extract data from Redshift dimension tables"""
        self.df_t = self.spark.sql("select * from dim_temperatures").alias("df_t")
        self.df_d = self.spark.sql("select * from dim_demographics").alias("df_d")
        self.df_i = self.spark.sql("select * from dim_immigrations").alias("df_i")

        return self.df

    def transform(self):
        """
        Use current Spark session to transform data to this model:
        |-- cicid: integer
        |-- year: integer
        |-- month: integer
        |-- travel_method: string
        |-- origin_country: integer
        |-- terminus_state_id: string
        |-- terminus_state_name: string
        |-- terminus_temperature: double
        |-- terminus_avg_household_size: double
        |-- terminus_male_population: long
        |-- terminus_female_population: long
        |-- visa_type: string
        |-- visa_class: string
        """
        self.df = (
            self.df_i.join(self.df_d)
            .where(self.df_i.terminus_state_id == self.df_d.id)
            .join(self.df_t)
            .where(
                (self.df_i.terminus_state_id == self.df_t.state_id)
                & (self.df_i.year == self.df_t.year)
                & (self.df_i.month == self.df_t.month)
            )
            .select(
                "df_i.cicid",
                "df_i.year",
                "df_i.month",
                "df_i.travel_method",
                "df_i.origin_country",
                "df_i.terminus_state_id",
                col("df_d.name").alias("terminus_state_name"),
                col("df_t.avg_tmp").alias("terminus_temperature"),
                col("df_d.avg_household_size").alias("terminus_avg_household_size"),
                col("df_d.male_population").alias("terminus_male_population"),
                col("df_d.female_population").alias("terminus_female_population"),
                "df_i.visa_type",
                "df_i.visa_class",
            )
        )

        return self.df
