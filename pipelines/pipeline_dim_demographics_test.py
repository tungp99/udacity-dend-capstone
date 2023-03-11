"""This ETL Test Pipeline is for dim_demographics"""
import logging

from pyspark.sql.functions import col

from udacity_capstone.pipelines import TestPipeline

logger = logging.getLogger(__name__)


class DimDemographicsTestPipeline(TestPipeline):
    """This ETL Test Pipeline is for dim_demographics"""

    def transform(self):
        """
        Test to make sure data was transformed to this model:
        |-- id: string (nullable = true)
        |-- name: string (nullable = true)
        |-- male_population: long (nullable = true)
        |-- female_population: long (nullable = true)
        |-- veterans_count: long (nullable = true)
        |-- foreigners_count: long (nullable = true)
        |-- avg_household_size: double (nullable = true)
        """
        df = self.pipeline.df

        assert df.count() > 0, f"{self.pipeline.table} has no data"
        logger.info("check passed!")

        assert (
            df.where(col("id").isNull()).count() == 0
        ), f"{self.pipeline.table} has malformed state id"
        logger.info("check passed!")

        assert (
            df.where(col("name").isNull()).count() == 0
        ), f"{self.pipeline.table} has malformed state name"
        logger.info("check passed!")

        assert (
            df.groupBy("id").count().where("count > 1").count() == 0
        ), f"{self.pipeline.table} has duplicated state id"
        logger.info("check passed!")

        assert (
            df.groupBy("name").count().where("count > 1").count() == 0
        ), f"{self.pipeline.table} has duplicated state name"
        logger.info("check passed!")
