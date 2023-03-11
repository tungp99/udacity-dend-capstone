"""This ETL Test Pipeline is for dim_temperatures"""
import logging

from pyspark.sql.functions import col

from udacity_capstone.pipelines import TestPipeline

logger = logging.getLogger(__name__)


class DimTemperaturesTestPipeline(TestPipeline):
    """This ETL Test Pipeline is for dim_temperatures"""

    def transform(self):
        """
        Test to make sure data was transformed to this model:
        |-- state_id: string (nullable = true)
        |-- year: integer (nullable = true)
        |-- month: integer (nullable = true)
        |-- avg_tmp: double (nullable = true)
        """
        df = self.pipeline.df

        assert df.count() > 0, f"{self.pipeline.table} has no data"
        logger.info("check passed!")

        assert (
            df.where(col("state_id").isNull()).count() == 0
        ), f"{self.pipeline.table} has malformed state_id"
        logger.info("check passed!")

        assert (
            df.groupBy("state_id", "year", "month").count().where("count > 1").count()
            == 0
        ), f"{self.pipeline.table} has duplicated statistic"
        logger.info("check passed!")
