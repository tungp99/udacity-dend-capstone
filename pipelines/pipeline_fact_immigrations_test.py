"""This ETL Test Pipeline is for fact_immigrations"""
import logging

from pyspark.sql.functions import col

from udacity_capstone.pipelines import TestPipeline

logger = logging.getLogger(__name__)


class FactImmigrationsTestPipeline(TestPipeline):
    """This ETL Test Pipeline is for fact_immigrations"""

    def transform(self):
        """
        Test to make sure data was transformed to this model:
        |-- cicid: integer
        |-- year: integer (nullable = true)
        |-- month: integer (nullable = true)
        |-- travel_method: string (nullable = true)
        |-- origin_country: integer (nullable = true)
        |-- origin_port: string (nullable = true)
        |-- terminus_state_id: string (nullable = true)
        |-- migrant_yob: integer (nullable = true)
        |-- visa_type: string (nullable = true)
        |-- visa_class: string (nullable = true)
        """
        df = self.pipeline.df

        assert df.count() > 0, f"{self.pipeline.table} has no data"
        logger.info("check passed!")

        assert (
            df.where(col("cicid").isNull()).count() == 0
        ), f"{self.pipeline.table} has malformed cicid"
        logger.info("check passed!")

        assert (
            df.groupBy("cicid").count().where("count > 1").count() == 0
        ), f"{self.pipeline.table} has duplicated rows"
        logger.info("check passed!")
