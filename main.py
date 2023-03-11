from os import environ

from udacity_capstone.pipelines import get_spark
from udacity_capstone.pipelines.pipeline_dim_demographics import DimDemographicsPipeline
from udacity_capstone.pipelines.pipeline_dim_demographics_test import (
    DimDemographicsTestPipeline,
)
from udacity_capstone.pipelines.pipeline_dim_immigrations import DimImmigrationsPipeline
from udacity_capstone.pipelines.pipeline_dim_immigrations_test import (
    DimImmigrationsTestPipeline,
)
from udacity_capstone.pipelines.pipeline_dim_temperatures import DimTemperaturesPipeline
from udacity_capstone.pipelines.pipeline_dim_temperatures_test import (
    DimTemperaturesTestPipeline,
)
from udacity_capstone.pipelines.pipeline_fact_immigrations import (
    FactImmigrationsPipeline,
)
from udacity_capstone.pipelines.pipeline_fact_immigrations_test import (
    FactImmigrationsTestPipeline,
)


def main():
    spark = get_spark()

    # ETL Pipelines
    dim_temperatures_pipeline = DimTemperaturesPipeline(
        f"{environ['INPUT_DIR']}/GlobalLandTemperaturesByState.csv"
    )
    dim_temperatures_pipeline.run()

    dim_demographics_pipeline = DimDemographicsPipeline(
        f"{environ['INPUT_DIR']}/us-cities-demographics.csv"
    )
    dim_demographics_pipeline.run()

    dim_immigrations_pipeline = DimImmigrationsPipeline(
        f"{environ['INPUT_DIR']}/immigration_data_sample.csv"
    )
    dim_immigrations_pipeline.run()

    fact_immigrations_pipeline = FactImmigrationsPipeline()
    fact_immigrations_pipeline.run()

    # TL TestPipelines
    DimTemperaturesTestPipeline(dim_temperatures_pipeline).run()

    DimDemographicsTestPipeline(dim_demographics_pipeline).run()

    DimImmigrationsTestPipeline(dim_immigrations_pipeline).run()

    FactImmigrationsTestPipeline(fact_immigrations_pipeline).run()

    spark.stop()


if __name__ == "__main__":
    main()
