# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from data_rentgen.openlineage.base import OpenLineageBase
from data_rentgen.openlineage.run_facets.airflow import (
    OpenLineageAirflowDagInfo,
    OpenLineageAirflowDagRunFacet,
    OpenLineageAirflowDagRunInfo,
    OpenLineageAirflowDagRunType,
    OpenLineageAirflowTaskGroupInfo,
    OpenLineageAirflowTaskInfo,
    OpenLineageAirflowTaskInstanceInfo,
    OpenLineageAirflowTaskRunFacet,
)
from data_rentgen.openlineage.run_facets.base import OpenLineageRunFacet
from data_rentgen.openlineage.run_facets.data_rentgen_operation import DataRentgenOperationInfoFacet
from data_rentgen.openlineage.run_facets.data_rentgen_run import DataRentgenRunInfoFacet
from data_rentgen.openlineage.run_facets.dbt_run import OpenLineageDbtRunRunFacet
from data_rentgen.openlineage.run_facets.flink_job import (
    OpenLineageFlinkJobDetailsRunFacet,
)
from data_rentgen.openlineage.run_facets.hive_query import OpenLineageHiveQueryInfoRunFacet
from data_rentgen.openlineage.run_facets.hive_session import OpenLineageHiveSessionInfoRunFacet
from data_rentgen.openlineage.run_facets.job_dependency import (
    OpenLineageJobDependenciesRunFacet,
    OpenLineageJobDependency,
    OpenLineageJobIdentifier,
    OpenLineageRunIdentifier,
)
from data_rentgen.openlineage.run_facets.nominal_time import OpenLineageNominalTimeRunFacet
from data_rentgen.openlineage.run_facets.parent_run import (
    OpenLineageParentJob,
    OpenLineageParentRun,
    OpenLineageParentRunFacet,
)
from data_rentgen.openlineage.run_facets.processing_engine import (
    OpenLineageProcessingEngineRunFacet,
)
from data_rentgen.openlineage.run_facets.run_tags import OpenLineageRunTagsFacet, OpenLineageRunTagsFacetField
from data_rentgen.openlineage.run_facets.spark_application import (
    OpenLineageSparkApplicationDetailsRunFacet,
    OpenLineageSparkDeployMode,
)
from data_rentgen.openlineage.run_facets.spark_job import (
    OpenLineageSparkJobDetailsRunFacet,
)
from data_rentgen.openlineage.run_facets.starrocks_session import OpenLineageStarRocksSessionInfoRunFacet

__all__ = [
    "DataRentgenOperationInfoFacet",
    "DataRentgenRunInfoFacet",
    "OpenLineageAirflowDagInfo",
    "OpenLineageAirflowDagRunFacet",
    "OpenLineageAirflowDagRunInfo",
    "OpenLineageAirflowDagRunType",
    "OpenLineageAirflowTaskGroupInfo",
    "OpenLineageAirflowTaskInfo",
    "OpenLineageAirflowTaskInstanceInfo",
    "OpenLineageAirflowTaskRunFacet",
    "OpenLineageDbtRunRunFacet",
    "OpenLineageFlinkJobDetailsRunFacet",
    "OpenLineageJobDependenciesRunFacet",
    "OpenLineageJobDependency",
    "OpenLineageJobIdentifier",
    "OpenLineageNominalTimeRunFacet",
    "OpenLineageParentJob",
    "OpenLineageParentRun",
    "OpenLineageParentRunFacet",
    "OpenLineageProcessingEngineRunFacet",
    "OpenLineageRunFacet",
    "OpenLineageRunFacets",
    "OpenLineageRunIdentifier",
    "OpenLineageRunTagsFacet",
    "OpenLineageRunTagsFacetField",
    "OpenLineageSparkApplicationDetailsRunFacet",
    "OpenLineageSparkDeployMode",
    "OpenLineageSparkJobDetailsRunFacet",
    "OpenLineageStarRocksSessionInfoRunFacet",
]


class OpenLineageRunFacets(OpenLineageBase):
    """All possible run facets.
    See [Run](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json).
    """

    parent: OpenLineageParentRunFacet | None = None
    processing_engine: OpenLineageProcessingEngineRunFacet | None = None
    tags: OpenLineageRunTagsFacet | None = None
    nominalTime: OpenLineageNominalTimeRunFacet | None = None
    jobDependencies: OpenLineageJobDependenciesRunFacet | None = None
    dataRentgen_run: DataRentgenRunInfoFacet | None = None
    dataRentgen_operation: DataRentgenOperationInfoFacet | None = None
    spark_applicationDetails: OpenLineageSparkApplicationDetailsRunFacet | None = None
    spark_jobDetails: OpenLineageSparkJobDetailsRunFacet | None = None
    airflow: OpenLineageAirflowTaskRunFacet | None = None
    airflowDagRun: OpenLineageAirflowDagRunFacet | None = None
    dbt_run: OpenLineageDbtRunRunFacet | None = None
    flink_job: OpenLineageFlinkJobDetailsRunFacet | None = None
    hive_query: OpenLineageHiveQueryInfoRunFacet | None = None
    hive_session: OpenLineageHiveSessionInfoRunFacet | None = None
    starrocks_session: OpenLineageStarRocksSessionInfoRunFacet | None = None
