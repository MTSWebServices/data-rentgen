from datetime import datetime, timezone

import pytest
from uuid6 import UUID

from data_rentgen.openlineage.dataset import (
    OpenLineageInputDataset,
    OpenLineageOutputDataset,
)
from data_rentgen.openlineage.dataset_facets import (
    OpenLineageColumnLineageDatasetFacet,
    OpenLineageColumnLineageDatasetFacetField,
    OpenLineageColumnLineageDatasetFacetFieldRef,
    OpenLineageColumnLineageDatasetFacetFieldTransformation,
    OpenLineageDatasetFacets,
)
from data_rentgen.openlineage.job import OpenLineageJob
from data_rentgen.openlineage.job_facets import (
    OpenLineageJobFacets,
    OpenLineageJobProcessingType,
    OpenLineageJobTypeJobFacet,
)
from data_rentgen.openlineage.run import OpenLineageRun
from data_rentgen.openlineage.run_event import (
    OpenLineageRunEvent,
    OpenLineageRunEventType,
)
from data_rentgen.openlineage.run_facets import (
    OpenLineageParentJob,
    OpenLineageParentRun,
    OpenLineageParentRunFacet,
    OpenLineageRunFacets,
)


@pytest.fixture
def output_event_with_one_to_two_direct_column_lineage() -> OpenLineageOutputDataset:
    return OpenLineageOutputDataset(
        namespace="hdfs://test-hadoop:9820",
        name="/user/hive/warehouse/mydb.db/mytable1",
        facets=OpenLineageDatasetFacets(
            columnLineage=OpenLineageColumnLineageDatasetFacet(
                fields={
                    "column_1": OpenLineageColumnLineageDatasetFacetField(
                        inputFields=[
                            OpenLineageColumnLineageDatasetFacetFieldRef(
                                namespace="hive://test-hadoop:9083",
                                name="mydb.mytable1",
                                field="source_col_1",
                                transformations=[
                                    OpenLineageColumnLineageDatasetFacetFieldTransformation(
                                        type="DIRECT",
                                        subtype="AGGREGATION",
                                    ),
                                ],
                            ),
                            OpenLineageColumnLineageDatasetFacetFieldRef(
                                namespace="hive://test-hadoop:9083",
                                name="mydb.mytable1",
                                field="source_col_2",
                                transformations=[
                                    OpenLineageColumnLineageDatasetFacetFieldTransformation(
                                        type="DIRECT",
                                        subtype="TRANSFORMATION",
                                    ),
                                ],
                            ),
                        ],
                    ),
                },
            ),
        ),
    )


@pytest.fixture
def output_event_with_one_to_two_direct_and_indirect_column_lineage() -> OpenLineageOutputDataset:
    return OpenLineageOutputDataset(
        namespace="hdfs://test-hadoop:9820",
        name="/user/hive/warehouse/mydb.db/mytable1",
        facets=OpenLineageDatasetFacets(
            columnLineage=OpenLineageColumnLineageDatasetFacet(
                fields={
                    "column_1": OpenLineageColumnLineageDatasetFacetField(
                        inputFields=[
                            OpenLineageColumnLineageDatasetFacetFieldRef(
                                namespace="hive://test-hadoop:9083",
                                name="mydb.mytable1",
                                field="source_col_1",
                                transformations=[
                                    OpenLineageColumnLineageDatasetFacetFieldTransformation(
                                        type="DIRECT",
                                        subtype="AGGREGATION",
                                    ),
                                ],
                            ),
                            OpenLineageColumnLineageDatasetFacetFieldRef(
                                namespace="hive://test-hadoop:9083",
                                name="mydb.mytable1",
                                field="source_col_2",
                                transformations=[
                                    OpenLineageColumnLineageDatasetFacetFieldTransformation(
                                        type="DIRECT",
                                        subtype="TRANSFORMATION",
                                    ),
                                ],
                            ),
                        ],
                    ),
                },
                dataset=[
                    OpenLineageColumnLineageDatasetFacetFieldRef(
                        namespace="hive://test-hadoop:9083",
                        name="mydb.mytable1",
                        field="source_col_2",
                        transformations=[
                            OpenLineageColumnLineageDatasetFacetFieldTransformation(
                                type="INDIRECT",
                                subtype="JOIN",
                                masking=False,
                            ),
                            OpenLineageColumnLineageDatasetFacetFieldTransformation(
                                type="INDIRECT",
                                subtype="SORT",
                                masking=False,
                            ),
                        ],
                    ),
                ],
            ),
        ),
    )


@pytest.fixture
def output_event_with_direct_and_legacy_indirect_column_lineage() -> OpenLineageOutputDataset:
    # https://github.com/OpenLineage/OpenLineage/pull/3098
    return OpenLineageOutputDataset(
        namespace="hdfs://test-hadoop:9820",
        name="/user/hive/warehouse/mydb.db/mytable1",
        facets=OpenLineageDatasetFacets(
            columnLineage=OpenLineageColumnLineageDatasetFacet(
                fields={
                    "column_1": OpenLineageColumnLineageDatasetFacetField(
                        inputFields=[
                            OpenLineageColumnLineageDatasetFacetFieldRef(
                                namespace="hive://test-hadoop:9083",
                                name="mydb.mytable1",
                                field="source_col_1",
                                transformations=[
                                    OpenLineageColumnLineageDatasetFacetFieldTransformation(
                                        type="DIRECT",
                                        subtype="AGGREGATION",
                                    ),
                                ],
                            ),
                            OpenLineageColumnLineageDatasetFacetFieldRef(
                                namespace="hive://test-hadoop:9083",
                                name="mydb.mytable1",
                                field="source_col_2",
                                transformations=[
                                    OpenLineageColumnLineageDatasetFacetFieldTransformation(
                                        type="INDIRECT",
                                        subtype="JOIN",
                                    ),
                                ],
                            ),
                            OpenLineageColumnLineageDatasetFacetFieldRef(
                                namespace="hive://test-hadoop:9083",
                                name="mydb.mytable1",
                                field="source_col_4",
                                transformations=[
                                    OpenLineageColumnLineageDatasetFacetFieldTransformation(
                                        type="INDIRECT",
                                        subtype="WINDOW",
                                    ),
                                ],
                            ),
                        ],
                    ),
                    "column_2": OpenLineageColumnLineageDatasetFacetField(
                        inputFields=[
                            OpenLineageColumnLineageDatasetFacetFieldRef(
                                namespace="hive://test-hadoop:9083",
                                name="mydb.mytable1",
                                field="source_col_3",
                                transformations=[
                                    OpenLineageColumnLineageDatasetFacetFieldTransformation(
                                        type="DIRECT",
                                        subtype="AGGREGATION",
                                    ),
                                ],
                            ),
                            OpenLineageColumnLineageDatasetFacetFieldRef(
                                namespace="hive://test-hadoop:9083",
                                name="mydb.mytable1",
                                field="source_col_2",
                                transformations=[
                                    OpenLineageColumnLineageDatasetFacetFieldTransformation(
                                        type="INDIRECT",
                                        subtype="JOIN",
                                    ),
                                ],
                            ),
                            OpenLineageColumnLineageDatasetFacetFieldRef(
                                namespace="hive://test-hadoop:9083",
                                name="mydb.mytable1",
                                field="source_col_4",
                                transformations=[
                                    OpenLineageColumnLineageDatasetFacetFieldTransformation(
                                        type="INDIRECT",
                                        subtype="WINDOW",
                                    ),
                                ],
                            ),
                        ],
                    ),
                },
            ),
        ),
    )


@pytest.fixture
def output_event_with_column_lineage_without_transformations() -> OpenLineageOutputDataset:
    return OpenLineageOutputDataset(
        namespace="hdfs://test-hadoop:9820",
        name="/user/hive/warehouse/mydb.db/mytable1",
        facets=OpenLineageDatasetFacets(
            columnLineage=OpenLineageColumnLineageDatasetFacet(
                fields={
                    "column_1": OpenLineageColumnLineageDatasetFacetField(
                        inputFields=[
                            OpenLineageColumnLineageDatasetFacetFieldRef(
                                namespace="hive://test-hadoop:9083",
                                name="mydb.mytable1",
                                field="source_col_1",
                            ),
                        ],
                    ),
                    "column_2": OpenLineageColumnLineageDatasetFacetField(
                        inputFields=[
                            OpenLineageColumnLineageDatasetFacetFieldRef(
                                namespace="hive://test-hadoop:9083",
                                name="mydb.mytable1",
                                field="source_col_3",
                            ),
                        ],
                    ),
                },
                dataset=[
                    OpenLineageColumnLineageDatasetFacetFieldRef(
                        namespace="hive://test-hadoop:9083",
                        name="mydb.mytable1",
                        field="source_col_2",
                    ),
                    OpenLineageColumnLineageDatasetFacetFieldRef(
                        namespace="hive://test-hadoop:9083",
                        name="mydb.mytable1",
                        field="source_col_4",
                    ),
                ],
            ),
        ),
    )


@pytest.fixture
def output_event_with_legacy_column_lineage() -> OpenLineageOutputDataset:
    # https://github.com/OpenLineage/OpenLineage/pull/2756
    # https://github.com/OpenLineage/OpenLineage/issues/2186
    return OpenLineageOutputDataset(
        namespace="hdfs://test-hadoop:9820",
        name="/user/hive/warehouse/mydb.db/mytable1",
        facets=OpenLineageDatasetFacets(
            columnLineage=OpenLineageColumnLineageDatasetFacet(
                fields={
                    "column_1": OpenLineageColumnLineageDatasetFacetField(
                        inputFields=[
                            OpenLineageColumnLineageDatasetFacetFieldRef(
                                namespace="hive://test-hadoop:9083",
                                name="mydb.mytable1",
                                field="source_col_1",
                            ),
                        ],
                        transformationType="IDENTITY",
                        transformationDescription="some description",
                    ),
                    "column_2": OpenLineageColumnLineageDatasetFacetField(
                        inputFields=[
                            OpenLineageColumnLineageDatasetFacetFieldRef(
                                namespace="hive://test-hadoop:9083",
                                name="mydb.mytable1",
                                field="source_col_3",
                            ),
                        ],
                        transformationType="MASKED",
                        transformationDescription="another description",
                    ),
                    "column_3": OpenLineageColumnLineageDatasetFacetField(
                        inputFields=[
                            OpenLineageColumnLineageDatasetFacetFieldRef(
                                namespace="hive://test-hadoop:9083",
                                name="mydb.mytable1",
                                field="source_col_2",
                            ),
                            OpenLineageColumnLineageDatasetFacetFieldRef(
                                namespace="hive://test-hadoop:9083",
                                name="mydb.mytable1",
                                field="source_col_4",
                            ),
                        ],
                        transformationType="INDIRECT",
                        transformationDescription="",
                    ),
                },
            ),
        ),
    )


def get_run_event_with_column_lineage(
    operation_id: UUID,
    column_lineage_facet: OpenLineageColumnLineageDatasetFacet,
) -> OpenLineageRunEvent:
    """
    Function for generating run events. One event = one operation.
    Args is: operation_id and column lineage facet, which will be add to outputs.
    Input is always PG dataset, output - hive dataset.
    """
    event_time = datetime(2024, 7, 5, 9, 7, 15, 642000, tzinfo=timezone.utc)
    run_id = UUID("01908224-8410-79a2-8de6-a769ad6944c9")
    return OpenLineageRunEvent(
        eventType=OpenLineageRunEventType.RUNNING,
        eventTime=event_time,
        job=OpenLineageJob(
            namespace="local://some.host.com",
            name="mysession1.execute_some_command",
            facets=OpenLineageJobFacets(
                jobType=OpenLineageJobTypeJobFacet(
                    processingType=OpenLineageJobProcessingType.BATCH,
                    integration="SPARK",
                    jobType="SQL_JOB",
                ),
            ),
        ),
        run=OpenLineageRun(
            runId=operation_id,
            facets=OpenLineageRunFacets(
                parent=OpenLineageParentRunFacet(
                    job=OpenLineageParentJob(
                        namespace="local://some.host.com",
                        name="mysession1",
                    ),
                    run=OpenLineageParentRun(
                        runId=run_id,
                    ),
                ),
            ),
        ),
        inputs=[
            OpenLineageInputDataset(
                namespace="postgres://192.168.1.1:5432",
                name="mydb.myschema.mytable1",
                facets=OpenLineageDatasetFacets(),
            ),
        ],
        outputs=[
            OpenLineageOutputDataset(
                namespace="hive://test-hadoop:9083",
                name="mydb.mytable1",
                facets=OpenLineageDatasetFacets(columnLineage=column_lineage_facet),
            ),
        ],
    )
