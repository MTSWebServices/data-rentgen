import pytest

from data_rentgen.openlineage.dataset import OpenLineageInputDataset, OpenLineageOutputDataset
from data_rentgen.openlineage.dataset_facets import OpenLineageDatasetFacets, OpenLineageOutputDatasetFacets
from data_rentgen.openlineage.dataset_facets.lifecycle_change import (
    OpenLineageDatasetLifecycleStateChange,
    OpenLineageLifecycleStateChangeDatasetFacet,
)
from data_rentgen.openlineage.dataset_facets.output_statistics import (
    OpenLineageOutputStatisticsOutputDatasetFacet,
)
from data_rentgen.openlineage.dataset_facets.schema import (
    OpenLineageSchemaDatasetFacet,
    OpenLineageSchemaField,
)
from data_rentgen.openlineage.dataset_facets.symlinks import (
    OpenLineageSymlinkIdentifier,
    OpenLineageSymlinksDatasetFacet,
    OpenLineageSymlinkType,
)


@pytest.fixture
def postgres_input() -> OpenLineageInputDataset:
    return OpenLineageInputDataset(
        namespace="postgres://192.168.1.1:5432",
        name="mydb.myschema.mytable",
        facets=OpenLineageDatasetFacets(
            schema=OpenLineageSchemaDatasetFacet(
                fields=[
                    OpenLineageSchemaField(
                        name="dt",
                        type="timestamp",
                        description="Business date",
                    ),
                    OpenLineageSchemaField(
                        name="customer_id",
                        type="decimal(20,0)",
                    ),
                    OpenLineageSchemaField(name="total_spent", type="float"),
                    OpenLineageSchemaField(
                        name="phones",
                        type="array",
                        fields=[
                            OpenLineageSchemaField(
                                name="_element",
                                type="string",
                            ),
                        ],
                    ),
                    OpenLineageSchemaField(
                        name="address",
                        type="struct",
                        fields=[
                            OpenLineageSchemaField(
                                name="street",
                                type="string",
                            ),
                            OpenLineageSchemaField(name="city", type="string"),
                            OpenLineageSchemaField(name="state", type="string"),
                            OpenLineageSchemaField(name="zip", type="string"),
                        ],
                    ),
                ],
            ),
        ),
    )


@pytest.fixture
def hive_input() -> OpenLineageInputDataset:
    return OpenLineageInputDataset(
        namespace="hive://test-hadoop:9083",
        name="mydb.mytable1",
        facets=OpenLineageDatasetFacets(
            symlinks=OpenLineageSymlinksDatasetFacet(
                identifiers=[
                    OpenLineageSymlinkIdentifier(
                        namespace="hdfs://test-hadoop:9820",
                        name="/user/hive/warehouse/mydb.db/mytable1",
                        type=OpenLineageSymlinkType.LOCATION,
                    ),
                ],
            ),
            schema=OpenLineageSchemaDatasetFacet(
                fields=[
                    OpenLineageSchemaField(
                        name="dt",
                        type="timestamp",
                        description="Business date",
                    ),
                    OpenLineageSchemaField(
                        name="customer_id",
                        type="decimal(20,0)",
                    ),
                    OpenLineageSchemaField(name="total_spent", type="float"),
                    OpenLineageSchemaField(
                        name="phones",
                        type="array",
                        fields=[
                            OpenLineageSchemaField(
                                name="_element",
                                type="string",
                            ),
                        ],
                    ),
                    OpenLineageSchemaField(
                        name="address",
                        type="struct",
                        fields=[
                            OpenLineageSchemaField(
                                name="street",
                                type="string",
                            ),
                            OpenLineageSchemaField(name="city", type="string"),
                            OpenLineageSchemaField(name="state", type="string"),
                            OpenLineageSchemaField(name="zip", type="string"),
                        ],
                    ),
                ],
            ),
        ),
    )


@pytest.fixture
def hive_output() -> OpenLineageOutputDataset:
    return OpenLineageOutputDataset(
        namespace="hive://test-hadoop:9083",
        name="mydb.mytable2",
        facets=OpenLineageDatasetFacets(
            symlinks=OpenLineageSymlinksDatasetFacet(
                identifiers=[
                    OpenLineageSymlinkIdentifier(
                        namespace="hdfs://test-hadoop:9820",
                        name="/user/hive/warehouse/mydb.db/mytable2",
                        type=OpenLineageSymlinkType.LOCATION,
                    ),
                ],
            ),
            schema=OpenLineageSchemaDatasetFacet(
                fields=[
                    OpenLineageSchemaField(
                        name="dt",
                        type="timestamp",
                        description="Business date",
                    ),
                    OpenLineageSchemaField(
                        name="customer_id",
                        type="decimal(20,0)",
                    ),
                    OpenLineageSchemaField(name="total_spent", type="float"),
                    OpenLineageSchemaField(
                        name="phones",
                        type="array",
                        fields=[
                            OpenLineageSchemaField(
                                name="_element",
                                type="string",
                            ),
                        ],
                    ),
                    OpenLineageSchemaField(
                        name="address",
                        type="struct",
                        fields=[
                            OpenLineageSchemaField(
                                name="street",
                                type="string",
                            ),
                            OpenLineageSchemaField(name="city", type="string"),
                            OpenLineageSchemaField(name="state", type="string"),
                            OpenLineageSchemaField(name="zip", type="string"),
                        ],
                    ),
                ],
            ),
        ),
    )


@pytest.fixture
def hdfs_output() -> OpenLineageOutputDataset:
    return OpenLineageOutputDataset(
        namespace="hdfs://test-hadoop:9820",
        name="/user/hive/warehouse/mydb.db/mytable1",
        facets=OpenLineageDatasetFacets(
            lifecycleStateChange=OpenLineageLifecycleStateChangeDatasetFacet(
                lifecycleStateChange=OpenLineageDatasetLifecycleStateChange.CREATE,
            ),
            symlinks=OpenLineageSymlinksDatasetFacet(
                identifiers=[
                    OpenLineageSymlinkIdentifier(
                        namespace="hive://test-hadoop:9083",
                        name="mydb.mytable1",
                        type=OpenLineageSymlinkType.TABLE,
                    ),
                ],
            ),
            schema=OpenLineageSchemaDatasetFacet(
                fields=[
                    OpenLineageSchemaField(
                        name="dt",
                        type="timestamp",
                        description="Business date",
                    ),
                    OpenLineageSchemaField(
                        name="customer_id",
                        type="decimal(20,0)",
                    ),
                    OpenLineageSchemaField(name="total_spent", type="float"),
                    OpenLineageSchemaField(
                        name="phones",
                        type="array",
                        fields=[
                            OpenLineageSchemaField(
                                name="_element",
                                type="string",
                            ),
                        ],
                    ),
                    OpenLineageSchemaField(
                        name="address",
                        type="struct",
                        fields=[
                            OpenLineageSchemaField(
                                name="street",
                                type="string",
                            ),
                            OpenLineageSchemaField(name="city", type="string"),
                            OpenLineageSchemaField(name="state", type="string"),
                            OpenLineageSchemaField(name="zip", type="string"),
                        ],
                    ),
                ],
            ),
        ),
    )


@pytest.fixture
def hdfs_output_with_stats(
    hdfs_output: OpenLineageOutputDataset,
) -> OpenLineageOutputDataset:
    return hdfs_output.model_copy(
        update={
            "outputFacets": OpenLineageOutputDatasetFacets(
                outputStatistics=OpenLineageOutputStatisticsOutputDatasetFacet(
                    rowCount=1_000_000,
                    size=1000 * 1024 * 1024,
                ),
            ),
        },
    )


@pytest.fixture
def kafka_output() -> OpenLineageOutputDataset:
    return OpenLineageOutputDataset(
        namespace="kafka://server1:9092,server2:9092,",
        name="mytopic",
        facets=OpenLineageDatasetFacets(
            schema=OpenLineageSchemaDatasetFacet(
                fields=[
                    OpenLineageSchemaField(
                        name="dt",
                        type="timestamp",
                        description="Business date",
                    ),
                    OpenLineageSchemaField(
                        name="customer_id",
                        type="decimal(20,0)",
                    ),
                    OpenLineageSchemaField(name="total_spent", type="float"),
                    OpenLineageSchemaField(
                        name="phones",
                        type="array",
                        fields=[
                            OpenLineageSchemaField(
                                name="_element",
                                type="string",
                            ),
                        ],
                    ),
                    OpenLineageSchemaField(
                        name="address",
                        type="struct",
                        fields=[
                            OpenLineageSchemaField(
                                name="street",
                                type="string",
                            ),
                            OpenLineageSchemaField(name="city", type="string"),
                            OpenLineageSchemaField(name="state", type="string"),
                            OpenLineageSchemaField(name="zip", type="string"),
                        ],
                    ),
                ],
            ),
        ),
    )


@pytest.fixture
def kafka_output_with_stats(
    kafka_output: OpenLineageOutputDataset,
) -> OpenLineageOutputDataset:
    return kafka_output.model_copy(
        update={
            "outputFacets": OpenLineageOutputDatasetFacets(
                outputStatistics=OpenLineageOutputStatisticsOutputDatasetFacet(
                    rowCount=1_000_000,
                    size=1000 * 1024 * 1024,
                ),
            ),
        },
    )


@pytest.fixture
def iceberg_input() -> OpenLineageInputDataset:
    return OpenLineageInputDataset(
        namespace="http://test-iceberg:8181",
        name="test_db.test_table",
    )


@pytest.fixture
def iceberg_output() -> OpenLineageOutputDataset:
    return OpenLineageOutputDataset(
        namespace="http://test-iceberg:8181",
        name="test_db.users_backup",
    )
