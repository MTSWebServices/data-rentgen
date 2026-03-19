from data_rentgen.consumer.extractors.generic import GenericExtractor
from data_rentgen.consumer.extractors.impl import DbtExtractor, FlinkExtractor, SparkExtractor
from data_rentgen.dto import (
    DatasetDTO,
    DatasetSymlinkDTO,
    DatasetSymlinkTypeDTO,
    LocationDTO,
    TagDTO,
    TagValueDTO,
)
from data_rentgen.openlineage.dataset import (
    OpenLineageDataset,
)
from data_rentgen.openlineage.dataset_facets import (
    OpenLineageDatasetFacets,
    OpenLineageDatasetTagsFacet,
    OpenLineageDatasetTagsFacetField,
    OpenLineageSymlinkIdentifier,
    OpenLineageSymlinksDatasetFacet,
    OpenLineageSymlinkType,
)


def test_extractors_extract_dataset_hdfs():
    dataset = OpenLineageDataset(
        namespace="hdfs://test-hadoop:9820",
        name="/user/hive/warehouse/mydb.db/mytable",
    )

    dataset_dto, symlinks_dto = GenericExtractor().extract_dataset_and_symlinks(dataset)
    assert dataset_dto == DatasetDTO(
        location=LocationDTO(
            type="hdfs",
            name="test-hadoop:9820",
            addresses={"hdfs://test-hadoop:9820"},
        ),
        name="/user/hive/warehouse/mydb.db/mytable",
    )
    assert symlinks_dto == []


def test_extractors_extract_dataset_hdfs_with_patition():
    dataset = OpenLineageDataset(
        namespace="hdfs://test-hadoop:9820",
        name="/user/hive/warehouse/mydb.db/mytable/business_dt=2025-01-01/reg_id=99/part_dt=2025-01-01",
    )

    dataset_dto, symlinks_dto = SparkExtractor().extract_dataset_and_symlinks(dataset)
    assert dataset_dto == DatasetDTO(
        location=LocationDTO(
            type="hdfs",
            name="test-hadoop:9820",
            addresses={"hdfs://test-hadoop:9820"},
        ),
        name="/user/hive/warehouse/mydb.db/mytable",
    )
    assert symlinks_dto == []


def test_extractors_extract_dataset_hdfs_with_table_symlink():
    dataset = OpenLineageDataset(
        namespace="hdfs://test-hadoop:9820",
        name="/warehouse/mydb.db/mytable",
        facets=OpenLineageDatasetFacets(
            symlinks=OpenLineageSymlinksDatasetFacet(
                identifiers=[
                    OpenLineageSymlinkIdentifier(
                        namespace="hive://test-hadoop:9083",
                        name="mydb.mytable",
                        type=OpenLineageSymlinkType.TABLE,
                    ),
                ],
            ),
        ),
    )

    hdfs_dataset = DatasetDTO(
        location=LocationDTO(
            type="hdfs",
            name="test-hadoop:9820",
            addresses={"hdfs://test-hadoop:9820"},
        ),
        name="/warehouse/mydb.db/mytable",
    )

    hive_dataset = DatasetDTO(
        location=LocationDTO(
            type="hive",
            name="test-hadoop:9083",
            addresses={"hive://test-hadoop:9083"},
        ),
        name="mydb.mytable",
    )

    dataset_dto, symlinks_dto = SparkExtractor().extract_dataset_and_symlinks(dataset)
    assert dataset_dto == hive_dataset
    assert symlinks_dto == [
        DatasetSymlinkDTO(from_dataset=hdfs_dataset, to_dataset=hive_dataset, type=DatasetSymlinkTypeDTO.METASTORE),
        DatasetSymlinkDTO(from_dataset=hive_dataset, to_dataset=hdfs_dataset, type=DatasetSymlinkTypeDTO.WAREHOUSE),
    ]


def test_extractors_extract_dataset_s3():
    dataset = OpenLineageDataset(
        namespace="s3://bucket",
        name="warehouse/mydb.db/mytable",
    )

    dataset_dto, symlinks_dto = GenericExtractor().extract_dataset_and_symlinks(dataset)
    assert dataset_dto == DatasetDTO(
        location=LocationDTO(
            type="s3",
            name="bucket",
            addresses={"s3://bucket"},
        ),
        name="warehouse/mydb.db/mytable",
    )
    assert symlinks_dto == []


def test_extractors_extract_dataset_file():
    dataset = OpenLineageDataset(
        namespace="file",
        name="/warehouse/mydb.db/mytable",
    )

    dataset_dto, symlinks_dto = GenericExtractor().extract_dataset_and_symlinks(dataset)
    assert dataset_dto == DatasetDTO(
        location=LocationDTO(
            type="file",
            name="unknown",
            addresses={"file://unknown"},
        ),
        name="/warehouse/mydb.db/mytable",
    )
    assert symlinks_dto == []


def test_extractors_extract_dataset_hive():
    dataset = OpenLineageDataset(
        namespace="hive://test-hadoop:9083",
        name="mydb.mytable",
    )

    dataset_dto, symlinks_dto = GenericExtractor().extract_dataset_and_symlinks(dataset)
    assert dataset_dto == DatasetDTO(
        location=LocationDTO(
            type="hive",
            name="test-hadoop:9083",
            addresses={"hive://test-hadoop:9083"},
        ),
        name="mydb.mytable",
    )
    assert symlinks_dto == []


def test_extractors_extract_dataset_hive_with_location_symlink():
    # Not accepted yet, see https://github.com/OpenLineage/OpenLineage/issues/2718
    dataset = OpenLineageDataset(
        namespace="hive://test-hadoop:9083",
        name="mydb.mytable",
        facets=OpenLineageDatasetFacets(
            symlinks=OpenLineageSymlinksDatasetFacet(
                identifiers=[
                    OpenLineageSymlinkIdentifier(
                        namespace="hdfs://test-hadoop:9820",
                        name="/warehouse/mydb.db/mytable",
                        type=OpenLineageSymlinkType.LOCATION,
                    ),
                ],
            ),
        ),
    )

    hdfs_dataset = DatasetDTO(
        location=LocationDTO(
            type="hdfs",
            name="test-hadoop:9820",
            addresses={"hdfs://test-hadoop:9820"},
        ),
        name="/warehouse/mydb.db/mytable",
    )
    hive_dataset = DatasetDTO(
        location=LocationDTO(
            type="hive",
            name="test-hadoop:9083",
            addresses={"hive://test-hadoop:9083"},
        ),
        name="mydb.mytable",
    )

    dataset_dto, symlinks_dto = SparkExtractor().extract_dataset_and_symlinks(dataset)
    assert dataset_dto == hive_dataset
    assert symlinks_dto == [
        DatasetSymlinkDTO(from_dataset=hdfs_dataset, to_dataset=hive_dataset, type=DatasetSymlinkTypeDTO.METASTORE),
        DatasetSymlinkDTO(from_dataset=hive_dataset, to_dataset=hdfs_dataset, type=DatasetSymlinkTypeDTO.WAREHOUSE),
    ]


def test_extractors_extract_dataset_postgres():
    dataset = OpenLineageDataset(
        namespace="postgres://192.168.1.1:5432",
        name="mydb.myschema.mytable",
    )

    dataset_dto, symlinks_dto = GenericExtractor().extract_dataset_and_symlinks(dataset)
    assert dataset_dto == DatasetDTO(
        location=LocationDTO(
            type="postgres",
            name="192.168.1.1:5432",
            addresses={"postgres://192.168.1.1:5432"},
        ),
        name="mydb.myschema.mytable",
    )
    assert symlinks_dto == []


def test_extractors_extract_dataset_kafka():
    dataset = OpenLineageDataset(
        namespace="kafka://192.168.1.1:9092,192.168.1.2:9092",
        name="mytopic",
    )

    dataset_dto, symlinks_dto = GenericExtractor().extract_dataset_and_symlinks(dataset)
    assert dataset_dto == DatasetDTO(
        location=LocationDTO(
            type="kafka",
            name="192.168.1.1:9092",
            addresses={"kafka://192.168.1.1:9092", "kafka://192.168.1.2:9092"},
        ),
        name="mytopic",
    )
    assert symlinks_dto == []


def test_extractors_extract_dataset_kafka_with_flink2_legacy_symlinks():
    # https://github.com/OpenLineage/OpenLineage/pull/3657
    dataset = OpenLineageDataset(
        namespace="kafka://192.168.1.1:9092",
        name="mytopic",
        facets=OpenLineageDatasetFacets(
            symlinks=OpenLineageSymlinksDatasetFacet(
                identifiers=[
                    OpenLineageSymlinkIdentifier(
                        namespace="kafka://192.168.1.1:9092",
                        name="default_catalog.default_database.sometable",
                        type=OpenLineageSymlinkType.TABLE,
                    ),
                ],
            ),
        ),
    )

    dataset_dto, symlinks_dto = FlinkExtractor().extract_dataset_and_symlinks(dataset)
    assert dataset_dto == DatasetDTO(
        location=LocationDTO(
            type="kafka",
            name="192.168.1.1:9092",
            addresses={"kafka://192.168.1.1:9092"},
        ),
        name="mytopic",
    )
    assert symlinks_dto == []


def test_extractors_extract_dataset_dbt_none():
    dataset = OpenLineageDataset(
        namespace="some-namespace",
        name="None.some.name",
    )

    dataset_dto, symlinks_dto = DbtExtractor().extract_dataset_and_symlinks(dataset)
    assert dataset_dto == DatasetDTO(
        location=LocationDTO(
            type="unknown",
            name="some-namespace",
            addresses={"unknown://some-namespace"},
        ),
        name="some.name",
    )
    assert symlinks_dto == []


def test_extractors_extract_dataset_unknown():
    dataset = OpenLineageDataset(
        namespace="some-namespace",
        name="some.name",
    )

    dataset_dto, symlinks_dto = GenericExtractor().extract_dataset_and_symlinks(dataset)
    assert dataset_dto == DatasetDTO(
        location=LocationDTO(
            type="unknown",
            name="some-namespace",
            addresses={"unknown://some-namespace"},
        ),
        name="some.name",
    )
    assert symlinks_dto == []


def test_extractors_extract_dataset_with_tags():
    dataset = OpenLineageDataset(
        namespace="postgres://192.168.1.1:5432",
        name="mydb.myschema.mytable",
        facets=OpenLineageDatasetFacets(
            tags=OpenLineageDatasetTagsFacet(
                tags=[
                    OpenLineageDatasetTagsFacetField(key="somekey", value="somevalue"),
                    OpenLineageDatasetTagsFacetField(key="somekey", value="othervalue", source="OTHER"),
                    OpenLineageDatasetTagsFacetField(key="anotherkey", value="anothervalue", source="ABC", field="abc"),
                ],
            ),
        ),
    )

    dataset_dto, symlinks_dto = GenericExtractor().extract_dataset_and_symlinks(dataset)
    assert dataset_dto == DatasetDTO(
        location=LocationDTO(
            type="postgres",
            name="192.168.1.1:5432",
            addresses={"postgres://192.168.1.1:5432"},
        ),
        name="mydb.myschema.mytable",
        tag_values={
            TagValueDTO(tag=TagDTO(name="somekey"), value="somevalue"),
            TagValueDTO(tag=TagDTO(name="somekey"), value="othervalue"),
            TagValueDTO(tag=TagDTO(name="anotherkey"), value="anothervalue"),
        },
    )
    assert symlinks_dto == []
