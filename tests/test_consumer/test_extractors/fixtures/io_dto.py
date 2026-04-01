import pytest

from data_rentgen.dto import (
    DatasetDTO,
    DatasetSymlinkDTO,
    DatasetSymlinkTypeDTO,
    LocationDTO,
    SchemaDTO,
    UserDTO,
)


@pytest.fixture
def extracted_postgres_location() -> LocationDTO:
    return LocationDTO(
        type="postgres",
        name="192.168.1.1:5432",
        addresses={"postgres://192.168.1.1:5432"},
    )


@pytest.fixture
def extracted_postgres_dataset(
    extracted_postgres_location: LocationDTO,
) -> DatasetDTO:
    return DatasetDTO(
        location=extracted_postgres_location,
        name="mydb.myschema.mytable",
    )


@pytest.fixture
def extracted_hdfs_location() -> LocationDTO:
    return LocationDTO(
        type="hdfs",
        name="test-hadoop:9820",
        addresses={"hdfs://test-hadoop:9820"},
    )


@pytest.fixture
def extracted_hdfs_dataset1(
    extracted_hdfs_location: LocationDTO,
) -> DatasetDTO:
    return DatasetDTO(
        location=extracted_hdfs_location,
        name="/user/hive/warehouse/mydb.db/mytable1",
    )


@pytest.fixture
def extracted_hdfs_dataset2(
    extracted_hdfs_location: LocationDTO,
) -> DatasetDTO:
    return DatasetDTO(
        location=extracted_hdfs_location,
        name="/user/hive/warehouse/mydb.db/mytable2",
    )


@pytest.fixture
def extracted_hive_metastore_location() -> LocationDTO:
    return LocationDTO(
        type="hive",
        name="test-hadoop:9083",
        addresses={"hive://test-hadoop:9083"},
    )


@pytest.fixture
def extracted_hive_dataset1(
    extracted_hive_metastore_location: LocationDTO,
) -> DatasetDTO:
    return DatasetDTO(
        location=extracted_hive_metastore_location,
        name="mydb.mytable1",
    )


@pytest.fixture
def extracted_hive_dataset2(
    extracted_hive_metastore_location: LocationDTO,
) -> DatasetDTO:
    return DatasetDTO(
        location=extracted_hive_metastore_location,
        name="mydb.mytable2",
    )


@pytest.fixture
def extracted_hdfs_dataset1_symlink(
    extracted_hdfs_dataset1: DatasetDTO,
    extracted_hive_dataset1: DatasetDTO,
) -> DatasetSymlinkDTO:
    return DatasetSymlinkDTO(
        from_dataset=extracted_hdfs_dataset1,
        to_dataset=extracted_hive_dataset1,
        type=DatasetSymlinkTypeDTO.METASTORE,
    )


@pytest.fixture
def extracted_hive_dataset1_symlink(
    extracted_hdfs_dataset1: DatasetDTO,
    extracted_hive_dataset1: DatasetDTO,
) -> DatasetSymlinkDTO:
    return DatasetSymlinkDTO(
        from_dataset=extracted_hive_dataset1,
        to_dataset=extracted_hdfs_dataset1,
        type=DatasetSymlinkTypeDTO.WAREHOUSE,
    )


@pytest.fixture
def extracted_hdfs_dataset2_symlink(
    extracted_hdfs_dataset2: DatasetDTO,
    extracted_hive_dataset2: DatasetDTO,
) -> DatasetSymlinkDTO:
    return DatasetSymlinkDTO(
        from_dataset=extracted_hdfs_dataset2,
        to_dataset=extracted_hive_dataset2,
        type=DatasetSymlinkTypeDTO.METASTORE,
    )


@pytest.fixture
def extracted_hive_dataset2_symlink(
    extracted_hdfs_dataset2: DatasetDTO,
    extracted_hive_dataset2: DatasetDTO,
) -> DatasetSymlinkDTO:
    return DatasetSymlinkDTO(
        from_dataset=extracted_hive_dataset2,
        to_dataset=extracted_hdfs_dataset2,
        type=DatasetSymlinkTypeDTO.WAREHOUSE,
    )


@pytest.fixture
def extracted_kafka_location() -> LocationDTO:
    return LocationDTO(
        type="kafka",
        name="server1:9092",
        addresses={"kafka://server1:9092", "kafka://server2:9092"},
    )


@pytest.fixture
def extracted_kafka_dataset(
    extracted_kafka_location: LocationDTO,
) -> DatasetDTO:
    return DatasetDTO(
        location=extracted_kafka_location,
        name="mytopic",
    )


@pytest.fixture
def extracted_dataset_schema() -> SchemaDTO:
    return SchemaDTO(
        fields=[
            {
                "name": "dt",
                "type": "timestamp",
                "description": "Business date",
            },
            {
                "name": "customer_id",
                "type": "decimal(20,0)",
            },
            {
                "name": "total_spent",
                "type": "float",
            },
            {
                "name": "phones",
                "type": "array",
                "fields": [
                    {
                        "name": "_element",
                        "type": "string",
                    },
                ],
            },
            {
                "name": "address",
                "type": "struct",
                "fields": [
                    {
                        "name": "street",
                        "type": "string",
                    },
                    {
                        "name": "city",
                        "type": "string",
                    },
                    {
                        "name": "state",
                        "type": "string",
                    },
                    {
                        "name": "zip",
                        "type": "string",
                    },
                ],
            },
        ],
    )


@pytest.fixture
def extracted_user() -> UserDTO:
    return UserDTO(name="myuser")


@pytest.fixture
def extracted_iceberg_metastore_location() -> LocationDTO:
    return LocationDTO(
        type="http",
        name="test-iceberg:8181",
        addresses={"http://test-iceberg:8181"},
    )


@pytest.fixture
def extracted_iceberg_dataset1(
    extracted_iceberg_metastore_location: LocationDTO,
) -> DatasetDTO:
    return DatasetDTO(
        location=extracted_iceberg_metastore_location,
        name="test_db.test_table",
    )


@pytest.fixture
def extracted_iceberg_dataset2(
    extracted_iceberg_metastore_location: LocationDTO,
) -> DatasetDTO:
    return DatasetDTO(
        location=extracted_iceberg_metastore_location,
        name="test_db.users_backup",
    )
