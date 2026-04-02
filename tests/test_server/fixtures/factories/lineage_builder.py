from __future__ import annotations

from typing import TYPE_CHECKING

from data_rentgen.db.models import (
    Address,
    ColumnLineage,
    Dataset,
    DatasetColumnRelation,
    DatasetSymlink,
    DatasetSymlinkType,
    Input,
    Job,
    JobDependency,
    JobType,
    Location,
    Operation,
    Output,
    OutputType,
    Run,
    Schema,
    Tag,
    User,
)
from tests.test_server.fixtures.factories.address import create_address as create_address_model
from tests.test_server.fixtures.factories.dataset import create_dataset as create_dataset_model
from tests.test_server.fixtures.factories.dataset import make_symlink as make_dataset_symlink_model
from tests.test_server.fixtures.factories.input import create_input as create_input_model
from tests.test_server.fixtures.factories.job import create_job as create_job_model
from tests.test_server.fixtures.factories.job_type import create_job_type as create_job_type_model
from tests.test_server.fixtures.factories.location import create_location as create_location_model
from tests.test_server.fixtures.factories.operation import create_operation as create_operation_model
from tests.test_server.fixtures.factories.output import create_output as create_output_model
from tests.test_server.fixtures.factories.personal_token import create_personal_token as create_personal_token_model
from tests.test_server.fixtures.factories.relations import create_column_lineage as create_column_lineage_model
from tests.test_server.fixtures.factories.relations import create_column_relation as create_column_relation_model
from tests.test_server.fixtures.factories.run import create_run as create_run_model
from tests.test_server.fixtures.factories.schema import create_schema as create_schema_model
from tests.test_server.fixtures.factories.sql_query import create_sql_query as create_sql_query_model
from tests.test_server.fixtures.factories.tag import create_tag as create_tag_model
from tests.test_server.fixtures.factories.tag import create_tag_value as create_tag_value_model
from tests.test_server.fixtures.factories.user import create_user as create_user_model
from tests.test_server.utils.lineage_result import LineageResult

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession
    from uuid6 import UUID

    from data_rentgen.db.models.personal_token import PersonalToken
    from data_rentgen.db.models.sql_query import SQLQuery
    from data_rentgen.db.models.tag_value import TagValue


class LineageBuilder:
    def __init__(self, async_session: AsyncSession) -> None:
        self.async_session = async_session
        self.lineage_result = LineageResult()

        # Entity registries by user-defined keys for deterministic reuse in tests.
        self.addresses: dict[str, Address] = {}
        self.locations: dict[str, Location] = {}
        self.schemas: dict[str, Schema] = {}
        self.tags: dict[str, Tag] = {}
        self.tag_values: dict[str, TagValue] = {}
        self.datasets: dict[str, Dataset] = {}
        self.job_types: dict[str, JobType] = {}
        self.jobs: dict[str, Job] = {}
        self.users: dict[str, User] = {}
        self.runs: dict[str, Run] = {}
        self.sql_queries: dict[str, SQLQuery] = {}
        self.operations: dict[str, Operation] = {}
        self.inputs: dict[str, Input] = {}
        self.outputs: dict[str, Output] = {}
        self.personal_tokens: dict[str, PersonalToken] = {}

        # Relation registries.
        self.dataset_symlinks: dict[str, DatasetSymlink] = {}
        self.job_dependencies: dict[str, JobDependency] = {}
        self.column_lineage: dict[str, ColumnLineage] = {}
        self.column_relations: dict[str, DatasetColumnRelation] = {}

        # Optional reverse index useful for debugging and ad-hoc assertions.
        self.by_id: dict[str, object] = {}

    async def create_location(
        self,
        key: str,
        location_kwargs: dict | None = None,
        address_kwargs: dict | None = None,
    ) -> Location:
        if key in self.locations:
            return self.locations[key]

        location = await create_location_model(
            async_session=self.async_session,
            location_kwargs=location_kwargs,
            address_kwargs=address_kwargs,
        )
        self.locations[key] = location
        self.by_id[str(location.id)] = location
        return location

    async def create_address(
        self,
        key: str,
        location: Location,
        address_kwargs: dict | None = None,
    ) -> Address:
        if key in self.addresses:
            return self.addresses[key]

        payload = dict(address_kwargs or {})
        address = await create_address_model(
            async_session=self.async_session,
            location_id=location.id,
            address_kwargs=payload,
        )
        self.addresses[key] = address
        self.by_id[str(address.id)] = address
        return address

    async def create_schema(
        self,
        key: str,
        schema_kwargs: dict | None = None,
    ) -> Schema:
        if key in self.schemas:
            return self.schemas[key]

        payload = dict(schema_kwargs or {})
        schema = await create_schema_model(
            async_session=self.async_session,
            schema_kwargs=payload,
        )
        self.schemas[key] = schema
        self.by_id[str(schema.id)] = schema
        return schema

    async def create_tag(
        self,
        key: str,
        tag_kwargs: dict | None = None,
    ) -> Tag:
        if key in self.tags:
            return self.tags[key]

        payload = dict(tag_kwargs or {})
        tag = await create_tag_model(
            async_session=self.async_session,
            tag_kwargs=payload,
        )
        self.tags[key] = tag
        self.by_id[str(tag.id)] = tag
        return tag

    async def create_tag_value(
        self,
        key: str,
        tag: Tag,
        tag_value_kwargs: dict | None = None,
    ) -> TagValue:
        if key in self.tag_values:
            return self.tag_values[key]

        payload = dict(tag_value_kwargs or {})
        tag_value = await create_tag_value_model(
            async_session=self.async_session,
            tag_id=tag.id,
            tag_value_kwargs=payload,
        )
        self.tag_values[key] = tag_value
        self.by_id[str(tag_value.id)] = tag_value
        return tag_value

    async def create_dataset(
        self,
        key: str,
        location: Location,
        dataset_kwargs: dict | None = None,
        tag_values: set[TagValue] | None = None,
    ) -> Dataset:
        if key in self.datasets:
            return self.datasets[key]

        payload = dict(dataset_kwargs or {})
        dataset = await create_dataset_model(
            async_session=self.async_session,
            location_id=location.id,
            dataset_kwargs=payload,
            tag_values=tag_values,
        )
        self.datasets[key] = dataset
        self.by_id[str(dataset.id)] = dataset
        return dataset

    async def create_dataset_symlink(
        self,
        key: str,
        from_dataset: Dataset,
        to_dataset: Dataset,
        type: DatasetSymlinkType,
    ) -> DatasetSymlink:
        if key in self.dataset_symlinks:
            return self.dataset_symlinks[key]

        symlink = await make_dataset_symlink_model(
            async_session=self.async_session,
            from_dataset=from_dataset,
            to_dataset=to_dataset,
            type=type,
        )
        self.dataset_symlinks[key] = symlink
        self.by_id[str(symlink.id)] = symlink
        return symlink

    async def create_job_type(
        self,
        key: str,
        job_type_kwargs: dict | None = None,
    ) -> JobType:
        if key in self.job_types:
            return self.job_types[key]

        payload = dict(job_type_kwargs or {})
        job_type = await create_job_type_model(
            async_session=self.async_session,
            job_type_kwargs=payload,
        )
        self.job_types[key] = job_type
        self.by_id[str(job_type.id)] = job_type
        return job_type

    async def create_job(
        self,
        key: str,
        location: Location,
        job_type: JobType,
        job_kwargs: dict | None = None,
        tag_values: set[TagValue] | None = None,
    ) -> Job:
        if key in self.jobs:
            return self.jobs[key]

        payload = dict(job_kwargs or {})
        job = await create_job_model(
            async_session=self.async_session,
            location_id=location.id,
            job_type_id=job_type.id,
            job_kwargs=payload,
            tag_values=tag_values,
        )
        self.jobs[key] = job
        self.by_id[str(job.id)] = job
        return job

    async def create_job_dependency(
        self,
        key: str,
        from_job: Job,
        to_job: Job,
        dependency_type: str = "DIRECT_DEPENDENCY",
    ) -> JobDependency:
        if key in self.job_dependencies:
            return self.job_dependencies[key]

        dependency = JobDependency(
            from_job_id=from_job.id,
            to_job_id=to_job.id,
            type=dependency_type,
        )
        self.async_session.add(dependency)
        await self.async_session.commit()
        await self.async_session.refresh(dependency)

        self.job_dependencies[key] = dependency
        # JobDependency doesn't expose a stable single-column id in this model.
        self.by_id[key] = dependency
        return dependency

    async def create_user(
        self,
        key: str,
        user_kwargs: dict | None = None,
    ) -> User:
        if key in self.users:
            return self.users[key]

        payload = dict(user_kwargs or {})
        user = await create_user_model(
            async_session=self.async_session,
            user_kwargs=payload,
        )
        self.users[key] = user
        self.by_id[str(user.id)] = user
        return user

    async def create_run(
        self,
        key: str,
        job: Job,
        run_kwargs: dict | None = None,
    ) -> Run:
        if key in self.runs:
            return self.runs[key]

        payload = dict(run_kwargs or {})
        payload["job_id"] = job.id
        run = await create_run_model(
            async_session=self.async_session,
            run_kwargs=payload,
        )
        self.runs[key] = run
        self.by_id[str(run.id)] = run
        return run

    async def create_sql_query(
        self,
        key: str,
        sql_query_kwargs: dict | None = None,
    ) -> SQLQuery:
        if key in self.sql_queries:
            return self.sql_queries[key]

        payload = dict(sql_query_kwargs or {})
        sql_query = await create_sql_query_model(
            async_session=self.async_session,
            sql_query_kwargs=payload,
        )
        self.sql_queries[key] = sql_query
        self.by_id[str(sql_query.id)] = sql_query
        return sql_query

    async def create_operation(
        self,
        key: str,
        run: Run,
        operation_kwargs: dict | None = None,
        sql_query: SQLQuery | None = None,
    ) -> Operation:
        if key in self.operations:
            return self.operations[key]

        payload = dict(operation_kwargs or {})
        payload["run_id"] = run.id
        if sql_query is not None:
            payload["sql_query_id"] = sql_query.id

        operation = await create_operation_model(
            async_session=self.async_session,
            operation_kwargs=payload,
        )
        self.operations[key] = operation
        self.by_id[str(operation.id)] = operation
        return operation

    async def create_input(
        self,
        key: str,
        operation: Operation,
        run: Run,
        job: Job,
        dataset: Dataset,
        schema: Schema | None = None,
        input_kwargs: dict | None = None,
    ) -> Input:
        if key in self.inputs:
            return self.inputs[key]

        payload = dict(input_kwargs or {})
        payload.update(
            {
                "operation_id": operation.id,
                "run_id": run.id,
                "job_id": job.id,
                "dataset_id": dataset.id,
                "schema_id": schema.id if schema is not None else None,
            },
        )
        input_row = await create_input_model(
            async_session=self.async_session,
            input_kwargs=payload,
        )
        self.inputs[key] = input_row
        self.by_id[str(input_row.id)] = input_row
        return input_row

    async def create_output(
        self,
        key: str,
        operation: Operation,
        run: Run,
        job: Job,
        dataset: Dataset,
        output_type: OutputType | None = None,
        schema: Schema | None = None,
        output_kwargs: dict | None = None,
    ) -> Output:
        if key in self.outputs:
            return self.outputs[key]

        payload = dict(output_kwargs or {})
        payload.update(
            {
                "operation_id": operation.id,
                "run_id": run.id,
                "job_id": job.id,
                "dataset_id": dataset.id,
                "schema_id": schema.id if schema is not None else None,
            },
        )
        if output_type is not None:
            payload["type"] = output_type

        output = await create_output_model(
            async_session=self.async_session,
            output_kwargs=payload,
        )
        self.outputs[key] = output
        self.by_id[str(output.id)] = output
        return output

    async def create_column_lineage(
        self,
        key: str,
        operation: Operation,
        run: Run,
        job: Job,
        source_dataset: Dataset,
        target_dataset: Dataset,
        fingerprint: UUID | None = None,
        column_lineage_kwargs: dict | None = None,
    ) -> ColumnLineage:
        if key in self.column_lineage:
            return self.column_lineage[key]

        payload = dict(column_lineage_kwargs or {})
        payload.update(
            {
                "operation_id": operation.id,
                "run_id": run.id,
                "job_id": job.id,
                "source_dataset_id": source_dataset.id,
                "target_dataset_id": target_dataset.id,
            },
        )
        if fingerprint is not None:
            payload["fingerprint"] = fingerprint

        column_lineage = await create_column_lineage_model(
            async_session=self.async_session,
            column_lineage_kwargs=payload,
        )
        self.column_lineage[key] = column_lineage
        self.by_id[str(column_lineage.id)] = column_lineage
        return column_lineage

    async def create_column_relation(
        self,
        key: str,
        fingerprint: UUID,
        column_relation_kwargs: dict | None = None,
    ) -> DatasetColumnRelation:
        if key in self.column_relations:
            return self.column_relations[key]

        payload = dict(column_relation_kwargs or {})
        column_relation = await create_column_relation_model(
            async_session=self.async_session,
            fingerprint=fingerprint,
            column_relation_kwargs=payload,
        )
        self.column_relations[key] = column_relation
        self.by_id[str(column_relation.id)] = column_relation
        return column_relation

    async def create_personal_token(
        self,
        key: str,
        user: User,
        token_kwargs: dict | None = None,
    ) -> PersonalToken:
        if key in self.personal_tokens:
            return self.personal_tokens[key]

        payload = dict(token_kwargs or {})
        token = await create_personal_token_model(
            async_session=self.async_session,
            user=user,
            token_kwargs=payload,
        )
        self.personal_tokens[key] = token
        self.by_id[str(token.id)] = token
        return token

    def register_direct_column_relation(
        self,
        column_lineage: ColumnLineage,
        source_relations: list[DatasetColumnRelation],
        target_relations: list[DatasetColumnRelation],
    ) -> None:
        self.lineage_result.direct_column_relations[column_lineage.fingerprint] = {
            "source": source_relations,
            "target": target_relations,
        }
        self.lineage_result.direct_column_lineage.append(column_lineage)

    def register_indirect_column_relation(
        self,
        column_lineage: ColumnLineage,
        relations: list[DatasetColumnRelation],
    ) -> None:
        self.lineage_result.indirect_column_relations[column_lineage.fingerprint] = relations
        self.lineage_result.indirect_column_lineage.append(column_lineage)

    def build(self) -> LineageResult:
        self.lineage_result.jobs = list(self.jobs.values())
        self.lineage_result.runs = list(self.runs.values())
        self.lineage_result.operations = list(self.operations.values())
        self.lineage_result.datasets = list(self.datasets.values())
        self.lineage_result.inputs = list(self.inputs.values())
        self.lineage_result.outputs = list(self.outputs.values())
        self.lineage_result.dataset_symlinks = list(self.dataset_symlinks.values())
        if not self.lineage_result.direct_column_lineage:
            self.lineage_result.direct_column_lineage = list(self.column_lineage.values())
        if not self.lineage_result.indirect_column_lineage:
            self.lineage_result.indirect_column_lineage = list(self.column_lineage.values())
        return self.lineage_result
