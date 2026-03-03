# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from graphlib import TopologicalSorter

from faststream import Logger
from sqlalchemy.exc import DatabaseError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from data_rentgen.consumer.extractors import BatchExtractionResult
from data_rentgen.services.uow import UnitOfWork


class DatabaseSaver:
    def __init__(
        self,
        session: AsyncSession,
        logger: Logger,
    ) -> None:
        self.unit_of_work = UnitOfWork(session)
        self.logger = logger

    async def save(self, data: BatchExtractionResult):
        self.logger.info("Saving to database")

        await self.create_locations(data)
        await self.create_tags(data)
        await self.create_tag_values(data)
        await self.create_datasets(data)
        await self.create_dataset_symlinks(data)
        await self.create_job_types(data)
        await self.create_jobs(data)
        await self.create_users(data)
        await self.create_sql_queries(data)
        await self.create_schemas(data)

        try:
            await self.create_runs_bulk(data)
        except DatabaseError:
            await self.create_runs_one_by_one(data)

        await self.create_operations(data)
        await self.create_inputs(data)
        await self.create_outputs(data)
        await self.create_column_lineage(data)

        self.logger.info("Saved successfully")

    async def create_locations(self, data: BatchExtractionResult):
        self.logger.debug("Creating locations")
        # It's hard to fetch locations in bulk, and number of locations is usually small,
        # so using a row-by-row approach
        for location_dto in data.locations():
            async with self.unit_of_work:
                location = await self.unit_of_work.location.create_or_update(location_dto)
            location_dto.id = location.id

    # To avoid deadlocks when parallel consumer instances insert/update the same row,
    # commit changes for each row instead of committing the whole batch. Yes, this cloud be slow.
    # But most entities are unchanged after creation, so we could just fetch them, and do nothing.
    async def create_datasets(self, data: BatchExtractionResult):
        self.logger.debug("Creating datasets")
        dataset_pairs = await self.unit_of_work.dataset.fetch_bulk(data.datasets())
        for dataset_dto, dataset in dataset_pairs:
            async with self.unit_of_work:
                if not dataset:
                    dataset = await self.unit_of_work.dataset.create_or_update(dataset_dto)  # noqa: PLW2901
                else:
                    dataset = await self.unit_of_work.dataset.update(dataset, dataset_dto)  # noqa: PLW2901
            dataset_dto.id = dataset.id

    async def create_dataset_symlinks(self, data: BatchExtractionResult):
        self.logger.debug("Creating dataset symlinks")
        dataset_symlinks_pairs = await self.unit_of_work.dataset_symlink.fetch_bulk(data.dataset_symlinks())
        for dataset_symlink_dto, dataset_symlink in dataset_symlinks_pairs:
            if not dataset_symlink:
                async with self.unit_of_work:
                    dataset_symlink = await self.unit_of_work.dataset_symlink.create(dataset_symlink_dto)  # noqa: PLW2901
            dataset_symlink_dto.id = dataset_symlink.id

    async def create_job_types(self, data: BatchExtractionResult):
        self.logger.debug("Creating job types")
        job_type_pairs = await self.unit_of_work.job_type.fetch_bulk(data.job_types())
        for job_type_dto, job_type in job_type_pairs:
            if not job_type:
                async with self.unit_of_work:
                    job_type = await self.unit_of_work.job_type.create(job_type_dto)  # noqa: PLW2901
            job_type_dto.id = job_type.id

    async def create_jobs(self, data: BatchExtractionResult):
        self.logger.debug("Creating jobs")
        job_pairs = await self.unit_of_work.job.fetch_bulk(data.jobs())

        key_to_pair = {job_dto.unique_key: (job_dto, job) for job_dto, job in job_pairs}
        # It's possible that parent_job doesn't exist yet.
        # By using topological sort (parent before child) we can be sure that parent_job will be created before child.
        sorter: TopologicalSorter = TopologicalSorter()
        for job_dto, _ in job_pairs:
            sorter.add(job_dto.unique_key)
            if job_dto.parent_job:
                sorter.add(job_dto.unique_key, job_dto.parent_job.unique_key)

        sorted_job_pairs = [key_to_pair[key] for key in sorter.static_order()]

        for job_dto, job in sorted_job_pairs:
            async with self.unit_of_work:
                if not job:
                    job = await self.unit_of_work.job.create_or_update(job_dto)  # noqa: PLW2901
                else:
                    job = await self.unit_of_work.job.update(job, job_dto)  # noqa: PLW2901
            job_dto.id = job.id

    async def create_users(self, data: BatchExtractionResult):
        self.logger.debug("Creating users")
        user_pairs = await self.unit_of_work.user.fetch_bulk(data.users())
        for user_dto, user in user_pairs:
            if not user:
                async with self.unit_of_work:
                    user = await self.unit_of_work.user.create(user_dto)  # noqa: PLW2901
            user_dto.id = user.id

    async def create_sql_queries(self, data: BatchExtractionResult):
        self.logger.debug("Creating sql queries")
        sql_query_ids = await self.unit_of_work.sql_query.fetch_known_ids(data.sql_queries())
        for sql_query_dto, sql_query_id in sql_query_ids:
            if not sql_query_id:
                async with self.unit_of_work:
                    sql_query = await self.unit_of_work.sql_query.create(sql_query_dto)
                    sql_query_dto.id = sql_query.id
            else:
                sql_query_dto.id = sql_query_id

    async def create_schemas(self, data: BatchExtractionResult):
        self.logger.debug("Creating schemas")
        schema_ids = await self.unit_of_work.schema.fetch_known_ids(data.schemas())
        for schema_dto, schema_id in schema_ids:
            if not schema_id:
                async with self.unit_of_work:
                    schema = await self.unit_of_work.schema.create(schema_dto)
                    schema_dto.id = schema.id
            else:
                schema_dto.id = schema_id

    async def create_tags(self, data: BatchExtractionResult):
        self.logger.debug("Creating tags")
        tag_pairs = await self.unit_of_work.tag.fetch_bulk(data.tags())
        for tag_dto, tag in tag_pairs:
            if not tag:
                async with self.unit_of_work:
                    tag = await self.unit_of_work.tag.create(tag_dto)  # noqa: PLW2901
            tag_dto.id = tag.id

    async def create_tag_values(self, data: BatchExtractionResult):
        self.logger.debug("Creating tag values")
        tag_value_pairs = await self.unit_of_work.tag_value.fetch_bulk(data.tag_values())
        for tag_value_dto, tag_value in tag_value_pairs:
            if not tag_value:
                async with self.unit_of_work:
                    tag_value = await self.unit_of_work.tag_value.create(tag_value_dto)  # noqa: PLW2901
            tag_value_dto.id = tag_value.id

    # In most cases, all the run tree created by some parent is send into one
    # Kafka partition, and thus handled by just one worker.
    # Cross fingers and create all runs in one transaction.
    async def create_runs_bulk(self, data: BatchExtractionResult):
        self.logger.debug("Creating runs in bulk")
        async with self.unit_of_work:
            await self.unit_of_work.run.create_or_update_bulk(data.runs())

    # In case then child and parent runs are in different partitions,
    # multiple workers may try to create/update the same run, leading to a deadlock.
    # Fallback to creating runs one by one
    async def create_runs_one_by_one(self, data: BatchExtractionResult):
        self.logger.debug("Creating runs in one-by-one")
        run_pairs = await self.unit_of_work.run.fetch_bulk(data.runs())
        for run_dto, run in run_pairs:
            try:
                async with self.unit_of_work:
                    if not run:
                        await self.unit_of_work.run.create(run_dto)
                    else:
                        await self.unit_of_work.run.update(run, run_dto)
            except IntegrityError:  # noqa: PERF203
                # deadlock occurred, states in DB and RAM are out of sync,
                # so we have to fetch run from DB
                async with self.unit_of_work:
                    await self.unit_of_work.run.create_or_update(run_dto)

    # All events related to same operation are always send to the same Kafka partition,
    # so other workers never insert/update the same operation in parallel.
    # These rows can be inserted/updated in bulk, in one transaction.
    async def create_operations(self, data: BatchExtractionResult):
        async with self.unit_of_work:
            self.logger.debug("Creating operations")
            await self.unit_of_work.operation.create_or_update_bulk(data.operations())

    async def create_inputs(self, data: BatchExtractionResult):
        async with self.unit_of_work:
            self.logger.debug("Creating inputs")
            await self.unit_of_work.input.create_or_update_bulk(data.inputs())

    async def create_outputs(self, data: BatchExtractionResult):
        async with self.unit_of_work:
            self.logger.debug("Creating outputs")
            await self.unit_of_work.output.create_or_update_bulk(data.outputs())

    async def create_column_lineage(self, data: BatchExtractionResult):
        async with self.unit_of_work:
            self.logger.debug("Creating dataset column relations")
            await self.unit_of_work.dataset_column_relation.create_bulk_for_column_lineage(data.column_lineage())

            self.logger.debug("Creating column lineage")
            await self.unit_of_work.column_lineage.create_bulk(data.column_lineage())
