# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from data_rentgen.consumer.extractors.batch_extraction_result import BatchExtractionResult
from data_rentgen.consumer.extractors.impl import (
    AirflowDagExtractor,
    AirflowTaskExtractor,
    DbtExtractor,
    ExtractorInterface,
    FlinkExtractor,
    HiveExtractor,
    SparkExtractor,
    UnknownExtractor,
)
from data_rentgen.consumer.extractors.impl.starrocks import StarRocksExtractor
from data_rentgen.openlineage.run_event import OpenLineageRunEvent


class BatchExtractor:
    """
    Try all extractors in the chain, convert events to a bunch of DTOs, and add them to BatchExtractionResult
    """

    def __init__(self) -> None:
        self.result = BatchExtractionResult()
        # extractors may have some state which should be preserver between method calls,
        # so we need to initialize them in a constructor (not as a static field), and then reuse.
        self.extractors_chain: list[ExtractorInterface] = [
            SparkExtractor(),
            AirflowDagExtractor(),
            AirflowTaskExtractor(),
            HiveExtractor(),
            FlinkExtractor(),
            DbtExtractor(),
            StarRocksExtractor(),
        ]
        self.unknown_extractor = UnknownExtractor()

    def get_extractor_impl(self, event: OpenLineageRunEvent) -> ExtractorInterface:
        for extractor in self.extractors_chain:
            if extractor.match(event):
                return extractor
        return self.unknown_extractor

    def add_events(self, events: list[OpenLineageRunEvent]) -> BatchExtractionResult:
        for event in events:
            extractor = self.get_extractor_impl(event)
            if extractor.is_operation(event):
                self._add_operation(event, extractor)
            else:
                self._add_run(event, extractor)
        return self.result

    def _add_run(self, event: OpenLineageRunEvent, extractor: ExtractorInterface):
        run = extractor.extract_run(event)
        self.result.add_run(run)

    def _add_operation(self, event: OpenLineageRunEvent, extractor: ExtractorInterface):
        operation = extractor.extract_operation(event)
        self.result.add_operation(operation)

        for input_dataset in event.inputs:
            input_dto, symlink_dtos = extractor.extract_input(operation, input_dataset, event)
            self.result.add_input(input_dto)

            for symlink_dto in symlink_dtos:
                self.result.add_dataset_symlink(symlink_dto)

        for output_dataset in event.outputs:
            output_dto, symlink_dtos = extractor.extract_output(operation, output_dataset, event)
            self.result.add_output(output_dto)

            for symlink_dto in symlink_dtos:
                self.result.add_dataset_symlink(symlink_dto)

            column_lineage = extractor.extract_column_lineage(operation, output_dataset, event)
            for item in column_lineage:
                self.result.add_column_lineage(item)
