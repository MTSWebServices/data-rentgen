# SPDX-FileCopyrightText: 2024-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from datetime import timedelta

from data_rentgen.consumer.extractors.generic.column_lineage import ColumnLineageExtractorMixin
from data_rentgen.consumer.extractors.generic.dataset import DatasetExtractorMixin
from data_rentgen.consumer.extractors.generic.io import IOExtractorMixin
from data_rentgen.consumer.extractors.generic.job import JobExtractorMixin
from data_rentgen.consumer.extractors.generic.operation import OperationExtractorMixin
from data_rentgen.consumer.extractors.generic.run import RunExtractorMixin


class GenericExtractor(
    JobExtractorMixin,
    RunExtractorMixin,
    OperationExtractorMixin,
    DatasetExtractorMixin,
    IOExtractorMixin,
    ColumnLineageExtractorMixin,
):
    """
    Generic DTO extractor implementation. Uses mixin classes to extract specific entities:

    * JobDTO
    * RunDTO
    * OperationDTO
    * InputDTO
    * OutputDTO
    * ColumnLineageDTO

    Designed to be inherited by implementation-specific classes.
    """

    def __init__(self, io_time_resolution: timedelta = timedelta(hours=1)):
        super().__init__()
        self.io_time_resolution = io_time_resolution
