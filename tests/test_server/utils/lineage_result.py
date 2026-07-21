from dataclasses import dataclass, field

from uuid6 import UUID

from data_rentgen.db.models import (
    ColumnLineage,
    Dataset,
    DatasetColumnRelation,
    DatasetSymlink,
    Input,
    Job,
    Operation,
    Output,
    Run,
)


@dataclass(slots=True)
class LineageResult:
    jobs: list[Job] = field(default_factory=list)
    runs: list[Run] = field(default_factory=list)
    operations: list[Operation] = field(default_factory=list)
    datasets: list[Dataset] = field(default_factory=list)
    inputs: list[Input] = field(default_factory=list)
    outputs: list[Output] = field(default_factory=list)
    dataset_symlinks: list[DatasetSymlink] = field(default_factory=list)
    direct_column_lineage: list[ColumnLineage] = field(default_factory=list)
    direct_column_relations: dict[UUID, dict[str, list[DatasetColumnRelation]]] = field(default_factory=dict)
    indirect_column_lineage: list[ColumnLineage] = field(default_factory=list)
    indirect_column_relations: dict[UUID, list[DatasetColumnRelation]] = field(default_factory=dict)
