# CLI for creating partitions { #create-partitions-cli }

This script creates partitions for tables storing lineage data (`run`, `operation`, `input`, `output`, `column_lineage`), for a given date range and granularity.

```shell
usage: python3 -m data_rentgen.db.scripts.create_partitions --start 2024-01 --granularity month
```

It's recommended to run this script on schedule (e.g. via cron), so partitions for upcoming periods are always created in advance. See [Relation Database][database] for details.

## Arguments

- `--start`: (Optional) Start date for partitions.
    - Type: Date (e.g., `YYYY-MM-DD`). The script uses isoparse for parsing, so various ISO formats are supported.
    - Default: The first day of the current month.
- `--end`: (Optional) End date for partitions.
    - Type: Date (e.g., `YYYY-MM-DD`).
    - Default: The last day of the next month.
- `--granularity`: (Optional) Granularity of created partitions.
    - Choices: `day`, `month`, `year`
    - Default: `month`

## Examples

1. Create monthly partitions for current and next month (default behavior):

    ```shell
    python3 -m data_rentgen.db.scripts.create_partitions
    ```

2. Create daily partitions for a specific range:

    ```shell
    python3 -m data_rentgen.db.scripts.create_partitions --start 2024-01-01 --end 2024-02-01 --granularity day
    ```

3. Create yearly partitions starting from a specific date:

    ```shell
    python3 -m data_rentgen.db.scripts.create_partitions --start 2024-01-01 --granularity year
    ```
