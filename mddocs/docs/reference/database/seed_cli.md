# CLI for seeding database { #db-seed-cli }

This script seeds the database with random-generated example data (Spark, Hive, Flink and dbt runs), useful for local development and demos.

```shell
usage: python3 -m data_rentgen.db.scripts.seed
```

## Arguments

- `--start`: (Optional) Start date for generated data.
    - Type: Date (e.g., `YYYY-MM-DD`). The script uses isoparse for parsing, so various ISO formats are supported.
    - Default: 2 weeks ago.
- `--end`: (Optional) End date for generated data.
    - Type: Date (e.g., `YYYY-MM-DD`).
    - Default: now.
- `--min-runs`: (Optional) Minimum number of runs to generate per job type.
    - Type: Integer.
    - Default: `50`.
- `--max-runs`: (Optional) Maximum number of runs to generate per job type.
    - Type: Integer.
    - Default: `100`.

## Examples

1. Seed database with default settings (data for the last 2 weeks):

    ```shell
    python3 -m data_rentgen.db.scripts.seed
    ```

2. Seed database with data for a specific date range:

    ```shell
    python3 -m data_rentgen.db.scripts.seed --start 2024-01-01 --end 2024-02-01
    ```

3. Seed database with a smaller amount of data:

    ```shell
    python3 -m data_rentgen.db.scripts.seed --min-runs 5 --max-runs 10
    ```
