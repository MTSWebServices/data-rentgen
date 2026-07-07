# CLI for refreshing analytic views { #refresh-analytic-views-cli }

Analytic views are:

- `input_daily_stats`, `input_weekly_stats`, `input_monthly_stats`
- `output_daily_stats`, `output_weekly_stats`, `output_monthly_stats`

Views content is based on data in `output` and `input` tables and has such structure:

- `dataset_name` - Name of dataset.
- `dataset_location` - Name of dataset location (e.g. clusster name).
- `dataset_location_type` - Type of dataset location (e.g. hive, hdfs, postgres).
- `user_id` - Internal user id.
- `user_name` - Internal user name (e.g. name of user which run spark job).
- `last_interaction_dt` - Time when user lat time interact with dataset. Read or write depens on base table.
- `num_of_interactions` - Number of interactions in given interval.
- `sum_bytes` - Sum of bytes in given interval.
- `sum_rows` - Sum of rows in given interval.
- `sum_files` - Sum of files in given interval.

We provide three types of views: `day`, `week` and `month`, based on the time period in which the aggregation occur.

By default these materialized views are empty(`WITH NO DATA`).
In order to fill these tables with data you need to run refresh script (see below).

```shell
usage: python3 -m data_rentgen.db.scripts.refresh_analytic_views --depths day
```

## Arguments

- `--depths`: (Optional) Depth(s) of materialized view data (`created_at` filter). You can pass more than one value.
    - Choices: `day`, `week`, `month`
    - Default: all depths.

## Examples

1. Refresh views for all depths (default behavior):

    ```shell
    python3 -m data_rentgen.db.scripts.refresh_analytic_views
    ```

2. Refresh only daily views:

    ```shell
    python3 -m data_rentgen.db.scripts.refresh_analytic_views --depths day
    ```

3. Refresh several specific depths:

    ```shell
    python3 -m data_rentgen.db.scripts.refresh_analytic_views --depths day week
    ```
