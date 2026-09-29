# Local setup

This project was validated with dbt Core 1.12.5 and `dbt-bigquery` 1.12.1.
Install the pinned versions with `pip install -r requirements.txt`, then check
them with `dbt --version`.

The validated Cloud Shell profile was named `kkbox_revenuecat` and used the
BigQuery adapter, OAuth authentication, US location, and two threads. Use this
profile shape with your own BigQuery project and output dataset:

```yaml
kkbox_revenuecat:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: YOUR_BILLING_AND_OUTPUT_PROJECT
      dataset: YOUR_OUTPUT_DATASET
      threads: 2
      location: US
```

Choose one private profile location:

- Standard dbt location: save the profile as `~/.dbt/profiles.yml` and run dbt
  normally, for example `dbt debug` and `dbt parse`.
- Repo-local workflow used for this project: save the profile as
  `./profiles.yml` and pass `--profiles-dir .`, for example
  `dbt debug --profiles-dir .` and `dbt parse --profiles-dir .`.

The repo-local `profiles.yml` remains gitignored. Do not commit either profile
file or any credentials.

The profile's `project` and `dataset` control where dbt builds models. The raw
KKBOX sources are separate: `models/staging/sources.yml` defaults to the
existing `revenuecats-poc-project.kkbox_raw` source. Set
`DBT_KKBOX_RAW_PROJECT_ID` to read the same table names from another BigQuery
project, for example:

```sh
export DBT_KKBOX_RAW_PROJECT_ID=YOUR_RAW_SOURCE_PROJECT
```

Keep the raw dataset name `kkbox_raw` unless you intentionally update the
source configuration.

Running models or tests requires access to the raw KKBOX tables and permission
to create tables in the output dataset.

Do not commit `.env` files, service-account keys, or raw data. Use local
credentials or a managed identity outside this repository.
