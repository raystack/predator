# README

Predator (Profiler and Auditor) is a tool to provide statistical description and data quality checking of downstream data.

Predator consist of two components:
* Profile : Collect basic metrics of table and column and calculate data quality metrics.
* Audit : Compare the data quality metrics against tolerance rules.


### Requirements

* Go v1.18
* Postgres Instance

  ```
  docker run -d -p 127.0.0.1:5432:5432/tcp --name predator-abcd -e POSTGRES_PASSWORD=secretpassword -e POSTGRES_DB=predator -e POSTGRES_USER=predator postgres
  ```
* Tolerance Store
  
  * Local directory

    For producing metrics on Profile and check issues using Audit, tolerance specification is needed. Each of `.yaml` files in the local directory represents tolerance specification for a bigquery table. This options can be used for local testing. This store can be used by using local directory as `TOLERANCE_STORE_URL`

    ```
    example/tolerance
    ```
  
  * Google Cloud Storage
  
    Google cloud bucket is preferred for having file based tolerance spec to be used by Predator service, especially when combined with git repository for tolerance spec files collaboration with multiple users
    
    Please read this doc for creating gcs bucket [here](https://cloud.google.com/storage/docs/creating-buckets). The gcs bucket can be used as tolerance storage configuration in `TOLERANCE_STORE_URL`

    ```
    gs://your-bucket/audit-spec
    ```


* Unique Constraint Store (optional)

  Source of unique constraint column for each resource to calculate unique count and duplication percentage metrics, 
  in a single CSV file. This is an alternative solution if the unique constraint column is not specified in the tolerance 
  specification of each table. Please see documentation below for details of CSV content format.
  
* Publisher

  Predator publish data for profile and audit to for realtime data/event processing

  * Apache Kafka
      * Download apache kafka https://kafka.apache.org/quickstart
      * Start zookeeper `bin/zookeeper-server-start.sh config/zookeeper.properties`
      * Start kafka `bin/kafka-server-start.sh config/server.properties`
      * Create kafka topics for profile and audit
          * `bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic profile`
          * `bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic audit`
  
  * Console
    
    If Kafka broker and topic configuration are empty Predator publish the data to terminal/console. This type of Publisher is intended for local testing purpose

* Google Cloud credentials 

  Google cloud credentials is needed for predator to access Bigquery API

  * Google cloud personal account credentials

    Using this credential we can use our own Google suite email to access google cloud API including Bigquery API. 
    This credential is the most suitable for local testing/exploration purpose.

  * Application Default Credentials 
    
    This type of google cloud credentials is needed for deploy predator as service especially to use predator in a non-local environment.

    * Create google cloud application credentials
      Please read this [doc](https://cloud.google.com/docs/authentication/production) for creating an application default credentials (ADC)

    * Set local environment variable 
      ```
      GOOGLE_APPLICATION_CREDENTIALS=/path/key.json
      ```

### How to Build
* `make`

### How to Test
`make test`

### How to run predator service

#### Create .env file

1. Create copy conf/.env.template and create .env file
2. Put .env file to the root of repository
3. Set env variable

    example of config to run
    ```
    PORT=

    DB_HOST=localhost
    DB_PORT=5432
    DB_NAME=predator
    DB_USER=predator
    DB_PASS=secretpassword

    BIGQUERY_PROJECT_ID=sample-project

    PROFILE_KAFKA_TOPIC=profile
    AUDIT_KAFKA_TOPIC=audit
    KAFKA_BROKER=localhost:6668

    TOLERANCE_STORE_URL=example/tolerance

    UNIQUE_CONSTRAINT_STORE_URL=example/uniqueconstraints.csv
    MULTI_TENANCY_ENABLED=true
    GIT_AUTH_PRIVATE_KEY_PATH=~/.ssh/private.key
    TZ=UTC
    ```

#### Setup DB
`./predator migrate -e .env` to run the DB migration

Note: If any changes made on the migration files, re-run this command to re-generate the migration resource.  
`make generate-db-resource`

#### How to Run
`./predator start -e .env`

#### How to do Profile and Audit using API Call
Before begin, decide below profiling details.
  * URN
    Target table ID
  * Filter (optional)
    Filter expression in SQL syntax. This expression will be applied in the WHERE clause of profiling query. 
    For example: `__PARTITION__ = '2021-01-01'`.
  * Group (optional)
    Which field the result should be grouped with. Can be any field or __PARTITION__
  * Mode
    Profiling mode will differentiate how the result will be visualized. `complete` for presenting the results as 
    independent data result, or `incremental` for presenting it as part of another same group results.
  * Audit time
    Timestamp of when audit happened. 

1. Create profile job : `POST /v1beta1/profile`. Please include the profiling details as the payload.
2. Wait until `status` becomes `completed` 

    Call `GET /v1beta1/profile/{profile_id}` periodically until `status` becomes `completed` 

3. Audit the profiled data : `POST /v1beta1/profile/{profile_id}/audit`


#### How to do Profile and Audit using CLI
First, build by running `make build`

* To profile and audit
  `profile_audit -s {server} -u {urn} -f {filter} -g {group} -m {mode} -a {audit_time}`

* To only profile
  `profile -s {server} -u {urn} -f {filter} -g {group} -m {mode} -a {audit_time}`

Usage example:
```shell
predator profile_audit \
-s http://sample-predator-server \
-u sample-project.sample_dataset.sample_table \
-g "date(sample_timestamp_field)" \
-f "date(sample_timestamp_field) in (\"2020-12-02\",\"2020-12-01\",\"2020-11-30\")" \
-m complete \
-a "2020-12-02T07:00:00.000Z"
```

Usage example by using Docker:
```shell
docker run --rm -e SUB_COMMAND=profile_audit \
-e PREDATOR_URL=http://sample-predator-server \
-e URN=sample-project.sample_dataset.sample_table \
-e GROUP="date(sample_timestamp_field)" \
-e FILTER="__PARTITION__ = \"2020-11-01\"" \
-e MODE=complete \
-e AUDIT_TIME="2020-12-02T07:00:00.000Z" \
predator:latest
```

### Local Testing Guide

#### Dependencies

When doing local testing, some external dependency can be replaced with local files and folders. Here is the step by 
step for set up the configuration and running predator for local testing purpose. 

* Tolerance Rules Configuration
  Using yaml file in `example/tolerance`.

* Publisher
  For local testing, Apache Kafka is not required. The protobuf serialised message will be shown as console log.


#### How to do local testing

* checkout predator repository
* go to predator repository directory
* build predator binary by running `make build` script
* create .env file
* setup postgres database, please follow details on `Requirements` section for quick setup of postgres db. make sure
  to also run the db migration `./predator migrate -e .env`
* run predator service `./predator start -e .env`
* prepare the tolerance spec file
* create Profile job using API call
    ```shell script
        curl --location --request POST 'http://localhost:5000/v1beta1/profile' \
        --header 'Content-Type: application/json' \
        --data-raw '{
            "urn": "sample-project.sample_dataset.sample_table",
            "filter": "__PARTITION__ = '2020-03-01'",
            "group": "__PARTITION__",
            "mode": "complete"
        }'
    ```
* API call to get the Profile job status & result, poll the status until the status becomes `completed`
    ```shell script
   curl --location --request GET 'http://localhost:5000/v1beta1/profile/${profile_id}'
    ```
* API call to audit and get the result
    ```shell script
    curl --location --request POST 'http://localhost:5000/v1beta1/profile/${profile_id}/audit'
    ```

## Register Entity (optional)
Predator provide Upload tolerance spec feature for better collaboration among users (using git) and within a multiple entity 
environment. Each entity can be registered with its own git url, which at the time of upload Predator will clone the 
git repository to find the tolerance specs and upload them to the destination storage and being used when profile & auditing.
 
* register entity
    ```shell script
    curl --location --request POST 'http://localhost:5000/v1/entity/entity-1' \
    --header 'Content-Type: application/json' \
    --data-raw '{
        "entity_name": "sample-entity-1",
        "git_url": "git@sample-url:sample-entity-1.git",
        "environment" : "sample-env",
        "gcloud_project_ids": [
            "entity-1-project-1"
        ]
    }'
    ```


## Data Quality Spec

### Specifying Data Quality Spec

```
  tableid: "sample-project.sample_dataset.sample_table"

  tablemetrics:
  - metricname: "duplication_pct"
    tolerance:
      less_than_eq: 0
    metadata:
      uniquefields:
      - field_1

  fields:
  - fieldid: "field_1"
    fieldmetrics:
    - metricname: "nullness_pct"
      tolerance:
        less_than_eq: 10.0
  ```

  * Tolerance Rules
    * `less_than_eq`
    * `less_than`
    * `more_than_eq`
    * `more_than_eq`

  * Data quality metric available
    * `duplication_pct` (need uniquefields metadata) 
    * `nullness_pct`
    * `trend_inconsistency_pct`
    * `row_count`

### Data Quality Spec storage
  * Using Google cloud storage as file store
    * Decide GCS the bucket and base path
      
      for example if `gs://our-bucket` is our GCS bucket we can add `audit-spec` folder. So our base path folder become `gs://our-bucket/audit-spec`

    * save the spec to file with naming `<gcp-project-id>.<dataset>.<tablename>.yaml` format for example : `sample-project.sample_dataset.sample_table.yaml`

    * upload the file in format to this path `gs://sample-bucket/audit-spec/sample-project.sample_dataset.sample_table.yaml`
    * put another spec in the same folder/base path
  
  * Using local as file store
  
    * create directory on local for example `/Users/username/Documents/predator/tolerance`

    * save the spec to file with naming `<gcp-project-id>.<dataset>.<tablename>.yaml` format for    example : `sample-project.sample_dataset.sample_table.yaml`

    * move the file to the created directory so the file location will be `/Users/username/Documents/predator/tolerance/sample-project.sample_dataset.sample_table.yaml`
    * put more spec file to the directory as needed
    


### Upload Data Quality Spec
There are multiple way to upload data quality spec to predator storage, one of them is using `POST v1beta1/spec/upload` API.
Predator also provide cli to provide the same functionality. 

#### Upload through Predator CLI
```shell script
    usage: predator upload --host=HOST --git-url=GIT-URL [<flags>]
    
    upload spec from git repository to storage
    
    Flags:
          --help             Show context-sensitive help (also try --help-long and --help-man).
      -h, --host=http://sample-predator-server        predator server
      -g, --git-url=git@sample-url:sample-entity.git  url of git, the source of data quality spec
      -c, --commit-id="[sample-commit-id]"     specific git commit hash, default value will be empty and always upload latest commit
      -p, --path-prefix="predator"   path to root of predator specs directory, default will be empty
```

* Path Prefix (`--path-prefix`) is path to predator folder root directory on a git repository, fill this value if the directory root is not the same as git root. 
    ```yaml
    git_root:
        predator:
          sample-entity-1-project-1:
            dataset_a:
              table_x.yaml
    ```
* Commit ID (`--commit-id`) is commit hash of git that will be uploaded this is optional, when not set the latest commit will be used
* Git URL (`--git-url`) git url that used on git clone, only this `git@sample-url:sample-entity.git` format that is supported 

```shell script
    ./predator upload \
    --host http://sample-predator-server \
    --path-prefix predator --git-url git@sample-url:sample-entity-1.git \
    --commit-id sample-commit-id
```

#### Example of Upload through API call
from git repository to tolerance store (optional)
```shell script
    curl --location --request POST 'http://localhost:5000/v1beta1/spec/upload' \
    --header 'Content-Type: application/json' \
    --data-raw '{
        "git_url": "git@sample-url:sample-entity.git",
        "commit_id": "sample-commit-id",
        "path_prefix": "predator"
    }'
```


### API docs

`api/predator.postman_collection.json` or `api/swagger.json`

### Tech Debt
* remove ProfileMetric type and use only Metric type
* remove Meta from MetricSpec and Metric
* better abstraction of QualityMetricProfiler
* better abstraction of BasicMetricProfiler

### Monitoring

How to setup monitoring:

This step by step tutorial is taken from [cortex getting started tutorial](https://cortexmetrics.io/docs/getting-started/getting-started-chunks-storage/)
Prometheus is not required, because it only used as metric collector for Cortex, in this setup stats pushed from telegraf to cortex directly using remote write

#### Cortex

* build cortex
```shell
git clone https://github.com/cortexproject/cortex.git
cd cortex
go build ./cmd/cortex
```

* run cortex
```shell
./cortex -config.file=${PREDATOR_REPO_ROOT}/example/monitoring/single-process-config.yaml
```

#### Grafana
```shell
docker run --rm -d --name=grafana -p 3000:3000 grafana/grafana
```

In the Grafana UI (username/password admin/admin), add a Prometheus datasource for Cortex (http://host.docker.internal:9009/api/prom).
Dashboard config will be added later

Import dashboard by upload this [file](./example/monitoring/Predator-1614083874842.json)

#### Telegraf

* clone telegraf
```shell
cd ~/src
git clone https://github.com/influxdata/telegraf.git
```

* make binary
```shell
cd ~/src/telegraf
make
```

* run telegraf
```shell
./telegraf --config ${PREDATOR_REPO_ROOT}/example/monitoring/telegraf.conf
```
