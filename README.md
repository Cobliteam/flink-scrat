# flink-scrat
A python client to deploy JVM flink applications to a remote cluster

## Install

### Create a virtualenv

Flink-Scrat is written in python 3, so to create a virtual enviroment for installing and runnning it you'll have to expecify the python version with the following command:
```
virtualenv -p python3 scrat-env
source scrat-env/bin/activate
```
### Setup

After activating the virtualenv you should have a complete python enviroment with pip, so to install Scrat just run the following command and all required dependencies will be downloaded and installed with the client:

```
python setup.py install
```

## Usage

```
usage: flink-scrat [-h] [--y] [--app-id APP_ID] [--address ADDRESS || --yarn-address YARN_ADDRESS] [--port PORT || --yarn-port YARN_PORT] {submit,cancel} ...

A python client to deploy Flink applications to a remote cluster

positional arguments:
  {submit,cancel}    sub-command help
    submit           Submit a job to the flink cluster
    cancel           Cancel a running job

optional arguments:
  -h, --help         show this help message and exit
  --y                                If set, flink-scrat will use the {--app-id; --yarn-
                                     address; --yarn-port} to try to find the correct yarn-
                                     session info. Otherwise, it will assume that the
                                     correct JobManager info is provided in {--address; --port}
  --app-id APP_ID                    Identifier tag for the Flink yarn-session. If not set,
                                     it will be assumed that the yarn-session has no identifier
  --address ADDRESS                  Address for Flink JobManager
  --yarn-address YARN_ADDRESS        Address for yarn manager
  --port PORT                        Port for Flink JobManager (default 8081)
  --yarn-port YARN_PORT              Port for yarn manager
```

If `--y` is set, it is necessary to provide `--app-id`, `--yarn-address` and `--yarn-port`. Otherwise, `address` and `port` should be provided instead.

`--yarn-address` and `--address` are mutually exclusive, as well as `--yarn-port` and `--port`.

### Submit

When submitting a job to the flink cluster you have the option of restore that job from a previous savepoint or to just run that job. To use the restored state, if you wish to restore from a running job just add the jobId of the job you want to savepoint and the target directory of that savepoint to the submit action, if you want to restore from a given savepoint, add the path to the savepoint-path parameter. Otherwise just add the path to the jar and that's it.

```
usage: flink-scrat submit [-h] [--savepoint-path SAVEPOINT_PATH] --jar-path
                          JAR_PATH [--target-dir TARGET_DIR] [--job-id JOB_ID]
                          [--parallelism PARALLELISM]
                          [--entry-class ENTRY_CLASS]
                          [--allow-non-restore ANR] [--extra-args EXTRA_ARGS]

optional arguments:
  -h, --help            show this help message and exit
  --savepoint-path SAVEPOINT_PATH
                        Restore a job from a savepoint
  --jar-path JAR_PATH   Path for jar to be deployed
  --target-dir TARGET_DIR
                        Target directory to log job savepoints
  --job-id JOB_ID       Unique identifier for job to be restored
  --parallelism PARALLELISM
                        Number of parallelism for the job tasks
  --entry-class ENTRY_CLASS
                        Main class for runnning the job
  --allow-non-restore ANR
                        If present allows job to start even if restore from
                        savepoint fails
  --extra-args EXTRA_ARGS
                        Extra comma-separeted configuration options. See
                        https://ci.apache.org/projects/flink/flink-docs-
                        release-1.8/ops/config.html
```
### Cancel

Cancels a run of a flink job. Use this command with the job id of the job you want to cancel. You can use the flag `--s` if you also wish to trigger a savepoint before shutdown.

```
usage: flink-scrat cancel [-h] [--s] [--target-dir TARGET_DIR] --job-id JOB_ID

optional arguments:
  -h, --help            show this help message and exit
  --s                   Trigger a savepoint before canceling a job
  --target-dir TARGET_DIR
                        Target directory to log job savepoints
  --job-id JOB_ID       Unique identifier for job to be canceled
```

### Savepoint

Triggers a savepoint for a running job without cancelling it

```
usage: flink-scrat savepoint [-h] --target-dir TARGET_DIR --job-id JOB_ID

optional arguments:
  -h, --help            show this help message and exit
  --target-dir TARGET_DIR
                        Target directory to log job savepoints
  --job-id JOB_ID       Unique identifier for job to be snapshot
```
