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
usage: flink-scrat [-h] [--address ADDRESS] [--port PORT] {submit,cancel} ...

A python client to deploy Flink applications to a remote cluster

positional arguments:
  {submit,cancel}    sub-command help
    submit           Submit a job to the flink cluster
    cancel           Cancel a running job

optional arguments:
  -h, --help         show this help message and exit
  --address ADDRESS  Address for Flink JobManager
  --port PORT        Port for Flink JobManager (default 8081)
```

### Submit

When submitting a job to the flink cluster you have the option of restore that job from a previous savepoint or to just run that job. To use the restored state just add the jobId of the job you want to savepoint and the target directory of that savepoint to the submit action. Otherwise just add the path to the jar and that's it

```
flink-scrat submit [-h] --jar-path JAR_PATH [--target-dir TARGET_DIR]
                          [--job-id JOB_ID]

optional arguments:
  -h, --help            	show this help message and exit
  --jar-path JAR_PATH   	Path for jar to be deployed
  --target-dir TARGET_DIR	Target directory to log job savepoints
  --job-id JOB_ID       	Unique identifier for job to be restored
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