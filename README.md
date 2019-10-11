# Libris XL

----

* [Parts](#parts)
* [Dependencies](#dependencies)
* [Setup](#setup)
* [Data](#data)
* [Maintenance](#maintenance)

----

## Parts

The project consists of:

* Applications
    * `apix_export/`
        Exports data from Libris XL back to Voyager (the old system).
    * `importers/`
        Java application to load or reindex data into the system.
    * `oaipmh/`
        Servlet web application. OAIPMH service for Libris XL
    * `rest/`
        A servlet web application. The REST and other HTTP APIs
* Tools
    * `librisxl-tools/`
        Configuration and scripts used for setup, maintenance and operations.

Related external repositories:

* The applications above depend on the [Whelk
  Core](https://github.com/libris/whelk-core) repository.

* Core metadata to be loaded is managed in the
  [definitions](https://github.com/libris/definitions) repository.

* Also see [LXLViewer](https://github.com/libris/lxlviewer), our application
  for viewing and editing the datasets through the REST API.

## Dependencies

1. [Gradle](http://gradle.org/)

    No setup required. Just use the checked-in
    [gradle wrapper](https://docs.gradle.org/current/userguide/gradle_wrapper.html)
    to automatically get the specified version of Gradle and Groovy.

2. [Elasticsearch](http://elasticsearch.org/)

    For OS X:
    ```
    $ brew install elasticsearch
    ```

    For Debian, follow instructions on
    https://www.elastic.co/guide/en/elasticsearch/reference/current/deb.html
    first, then:
    ```
    apt-get install elasticsearch
    ```

    For Windows, download and install:
    https://www.elastic.co/downloads/past-releases/elasticsearch-2-4-1

    **NOTE:** You will also need to set `cluster.name` in
    `/etc/elasticsearch/elasticsearch.yml` to something unique on the
    network. This name is later specified when you configure the
    system. Don't forget to restart Elasticsearch after the change.

    For Elasticsearch version 2.2 and greater, you must also install the
    `delete-by-query` plugin. This functionality was removed in ElasticSearch
    2.0 and needs to be added as a plugin:
    ```
    $ /path/to/elasticsearch/bin/plugin install delete-by-query
    ```

    **NOTE:** You will need to reinstall the plugin whenever you
      upgrade ElasticSearch.

3. [PostgreSQL](https://www.postgresql.org/) (version 9.4 or later)

    ```
    # OS X
    $ brew install postgresql
    # Debian
    $ apt-get install postgresql postgresql-client
    ```
    Windows:
    Download and install https://www.postgresql.org/download/windows/

## Setup

### Configuring secrets

Use `librisxl/secret.properties.in` as a starting point:

```
$ cd $LIBRISXL
$ cp secret.properties.in secret.properties
$ vim secret.properties
```

### Setting up PostgreSQL

0. Ensure PostgreSQL is started

    E.g.:
    ```
    $ postgres -D /usr/local/var/postgres
    ```

1. Create database

    ```
    # You might need to become the postgres user (e.g. sudo -u postgres bash) first
    $ createdb whelk_dev
    ```

    (Optionally, create a database user)

    ```
    $ psql whelk_dev
    psql (9.5.4)
    Type "help" for help.

    whelk=# CREATE SCHEMA whelk_dev;
    CREATE SCHEMA
    whelk=# CREATE USER whelk PASSWORD 'whelk';
    CREATE ROLE
    whelk=# GRANT ALL ON SCHEMA whelk_dev TO whelk;
    GRANT
    whelk=# GRANT ALL ON ALL TABLES IN SCHEMA whelk_dev TO whelk;
    GRANT
    whelk=# \q
    ```

### Setting up Elasticsearch

TODO: This is now generated! This step can probably be omitted. (See the devops
repo for details.)

Create index and mappings:

```
$ cd $LIBRISXL
$ curl -XPOST http://localhost:9200/whelk_dev -d@librisxl-tools/elasticsearch/libris_config.json
```

**NOTE:** Windows users can install curl by:
```
$ choco install curl
```

### Import test data

Check out the Devops repository, which is private (ask a team member for access and put it in the same directory as the 'librisxl' repo):

For *NIX:
```bash
$ cd devops
$ pip install -r requirements.txt
$ fab conf.xl_local app.whelk.reload_example_data:force=True
```

### Running

To start the CRUD part of the whelk, run the following commands:

*NIX-systems:
```
$ cd $LIBRISXL/rest
$ export JAVA_OPTS="-Dfile.encoding=utf-8"
$ ../gradlew -Dxl.secret.properties=../secret.properties appRun
```

Windows:
```
$ cd $LIBRISXL/rest
$ setx JAVA_OPTS "-Dfile.encoding=utf-8"
$ ../gradlew.bat -Dxl.secret.properties=../secret.properties appRun
```

The system is then available on <http://localhost:8180>.


## Maintenance

Everything you would want to do should be covered by the devops repo. This
section is mostly kept as a reminder of alternate (less preferred) ways.


### Development Workflow

If you need to work locally (e.g. in this or the
"definitions" repo) and perform specific tests, you can use this workflow:

1. Create and push a branch for your work.
2. Set the branch in the `conf.xl_local` config in the devops repo.
3. Use the regular tasks to e.g. reload data.

### New Elasticsearch config

If a new index is to be set up, and unless you run locally in a pristine setup,
or use the recommended devops-method for loading data
you need to PUT the config to the index, like:

```
$ curl -XPUT http://localhost:9200/indexname_versionnumber \
    -d @librisxl-tools/elasticsearch/libris_config.json
```

Create an alias for your index

```
$ curl -XPOST http://localhost:9200/_aliases \
    -d  '{"actions":[{"add":{"index":"indexname_versionnumber","alias":"indexname"}}]}'
```

(To replace an existing setup with entirely new configuration, you need to
delete the index `curl -XDELETE http://localhost:9200/<indexname>/` and read
all data again (even locally).)

### Format updates

If the MARC conversion process has been updated and needs to be run anew, the only
option is to reload the data from vcopy using the importers application.

### Statistics

Produce a stats file (here for bib) by running:

```
$ cd importers && ../gradlew build
$ RECTYPE=bib && time java -Dxl.secret.properties=../secret.properties -Dxl.mysql.properties=../mysql.properties -jar build/libs/vcopyImporter.jar vcopyjsondump $RECTYPE | grep '^{' | pypy ../librisxl-tools/scripts/get_marc_usage_stats.py $RECTYPE /tmp/usage-stats-$RECTYPE.json
```
