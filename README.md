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

* Core
    * `whelk-core/`
        The root component of XL; a linked data store, including search and MARC conversion.

* Applications
    * `apix_export/`
        Exports data from Libris XL back to Voyager (the old system).
    * `importers/`
        Java application to load or reindex data into the system.
    * `oaipmh/`
        Servlet web application. OAIPMH service for Libris XL
    * `rest/`
        A servlet web application. The REST and other HTTP APIs
    * `marc_export/`
	    A servlet (and CLI program) for exporting libris data as MARC.

* Tools
    * `librisxl-tools/`
        Configuration and scripts used for setup, maintenance and operations.

Related external repositories:

* Core metadata to be loaded is managed in the
  [definitions](https://github.com/libris/definitions) repository.

* Also see [LXLViewer](https://github.com/libris/lxlviewer), our application
  for viewing and editing the datasets through the REST API.

## Dependencies

The instructions below assume an Ubuntu 20.04 system (Debian should be identical), but should work
for e.g. Fedora/CentOS/RHEL with minor adjustments.

1. [Gradle](http://gradle.org/)

    No setup required. Just use the checked-in
    [gradle wrapper](https://docs.gradle.org/current/userguide/gradle_wrapper.html)
    to automatically get the specified version of Gradle and Groovy.

2. [Elasticsearch](http://elasticsearch.org/) (version 7.x)

    [Download Elasticsearch](https://www.elastic.co/downloads/elasticsearch-oss)
    (for Ubuntu/Debian, select "Install with apt-get"; before importing the Elasticsearch
    PGP key you might have to do `sudo apt install gnupg` if you're running a minimal distribution.)

    **NOTE:** We use the elasticsearch-oss version.

3. [PostgreSQL](https://www.postgresql.org/) (version 9.4 or later)

    ```
    # Ubuntu/Debian
    sudo apt install postgresql postgresql-client
    # macOS
    brew install postgresql
    ```
    Windows:
    Download and install https://www.postgresql.org/download/windows/

4. [Java](https://openjdk.java.net/) (version 8)

    ```
    sudo apt install openjdk-8-jdk # or openjdk-8-headless
    ```

## Setup

### Cloning repositories

Make sure you check out this repository, and also [definitions](https://github.com/libris/definitions)
and [devops](https://github.com/libris/devops):

```
git clone git@github.com:libris/librisxl.git
git clone git@github.com:libris/definitions.git
# devops repo is private; ask for access
git clone git@github.com:libris/devops.git
```

You should now have the following directory structure:

```
.
├── definitions
├── devops
├── librisxl
```

### Setting up PostgreSQL

Ensure PostgreSQL is started. In Debian/Ubuntu, this happens automatically after
`apt install`. Otherwise, try `systemctl start postgresql` in any modern Linux system.

Create database and a database user and set up permissions:
```
sudo -u postgres bash
createdb whelk_dev
psql -c "CREATE USER whelk PASSWORD 'whelk';"
# !! Replace yourusername with your actual username (i.e., the user you'll run whelk, fab, etc. as)
psql -c "CREATE USER yourusername;"
psql -c "GRANT ALL ON SCHEMA public TO whelk;" whelk_dev
psql -c "GRANT ALL ON ALL TABLES IN SCHEMA public TO whelk;" whelk_dev
# Now find out where the pg_hba.conf file is:
psql -t -P format=unaligned -c 'show hba_file;'
exit
```

Give all users access to your local database by editing `pg_hba.conf`. You got the path
from the last `psql` command just above. It's probably something like
`/etc/postgresql/12/main/pg_hba.conf`. Edit it and add the following _above_ any uncommented
line (PostgreSQL uses the first match):

```
host    all             all        127.0.0.1/32            trust
host    all             all        ::1/128                 trust
```

Restart PostgreSQL for the changes to take effect:

```
sudo systemctl restart postgresql
```

Test connectivity:

```
psql -h localhost -U whelk whelk_dev
psql (12.5 (Ubuntu 12.5-0ubuntu0.20.04.1))
SSL connection (protocol: TLSv1.3, cipher: TLS_AES_256_GCM_SHA384, bits: 256, compression: off)
Type "help" for help.

whelk_dev=> \q
```

### Setting up Elasticsearch

Edit `/etc/elasticsearch/elasticsearch.yml`. Uncomment `cluster.name` and set it to something unique
on the network. This name is later specified when you configure the XL system. Then, (re)start
Elasticsearch:

```
sudo systemctl restart elasticsearch
```

(To adjust the JVM heap size for Elasticsearch, edit `/etc/elasticsearch/jvm.options` and then restart
Elasticsearch.)

### Configuring secrets

Use `librisxl/secret.properties.in` as a starting point:

```
cd librisxl
cp secret.properties.in secret.properties
# In secret.properties, set:
# - elasticCluster to whatever you set cluster.name to in the Elasticsearch configuration above.
vim secret.properties
# Make sure kblocalhost.kb.se points to 127.0.0.1
echo '127.0.0.1 kblocalhost.kb.se' | sudo tee -a /etc/hosts
```

### Importing test data

Run the fabric task that sets up a new Elasticsearch index and imports example data:

```
cd ../devops
# Make sure you have Python 3 and curl
sudo apt install python3 python3-pip curl
# Create virtual Python 3 environment for fab
python3 -m venv venv
# Activate virtual environment
source venv/bin/activate
# Install dependencies
pip install -r requirements.txt
# Create Elasticsearch index
fab conf.xl_local app.whelk.create_es_index
# Import test data
fab conf.xl_local app.whelk.import_work_example_data
```

### Running

To start the CRUD part of the whelk, run the following commands:

*NIX-systems:
```
cd ../librisxl/rest
export JAVA_OPTS="-Dfile.encoding=utf-8"
../gradlew -Dxl.secret.properties=../secret.properties appRun
```

Windows:
```
$ cd $LIBRISXL/rest
$ setx JAVA_OPTS "-Dfile.encoding=utf-8"
$ ../gradlew.bat -Dxl.secret.properties=../secret.properties appRun
```

The system is then available on <http://localhost:8180>.
(The OAI-PMH service is started in a similar way: just cd into `oaipmh`
instead of `rest`.)

To run the frontend, see [LXLViewer](https://github.com/libris/lxlviewer).

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
    -H 'Content-Type: application/json' \
    -d @librisxl-tools/elasticsearch/libris_config.json
```

Create an alias for your index

```
$ curl -XPOST http://localhost:9200/_aliases \
    -H 'Content-Type: application/json' \
    -d  '{"actions":[{"add":{"index":"indexname_versionnumber","alias":"indexname"}}]}'
```

(To replace an existing setup with entirely new configuration, you need to
delete the index `curl -XDELETE http://localhost:9200/<indexname>/` and read
all data again (even locally).)

### Format updates

If the MARC conversion process has been updated and needs to be run anew, the only
option is to reload the data from production using the importers application.

### Statistics

Produce a stats file (here for bib) by running:

```
$ cd importers && ../gradlew build
$ RECTYPE=bib && time java -Dxl.secret.properties=../secret.properties -Dxl.mysql.properties=../mysql.properties -jar build/libs/vcopyImporter.jar vcopyjsondump $RECTYPE | grep '^{' | pypy ../librisxl-tools/scripts/get_marc_usage_stats.py $RECTYPE /tmp/usage-stats-$RECTYPE.json
```
