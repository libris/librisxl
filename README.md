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
        The root component of XL. A shared library implementing a linked data store, including search and [MARC conversion](whelk-core/src/main/resources/ext).

* Applications
    * `oaipmh/`
        A servlet web application. OAIPMH service for Libris XL
    * `rest/`
        A servlet web application. [Search, RESTful CRUD and other HTTP APIs](rest/)
    * `marc_export/`
	A servlet (and CLI program) for exporting libris data as MARC.
    * `importers/`
        Java application to load or reindex data into the system.
    * `apix_server/`
        A servlet web application. XL reimplementation of the Libris legacy APIX API.

* Tools
    * `whelktool/`
        CLI tool for running scripted mass updates of data.
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

    **NOTE:**
    * We use the elasticsearch-oss version.
    * The [ICU Analysis plugin](https://www.elastic.co/guide/en/elasticsearch/plugins/7.12/analysis-icu.html) (`icu-analysis`) must be installed; see "Setting up Elasticsearch" below.

3. [PostgreSQL](https://www.postgresql.org/) (version 9.6 or later)

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

5. [Apache](https://httpd.apache.org/)

    ```
    sudo apt install apache2
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
on the network. This name is later specified when you configure the XL system.

Next, install the ICU Analysis plugin:

```
sudo /usr/share/elasticsearch/bin/elasticsearch-plugin install analysis-icu
```

Finally, (re)start Elasticsearch:

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

To run the frontend, first set up LXLViewer and the id.kb.se web app
(follow the README in each):

* [LXLViewer](https://github.com/libris/lxlviewer)
* [id.kb.se](https://github.com/libris/id.kb.se) (set `API_PATH=http://id.kblocalhost.kb.se:5000` in `.env`)

At this point, you should have the LXLViewer cataloging client running on port 8080
and the id.kb.se app running on port 3000, but they won't work yet. Next, edit
`/etc/apache2/sites-enabled/000-default.conf` and add the following:

```
<VirtualHost *:5000>
    ServerName kblocalhost.kb.se
    ProxyRequests Off
    ProxyPreserveHost On

    RewriteEngine On

    <LocationMatch "^/([bcdfghjklmnpqrstvwxz0-9]{15,16})$">
        ProxyPreserveHost Off
        RewriteCond %{HTTP_ACCEPT} (text/html|application/xhtml|\*/\*|^$)
        RewriteRule ([^/]+)$ http://id.kblocalhost.kb.se:5000/$1 [P]
    </LocationMatch>

    <Location /_nuxt>
        ProxyPreserveHost Off
        ProxyPass http://id.kblocalhost.kb.se:5000/_nuxt
    </Location>

    ProxyPassMatch ^/vocab/(data.*) http://localhost:8180/https://id.kb.se/vocab//$1
    ProxyPass /vocab http://localhost:8180/https://id.kb.se/vocab
    ProxyPass /context.jsonld http://localhost:8180/https://id.kb.se/vocab/context

    RewriteCond %{REQUEST_METHOD} ^(POST|PUT|DELETE|OPTIONS)$
    RewriteRule ^/data(.*)$ http://localhost:8180/$1 [P,L]

    ProxyPass / http://localhost:8180/

    AddOutputFilterByType DEFLATE text/css text/html text/plain text/xml
    AddOutputFilterByType DEFLATE application/x-javascript text/x-component application/javascript
    AddOutputFilterByType DEFLATE application/json application/ld+json
</VirtualHost>

<VirtualHost *:5000>
    ServerName id.kblocalhost.kb.se
    ProxyRequests Off
    ProxyPreserveHost On

    RewriteEngine On

    RewriteCond %{HTTP_ACCEPT} (text/html|application/xhtml|\*/\*) [OR]
    RewriteCond %{HTTP_ACCEPT} ^$
    RewriteCond %{REQUEST_URI} !\.(json|jsonld)$
    RewriteCond %{REQUEST_URI} !data\..+$
    RewriteRule ^/(.*)$ http://localhost:3000/$1 [P,L]
    ProxyPass /_nuxt http://localhost:3000/_nuxt
    ProxyPass /_loading http://localhost:3000/_loading
    ProxyPass /__webpack_hmr http://localhost:3000/__webpack_hmr

    ProxyPassMatch ^/vocab/(data.*) http://localhost:8180/https://id.kb.se/vocab//$1
    ProxyPass /vocab http://localhost:8180/https://id.kb.se/vocab
    ProxyPass /vocab/display/data.jsonld http://localhost:8180/https://id.kb.se/vocab/display
    ProxyPass /context.jsonld http://localhost:8180/https://id.kb.se/vocab/context

    ProxyPassMatch ^/(data.*)$ http://localhost:8180/$1
    ProxyPassMatch ^/find(.*) http://localhost:8180/find$1

    ProxyPassMatch ^/(http.*)$ http://localhost:8180/$1
    ProxyPassMatch ^/([bcdfghjklmnpqrstvwxz0-9]{15,16}) http://localhost:8180/$1
    ProxyPassMatch ^/(.*) http://localhost:8180/https://id.kb.se/$1

    AddOutputFilterByType DEFLATE text/css text/html text/plain text/xml
    AddOutputFilterByType DEFLATE application/x-javascript text/x-component application/javascript
    AddOutputFilterByType DEFLATE application/json application/ld+json
</VirtualHost>
```

Edit `/etc/apache2/ports.conf` and add the following line:

```
Listen 5000
```

Add these lines to `/etc/hosts`:

```
127.0.0.1 kblocalhost.kb.se
127.0.0.1 id.kblocalhost.kb.se
```

Make sure some necessary Apache modules are enabled:

```
a2enmod rewrite proxy proxy_http
```

Now (re)start Apache:

```
systemctl restart apache2
```

You should now able to visit http://id.kblocalhost.kb.se, and use the cataloging client
on http://localhost:8080/katalogisering/. The XL API itself is available on
http://kblocalhost.kb.se:5000 (proxied via Apache), or directly on http://localhost:8180.

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
