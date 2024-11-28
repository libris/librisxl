#
# This example Libris EMM client is intended to illustrate how one can use
# the EMM protocol to obtain a cache of some subset of libris data and keep
# that cache up to date over time. By default this example client caches
# data for one selected libris library (see the configuration section below).
# But the selected subset might as well be "all agents", "all everything"
# or something else.
#
# Beware: This client has not been written with defensive coding in mind,
# and is not intended for production use.
#
# Key to using the EMM protocol (at least as seen here), is to define what
# you consider to be the root set of entities to cache. For us this is
# going to mean:
# Entities (I) of type 'Item' with a library code (I->helBy->@id) identifying
# our selected library.
#
# Knowing this, we will consume a dump of this set of entities (the initial
# load).
# We are then going to consume/monitor the stream of all changes in Libris,
# and for every activity in that stream, we will consider the following:
# 1. Is this a creation or a deletion of an entity that fits our root entity
#    definition?
#    If so, we must add or remove said entity from our collection.
# 2. Is this an update of an entity that we have (be it as a root, or
#    embedded within another entity, or somewhere else)?
#    If so, we must update that entity wherever we keep it.
#
# This is all we need to do to keep our cache up date in perpetuity.
# 
# For this example client we are going to keep knowledge graphs in an SQLITE
# table called 'entities'. These knowledge graphs will always consist of an
# entity of type 'Item' with various other entities that are being linked to
# embedded inside that root entity.
# 
# We will also have a table called 'uris' which will provide a mapping from
# entity IDs (URIs) to all of our knowledge graphs that reference that URI
# (which means they also keep an embedded copy of the referenced entity).
#
# Finally we will have a single-value table called 'state', which will hold
# the latest update time from the server that we have already consumed.
#


#
# Configuration section
#

# This parameter tells the client where on disc, to store its cache.
cache_location = "./libris-cache.sqlite3"

# This parameter tells the client which EMM server to use.
libris_emm_base_url = "http://localhost:8182/"

# This parameter tells the client which library we are downloading data for.
local_library_code = "S"

# This parameter tells the client which properties we would like to follow
# links for and download additional data to keep with our entities.
properties_of_interest = ["itemOf", "instanceOf", "agent", "subject"]

# This parameter decides whether or not the client terminates after reaching
# the point where the cache is up to date. With this set to true, it instead
# runs in a loop, keeping up to date over time.
continous_mode = False

# This parameter tells the client to output entities as they are received or
# updated. If set to true, the output will be jsonld-lines, one root entity
# with all things it depends on, on each line, when the combined graph is
# changed. This is for when this client is used as a proxy or pipe to assemble
# embedded views of data for another system, WHICH IS NOT RECOMMENDED.
data_on_stdout = False

#
# Code section
#
# This client was written for python 3.13.0, but it is believed that many
# python 3 implementations can run this code.
# 
# It uses only the Python standard library. There are no 3rd party 
# dependencies.
#
import sqlite3
import urllib.request
from urllib.request import Request, urlopen
import json
import os
import time
import sys


#
# For a given entity, return all URIs within that entity (embedded or not)
# for which we have any data.
#
def collect_uris_with_data(entity):
    uris = []
    if isinstance(entity, dict):
        for key in entity:
            if key == "@id" and len(entity) > 1:
                uris.append(entity[key])
            uris = uris + collect_uris_with_data(entity[key])
    elif isinstance(entity, list):
        for item in entity:
            uris = uris + collect_uris_with_data(item)
    return uris


#
# For a given entity and uri, return any embedded entities identified by the
# uri.
#
def collect_entity_with_uri(entity, uri):
    if isinstance(entity, dict):
        if "@id" in entity and len(entity) > 1 and entity["@id"] == uri:
            return entity
        for key in entity:
            result = collect_entity_with_uri(entity[key], uri)
            if result:
                return result
    elif isinstance(entity, list):
        for item in entity:
            result = collect_entity_with_uri(item, uri)
            if result:
                return result
    return None


#
# Attempt to download and return data for a given URI.
#
def download_entity(url):
    #print(f"Requesting external resource: {url}")
    req = Request(url)
    req.add_header('accept', 'application/json+ld')
    try:
        return get_main_entity(json.load(urlopen(req)))
    except:
        return None


#
# For a given entity, iff that entity is a link (a lone @id-property and
# nothing else) attempt to find data on that URI, and embed that data into
# this entity.
#
def embed_links(entity, connection):
    if isinstance(entity, dict) and len(entity) == 1 and "@id" in entity:
        # Find data for this ID, somewhere and replace the stuff in our entity
        # First see if we have more of this data somewhere already
        cursor = connection.cursor()
        rows = cursor.execute("SELECT entities.entity FROM uris JOIN entities on uris.entity_id = entities.id WHERE uris.uri = ?", (entity["@id"],))
        row = rows.fetchone()
        if row:
            whole_other_record = json.loads(row[0])
            sought_entity = collect_entity_with_uri(whole_other_record, entity["@id"])
            entity.clear()
            entity.update(sought_entity)
        # Otherwise do a GET on the ID try to get some data from there
        else:
            sought_entity = download_entity(entity["@id"])
            if sought_entity:
                entity.clear()
                entity.update(sought_entity)
    elif isinstance(entity, list):
        for item in entity:
            embed_links(item, connection)


#
# For a given entity, check if we can get more data on the things it links
# to (that we are interested in - see the configuration section), and embed
# copies of that data.
#
def embellish(entity, connection):
    if isinstance(entity, dict):
        for key in entity:
            if key in properties_of_interest:
                embed_links(entity[key], connection)
            if not key == "@reverse":
                embellish(entity[key], connection)
    elif isinstance(entity, list):
        for item in entity:
            embellish(item, connection)


#
# Given an entity (that's been changed), update the mappings of URIs to
# this entity.
#
def update_uris_table(entity, entity_id, connection):
    cursor = connection.cursor()
    uris = collect_uris_with_data(entity)
    cursor.execute("DELETE FROM uris WHERE entity_id = ?", (entity_id,))
    for uri in uris:
        cursor.execute(
            """
        INSERT INTO
            uris(entity_id, uri)
        VALUES
            (?, ?)
        """,
            (entity_id, uri,)
        )
    connection.commit()


#
# Ingest a root entity from a dump.
#
def ingest_entity(entity, connection):
    cursor = connection.cursor()
    entity_id = cursor.execute(
        """
    INSERT INTO
        entities(entity)
    VALUES
        (?)
    """,
        (json.dumps(entity),)
    ).lastrowid
    connection.commit()
    update_uris_table(entity, entity_id, connection)
    if data_on_stdout:
        print(json.dumps(entity))


#
# Load an itial dump of the configured data set.
#
def load_dump(connection):
    next_url = f"{libris_emm_base_url}full?selection=itemAndInstance:{local_library_code}&offset=0"
    dump_creation_time = None
    items_so_far = 0
    while next_url:
        with urllib.request.urlopen(next_url) as response:
            data = json.load(response)
            dump_creation_time_on_page = data["startTime"]
            if data["totalItems"]:
                print(f"\rLoading initial dump itemAndInstance:{local_library_code}, currently at {(items_so_far / data["totalItems"]):.0%}", file=sys.stderr, end="")

            if (dump_creation_time and dump_creation_time != dump_creation_time_on_page):
                print(" DUMP INVALIDATED WHILE DOWNLOADING, TODO: DEAL WITH THIS ")
            dump_creation_time = dump_creation_time_on_page
            if "next" in data:
                next_url = data["next"]
            else:
                next_url = None
            if "items" in data:
                for item in data["items"]:
                    if "@graph" not in item: # skip @context
                        continue

                    entity = get_main_entity(item)
                    embellish(entity, connection)
                    ingest_entity(entity, connection)
                    items_so_far = items_so_far + 1
    print("\rLoaded initial dump itemAndInstance:{local_library_code}. Now done.", file=sys.stderr)
    cursor = connection.cursor()
    cursor.execute(
        """
    INSERT INTO
        state(changes_consumed_until)
    VALUES
        (?)
    """,
        (dump_creation_time,)
    )
    connection.commit()


def get_main_entity(named_graph):
    # FIXME? relying on XL convention @graph[0] = Record, @graph[1] = Main entity
    return named_graph["@graph"][1]


#
# Given a root entity 'r', and a replacement/update of some embedded entity 'u',
# update the data of 'u' wherever it is copied/embedded into 'r'.
#
def replace_subentity(node, replacement_entity):
    uri_to_replace = replacement_entity["@id"]
    if isinstance(node, dict):
        for key in node:
            if isinstance(node[key], dict) and "@id" in node[key] and node[key]["@id"] == uri_to_replace:
                node[key] = replacement_entity
            replace_subentity(node[key], replacement_entity)
    elif isinstance(node, list):
        for i in range(len(node)):
            if isinstance(node[i], dict) and "@id" in node[i] and node[i]["@id"] == uri_to_replace:
                node[i] = replacement_entity
            replace_subentity(node[i], replacement_entity)


#
# Given a root entity 'r', and a replacement/update of some embedded entity 'u',
# update the data of 'u' wherever it is copied/embedded into 'r'.
#
def replace_entity(node, replacement_entity):
    uri_to_replace = replacement_entity["@id"]
    if isinstance(node, dict) and "@id" in node and node["@id"] == uri_to_replace: # Root-replacement
        return replacement_entity
    else: # Embedded replacement
        replace_subentity(node, replacement_entity)
    return node


#
# Return True if 'entity' matches our root-entity criteria, otherwise False
#
def is_root_entity(entity):
    if entity == None:
        return False
    if "@type" in entity and entity["@type"] == 'Item':
        if "heldBy" in entity and isinstance(entity["heldBy"], dict) and "@id" in entity["heldBy"]:
            return entity["heldBy"]["@id"].endswith(local_library_code)
    return False


#
# This is the heart of the update mechanism. Consume an update activity, and take
# whatever action is necesseray to keep our cache up to date.
#
def handle_activity(connection, activity):
    cursor = connection.cursor()

    if activity["type"] == "create":
        created_data = download_entity(activity["object"]["id"])
        if is_root_entity(created_data):
            embellish(created_data, connection)
            ingest_entity(created_data, connection)

    elif activity["type"] == "delete":
        if data_on_stdout:
            rows = cursor.execute("SELECT entities.entity -> '@id' FROM entities JOIN uris ON uris.entity_id = entities.id WHERE uri = ?", (activity["object"]["id"],))
            for row in rows:
                deleted_entity = {"@id": row[0].strip('\"'), "status": "deleted"}
                print(f"{json.dumps(deleted_entity)}")

        # This is a "cascading delete", but doing so is safe as long as libris
        # maintains its principle that linked records cannot be deleted.
        cursor.execute("DELETE FROM uris WHERE uri = ?", (activity["object"]["id"],))
        connection.commit()

    elif activity["type"] == "update":
        # Find all of our records that depend on this URI
        rows = cursor.execute("SELECT entities.id, entities.entity FROM uris JOIN entities on uris.entity_id = entities.id WHERE uris.uri = ?", (activity["object"]["id"],))
        
        updated_data = None
        for row in rows:
            if not updated_data:
                updated_data = download_entity(activity["object"]["id"])
            entity_id = row[0]
            entity_data = json.loads(row[1])
            entity_data = replace_entity(entity_data, updated_data)
            embellish(entity_data, connection)
            cursor.execute("UPDATE entities SET entity = ? WHERE id = ?", (json.dumps(entity_data),entity_id))
            connection.commit()
            update_uris_table(entity_data, entity_id, connection)
            if data_on_stdout:
                print(json.dumps(entity_data))



#
# Scan for new updates to consume.
#
def update(connection):
    cursor = connection.cursor()
    with urllib.request.urlopen(libris_emm_base_url) as response:
        data = json.load(response)
        next_url = data["first"]["id"]
    
    while next_url:
        with urllib.request.urlopen(next_url) as response:
            data = json.load(response)
            if "next" in data:
                next_url = data["next"]
            else:
                next_url = None
            for item in data["orderedItems"]:
                result = cursor.execute("SELECT julianday(changes_consumed_until) - julianday(?) FROM state", (item["published"],))
                diff = result.fetchone()[0]
                if (float(diff) >= 0.0):
                    print(f"{item['published']} is before our last taken update, cache is now up to date.", file=sys.stderr)
                    next_url = None
                    break
                handle_activity(connection, item)
                cursor.execute("UPDATE state SET changes_consumed_until = ?", (item["published"],))
                connection.commit()


def main():
    cache_initialized = False
    if os.path.exists(cache_location):
        cache_initialized = True
    connection = sqlite3.connect(cache_location)
    cursor = connection.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=OFF")
    cursor.execute("PRAGMA foreign_keys=ON")

    # If the cache database does not exist, set up a new one
    if not cache_initialized:
        cursor.execute("""
CREATE TABLE entities (
    id INTEGER PRIMARY KEY,
    entity TEXT
);
""")
        cursor.execute("""
CREATE TABLE uris (
    id INTEGER PRIMARY KEY,
    uri TEXT,
    entity_id INTEGER,
    UNIQUE(uri, entity_id) ON CONFLICT IGNORE,
    FOREIGN KEY (entity_id) REFERENCES entities(id) ON DELETE CASCADE
);
""")
        cursor.execute("""
CREATE TABLE state (
    id INTEGER PRIMARY KEY,
    changes_consumed_until TEXT
);
""")
        cursor.execute("CREATE INDEX idx_uris_uri ON uris(uri);")
        cursor.execute("CREATE INDEX idx_uris_entity_id ON uris(entity_id);")
        connection.commit()
        load_dump(connection)

    update(connection)

    if continous_mode:
        while True:
            time.sleep(5)
            update(connection)

        

if __name__ == "__main__":
    main()
