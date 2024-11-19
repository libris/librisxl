# Standard library only, 3rd party dependencies are not permitted.
import sqlite3
import urllib.request
from urllib.request import Request, urlopen
import json
import os

# Configuration section:
cache_location = "./libris-cache.sqlite3"
libris_emm_base_url = "http://localhost:8182/"
local_library_code = "S"
properties_of_interest = ["itemOf", "instanceOf", "agent", "subject"]

def collect_entity(data, uri):
    return {}


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


def download_entity(url):
    req = Request(url)
    req.add_header('accept', 'application/json+ld')
    return json.load(urlopen(req))["@graph"][1]


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


def embellish(entity, connection):
    if isinstance(entity, dict):
        for key in entity:
            if key in properties_of_interest:
                embed_links(entity[key], connection)
            embellish(entity[key], connection)
    elif isinstance(entity, list):
        for item in entity:
            embellish(item, connection)


def ingest_entity(entity, connection):
    uris = collect_uris_with_data(entity)
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


def load_dump(connection):
    next_url = f"{libris_emm_base_url}?dump=itemAndInstance:{local_library_code}&offset=0"
    dumpCreationTime = None
    while next_url:
        with urllib.request.urlopen(next_url) as response:
            data = json.load(response)
            dumpCreationTimeOnPage = data["creationTime"]
            if (dumpCreationTime and dumpCreationTime != dumpCreationTimeOnPage):
                print(" DUMP INVALIDATED WHILE DOWNLOADING, TODO: DEAL WITH THIS ")
            dumpCreationTime = dumpCreationTimeOnPage
            if "next" in data:
                next_url = data["next"]
            else:
                next_url = None
            if "entities" in data:
                for entity in data["entities"]:
                    ingest_entity(entity, connection)
    cursor = connection.cursor()
    cursor.execute(
        """
    INSERT INTO
        state(changes_consumed_until)
    VALUES
        (?)
    """,
        (dumpCreationTime,)
    )
    connection.commit()


def download_entity(url):
    req = Request(url)
    req.add_header('accept', 'application/json+ld')
    return json.load(urlopen(req))["@graph"][1]


def replace_subentity(node, replacement_entity):
    uri_to_replace = replacement_entity["@id"]
    if isinstance(node, dict):
        for key in node.keys:
            if isinstance(node[key], dict) and "@id" in node[key] and node[key]["@id"] == uri_to_replace:
                node[key] = replacement_entity
            replace_subentity(node[key], replacement_entity)
    elif isinstance(node, list):
        for i in range(len(node)):
            if isinstance(node[i], dict) and "@id" in node[i] and node[i]["@id"] == uri_to_replace:
                node[i] = replacement_entity
            replace_subentity(node[i], replacement_entity)


def replace_entity(node, replacement_entity):
    uri_to_replace = replacement_entity["@id"]
    if isinstance(node, dict) and "@id" in node and node["@id"] == uri_to_replace: # Root-replacement
        return replacement_entity
    else: # Embedded replacement
        replace_subentity(node, replacement_entity)
    return node


def handle_activity(connection, activity):
    cursor = connection.cursor()
    if activity["type"] == "create":
        pass # TODO
    elif activity["type"] == "delete":
        pass # TODO
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
                    print(f"{item["published"]} is before our last taken update, stop here.")
                    next_url = None
                    break
                handle_activity(connection, item)
                # Disable for dev, temporary
                #cursor.execute("UPDATE state SET changes_consumed_until = ?", (item["published"],))
                #connection.commit()


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
    FOREIGN KEY (entity_id) REFERENCES entities(id)
);
""")
        cursor.execute("""
CREATE TABLE state (
    id INTEGER PRIMARY KEY,
    changes_consumed_until TEXT
);
""")
        connection.commit()
        load_dump(connection)

    update(connection)

        

if __name__ == "__main__":
    main()
