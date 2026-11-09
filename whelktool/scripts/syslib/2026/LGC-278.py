import json
import requests
from urllib.parse import urlparse
from datetime import datetime

# --- Settings ---
CLIENT_ID = "cf0351447ff1e59cd6c6a7b7e417dd28"          # your client ID
CLIENT_SECRET = "83b6c919c673a29738dacb8a6016dcb44ea6597694f016b2de60e2d74a659a4fbdb15f801ec04fa86cf1ed7687cac07bd71fc21a3f99485b537c12be44574b549f564949ce5016ba937105632b4e85833379b9f63ce88fac1c55aad41644233e6cf79fdb43e8355a76ec3a9d9f340a47a9db83479290fe4e5500ab5f7e45ab03"      # your client secret
SIGEL = "SEK"
INFILE = r"c:\script\LGC\LGC-278\LGC-278.txt"   # one bib ID or URL per line
BASE = "https://libris.kb.se"
DRY_RUN = False             # run with True first, check the output, then set False

MY_LIBRARY = f"https://libris.kb.se/library/Sko"

# --- Auth ---
resp = requests.post(
    "https://login.libris.kb.se/oauth/token",
    data={
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials",
    },
    headers={"Content-Type": "application/x-www-form-urlencoded"},
)
resp.raise_for_status()
TOKEN = resp.json()["access_token"]

auth_headers = {
    "Authorization": f"Bearer {TOKEN}",
    "XL-Active-Sigel": SIGEL,
}

GENERATION_PROCESS = "https://github.com/libris/librisxl/tree/develop/whelktool/scripts/syslib/2026/LGC-278.py"

def stamp_generation(data):
    record = data["@graph"][0]
    record["generationProcess"] = {"@id": GENERATION_PROCESS}
    record["generationDate"] = datetime.now().astimezone().isoformat(timespec="milliseconds")

# --- Helpers ---
def extract_record_id(line):
    """Bare ID or full URL (with or without #it) -> bare ID."""
    line = line.strip()
    if not line:
        return None
    if line.startswith(("http://", "https://")):
        return urlparse(line).path.rstrip("/").split("/")[-1]
    return line


def as_list(v):
    return v if isinstance(v, list) else [v]


def get_main_entity(data):
    """Return the node that the Record's mainEntity points to."""
    graph = data.get("@graph", [])
    if not graph or graph[0].get("@type") != "Record":
        return None
    main_id = graph[0].get("mainEntity", {}).get("@id")
    return next((n for n in graph[1:] if n.get("@id") == main_id), None)


def fetch(record_url):
    """GET a bare record. Returns (data, etag), or (None, None) on failure."""
    r = requests.get(
        record_url,
        params={"embellished": "false"},
        headers={**auth_headers, "Accept": "application/ld+json"},
    )
    if r.status_code >= 400:
        print(f"  GET {record_url} failed {r.status_code}: {r.text}")
        return None, None
    etag = r.headers.get("ETag")
    if not etag:
        print(f"  GET {record_url}: no ETag, skipping")
        return None, None
    return r.json(), etag


def save(record_url, data, etag):
    """PUT a record back. Returns True on success (204)."""
    stamp_generation(data)
    r = requests.put(
        record_url,
        headers={
            **auth_headers,
            "Content-Type": "application/ld+json",
            "If-Match": etag,
        },
        data=json.dumps(data, ensure_ascii=False).encode("utf-8"),
    )
    print(f"  PUT {record_url}: {r.status_code}")
    if r.status_code == 409:
        print("    -> Conflict: record changed since GET, re-run for this ID")
    elif r.status_code >= 400:
        print(f"    -> {r.text}")
    return r.status_code < 400


def holding_item_ids(bib_data):
    """
    Item @ids of holdings on this instance. Uses @reverse.itemOf if the
    GET included it, otherwise asks the search API.
    """
    instance = get_main_entity(bib_data)
    refs = as_list(instance.get("@reverse", {}).get("itemOf", []))
    ids = [r["@id"] for r in refs if "@id" in r]
    if ids:
        return ids

    r = requests.get(
        f"{BASE}/find.jsonld",
        params={"q": "*", "itemOf.@id": instance["@id"], "_limit": 200},
        headers={"Accept": "application/ld+json"},
    )
    if not r.ok:
        print(f"  holding search failed {r.status_code}: {r.text}")
        return []
    return [i["@id"] for i in r.json().get("items", []) if "@id" in i]


def find_my_holding(bib_data):
    """
    Return (url, data, etag) for this library's single holding,
    or None if there are zero or several.
    """
    mine = []
    for item_id in holding_item_ids(bib_data):
        url = item_id.split("#")[0]
        data, etag = fetch(url)
        if data is None:
            continue
        item = get_main_entity(data)
        if item and item.get("heldBy", {}).get("@id") == MY_LIBRARY:
            mine.append((url, data, etag))
    if len(mine) != 1:
        print(f"  found {len(mine)} {SIGEL} holdings, skipping")
        return None
    return mine[0]


def copy_notes_to_item(instance, item):
    """Copy the instance's hasNote onto the Item. Returns the notes copied."""
    moved = as_list(instance.get("hasNote", []))
    notes = as_list(item.get("hasNote", []))
    for n in moved:
        if n not in notes:          # dict equality avoids duplicates
            notes.append(n)
    item["hasNote"] = notes
    return moved


def remove_notes(instance, moved):
    """Remove exactly the moved notes from the instance."""
    remaining = [n for n in as_list(instance.get("hasNote", [])) if n not in moved]
    if remaining:
        instance["hasNote"] = remaining
    else:
        instance.pop("hasNote", None)
    # computed data, never send back
    instance.pop("@reverse", None)
    instance.pop("_categoryByCollection", None)


# --- Main loop ---
with open(INFILE, "r", encoding="utf-8") as f:
    for line_num, line in enumerate(f, start=1):
        record_id = extract_record_id(line)
        if not record_id:
            continue
        print(f"Line {line_num} ({record_id})")
        bib_url = f"{BASE}/{record_id}"

        # 1. Bib: does it have a note to move?
        bib, bib_etag = fetch(bib_url)
        if bib is None:
            continue
        instance = get_main_entity(bib)
        if instance is None or "hasNote" not in instance:
            print("  no hasNote on instance, skipping")
            continue

        # 2. Find our holding
        found = find_my_holding(bib)
        if found is None:
            continue
        holding_url, holding, holding_etag = found
        item = get_main_entity(holding)

        if item.get("itemOf", {}).get("@id") != instance.get("@id"):
            print(f"  holding itemOf {item.get('itemOf')} != {instance.get('@id')}, skipping")
            continue

        # 3. Copy the notes into the holding
        moved = copy_notes_to_item(instance, item)

        if DRY_RUN:
            remove_notes(instance, moved)
            print("  would PUT holding:")
            print(json.dumps(holding, indent=2, ensure_ascii=False))
            print("  would PUT bib:")
            print(json.dumps(bib, indent=2, ensure_ascii=False))
            continue

        # 4. Save holding FIRST, so the note is never lost
        if not save(holding_url, holding, holding_etag):
            print("  holding not saved, bib left untouched")
            continue

        # 5. Re-fetch bib for a fresh ETag, then remove the notes
        bib, bib_etag = fetch(bib_url)
        if bib is None:
            print("  !! note is now on BOTH holding and bib, fix bib manually")
            continue
        remove_notes(get_main_entity(bib), moved)
        if not save(bib_url, bib, bib_etag):
            print("  !! note is now on BOTH holding and bib, fix bib manually")