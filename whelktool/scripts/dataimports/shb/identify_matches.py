"""
Do I need to compare with ALL Libris records, or could I limti to eg physical ones?
"""

import argparse
import json
import unicodedata
import re
import requests
import time

from rapidfuzz import fuzz

def prepare(entity: dict, match_counts: dict) -> dict:

    instance = entity["@graph"]["@graph"][1]
    has_title = instance["hasTitle"]
    responsibility_statement = instance.get("responsibilityStatement")

    if has_title and responsibility_statement:
        prepped = {
            "@id": instance["@id"],
            "full_title": f"{has_title.get("mainTitle", "")} {has_title.get("subtitle", "")}",
            "responsibility_statement": responsibility_statement,
            "part_issn": instance.get("partOf", {}).get("identifiedBy", {}).get("value", "")
            }
        return prepped
    else: 
        if "insufficient" in match_counts:
            match_counts["insufficient"] += 1
        else:
            match_counts["insufficient"] = 1
        report.write(f"{instance["@id"]}\tInsufficient data - matching requires at least title and contributor\n")



def compare(shbd: dict, match_record: dict):

    title_score = (
        fuzz.ratio(shbd["title"], match_record["title"]) / 100
    )

    author_score = (
        fuzz.ratio(shbd["name"], match_record["name"]) / 100
    )

    #year_score = 1.0 if shbd["year"] == match_record["year"] else 0.0

    overall_score = 0.6 * title_score + 0.3 * author_score + 0.1# * year_score

    return overall_score


def normalize(value: str):
    # Remove diacritics -- ??? too radical or useful with the OCR'd data?
    "".join(
        c for c in unicodedata.normalize("NFKD", value) if not unicodedata.combining(c)
    )

    # Make lowercase
    value = value.lower()

    # Remove punctuation
    value = re.sub(r"[^\w\s]", " ", value)

    # Replace repeated whitespace with single
    value = re.sub(r"\s+", " ", value)

    # Strip leading and trailing spaces
    value = value.strip()

    return value


def find_matches(shbd_prepepd: dict, id_map, match_counts: dict):
    '''
    Given an SHBD record, finds matching records already in LIBRIS.
    '''
    headers = {"Accept": "application/ld+json"}

    # Match on full title and contributor
    query_string = f"title:{shbd_prepepd['full_title']} contributor:{shbd_prepepd['responsibility_statement']}* {shbd_prepepd['part_issn']}"

    params = {"_q": query_string,
          "_lens": "cards",
          "limit": 50}

    res = requests.get("http://libris-qa.kb.se/find?", params = params, headers=headers)
    res.raise_for_status()

    matches = res.json()["items"]

    number_of_matches = len(matches)

    if number_of_matches in match_counts:
        match_counts[number_of_matches] += 1
    else:
        match_counts[number_of_matches] = 1

    if matches:
        id_with_matches = {shbd_prepepd["@id"]: [item["@id"] for item in res.json()["items"]]}
        id_map.append(id_with_matches)
        match_file.write(f"{shbd_prepepd["@id"]}\t{number_of_matches}\t{query_string}\t{json.dumps(id_with_matches)}\n")
        return matches

### Main action ###

argp = argparse.ArgumentParser()
#argp.add_argument("libris_file")
argp.add_argument("shbd_file")
argp.add_argument("match_file")
argp.add_argument("report")
args = argp.parse_args()

start = time.time()

id_map = []
match_counts = {}

# Create a simple dictionary of minimal match records from Libris
#with open(args.libris_file) as lf:
#    libris_prepepd_list: list = []
#    for line in lf:
#        libris_instance = json.loads(line)["@graph"]["@graph"][1]
#        libris_prepepd_list.append(prepare(libris_instance))

with open(args.shbd_file, "r") as sf, open(args.match_file, "w") as match_file, open(args.report, "w") as report:
    match_file.write("number_of_matches\tquery_string\tid_map\n")


    for idx, line in enumerate(sf):
        
        if idx % 100 == 0:
            match_file.flush()
            report.flush()
            elapsed = idx / (time.time() - start)
            elapsed_formatted = "{0:.4g}".format(elapsed)
            if match_counts:
                print(f"{idx + 1} records processed\t\t{elapsed_formatted} records/sec."
            )
                print(match_counts)


        shbd_prepepd = prepare(json.loads(line), match_counts)

        if shbd_prepepd:
            matches = find_matches(shbd_prepepd, id_map, match_counts)

        #for libris_prepped in libris_prepepd_list:
        #    score = compare(libris_prepped, shbd_prepepd)
        #    if score > 0.9:
        #        matches[shbd_instance['@id']] = libris_prepped['@id']
        #
        #if len(matches) == 1:
        #    good_matches.append(good_matches)
        #elif len(matches) > 1:
        #    print(f"Too many matches for {shbd_instance['@id']}: {matches}")
        #else:
        #    print(f"No matches for {shbd_instance['@id']}: {matches}")

print(f"\nMatches:\n{match_counts}")