"""
Do I need to compare with ALL Libris records, or could I limti to eg physical ones?
"""

import argparse
import json
import unicodedata
import re
from rapidfuzz import fuzz

def prepare(instance: dict) -> dict:

    try: 
        has_title = instance.get("hasTitle", {})
        normalized = {
                "@id": instance["@id"],
                "title": normalize(has_title.get("mainTitle", "")) + normalize(has_title.get("subtitle", "")),
                "name": normalize(instance.get("responsibilityStatement", "")),
            }
        return normalized
    except Exception as e:
        print(f"{instance}\t{e}")
        exit()


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


### Main action ###

argp = argparse.ArgumentParser()
argp.add_argument("libris_file")
argp.add_argument("shbd_file")
args = argp.parse_args()

good_matches = []

# Create a simple dictionary of minimal match records from Libris
with open(args.libris_file) as lf:
    libris_prepepd_list: list = []
    for line in lf:
        libris_instance = json.loads(line)["@graph"]["@graph"][1]
        libris_prepepd_list.append(prepare(libris_instance))

with open(args.shbd_file) as sf:
    for idx, line in enumerate(sf):
        matches = {}

        if idx % 500 == 0:
            print(f"Processing row {idx}")

        shbd_instance = json.loads(line)
        shbd_prepepd = prepare(shbd_instance)

        for libris_prepped in libris_prepepd_list:
            score = compare(libris_prepped, shbd_prepepd)
            if score > 0.9:
                matches[shbd_instance['@id']] = libris_prepped['@id']
        
        if len(matches) == 1:
            good_matches.append(good_matches)
        elif len(matches) > 1:
            print(f"Too many matches for {shbd_instance['@id']}: {matches}")
        else:
            print(f"No matches for {shbd_instance['@id']}: {matches}")

    print(good_matches)