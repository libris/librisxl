import argparse
import json
import re
import requests
import time
import sys
import doctest
from rapidfuzz import fuzz


def prepare(entity: dict, match_counts: dict, report) -> dict:
    # Strip ":" to avoid search syntax bug

    prepped = {}

    instance = entity["@graph"][1]
    prepped["@id"] = instance["@id"]
    has_title = instance.get("hasTitle", {})
    prepped["full_title"] = (
        f"{has_title.get('mainTitle', '')} {has_title.get('subtitle', '')}".replace(
        ":", ""
    ).replace("(", "").replace(")", "")

    )
    if instance.get("responsibilityStatement"):
        prepped["responsibility_statement"] = instance.get("responsibilityStatement", "").replace(
        ":", ""
    )

    if instance.get("partOf") or instance.get("seriesMembership"):
        if instance.get("partOf"):
            host_or_series = instance["partOf"]
        elif instance.get("seriesMembership"):
            host_or_series = instance["seriesMembership"]

        host_or_series_title = host_or_series[0].get("hasTitle", {}).get("mainTitle")
        host_or_series_issn = host_or_series[0].get("identifiedBy", {}).get("value")

        if host_or_series_title:
            prepped["host_or_series_title"] = host_or_series_title
        if host_or_series_issn:
            prepped["host_or_series_issn"] = host_or_series_issn

    # Don't try to match if the instance has only one property
    if not has_title or len(prepped) < 2:
        if "insufficient" in match_counts:
            match_counts["insufficient"] += 1
        else:
            match_counts["insufficient"] = 1
        report.write(
            f"{instance['@id']}\tTitle or contributor missing\t{json.dumps(instance, ensure_ascii=False)}\n"
        )
        return None

    return prepped


def compare(shbd: dict, match_record: dict):

    title_score = fuzz.ratio(shbd["title"], match_record["title"]) / 100

    author_score = fuzz.ratio(shbd["name"], match_record["name"]) / 100

    # year_score = 1.0 if shbd["year"] == match_record["year"] else 0.0

    overall_score = 0.6 * title_score + 0.3 * author_score + 0.1  # * year_score

    return overall_score


def normalize(value: str):
    # Remove diacritics -- ??? too radical or useful with the OCR'd data?
    # "".join(
    #    c for c in unicodedata.normalize("NFKD", value) if not unicodedata.combining(c)
    # )

    # Make lowercase
    value = value.lower()

    # Remove punctuation
    value = re.sub(r"[^\w\s]", " ", value)

    # Replace repeated whitespace with single
    value = re.sub(r"\s+", " ", value)

    # Strip leading and trailing spaces
    value = value.strip()

    return value


def find_matches(shbd_prepepd: dict, perfect_matches, match_counts: dict):
    """
    Given an SHBD record, finds matching records already in LIBRIS.
    """
    headers = {"Accept": "application/ld+json"}

    # Match on full title and contributor
    query_string = f"instanceType:PhysicalResource title:({shbd_prepepd.get('full_title')})"

    if shbd_prepepd.get('responsibility_statement'):
        query_string = f"{query_string} contributor:({shbd_prepepd.get('responsibility_statement')}*)"
    if shbd_prepepd.get('host_or_series_title'):
        query_string = f"{query_string} {shbd_prepepd.get('host_or_series_title')}"
    if shbd_prepepd.get('host_or_series_issn'):
        query_string = f"{query_string} {shbd_prepepd.get('host_or_series_issn')}"

    params = {
        "_q": query_string,
        "_lens": "cards",
        "_stats": "false",  # Not needed
        "limit": 50,
    }

    try:
        res = requests.get(f"{base_url}/find?", params=params, headers=headers)
        res.raise_for_status()

        time.sleep(0.001)

        matches = res.json()["items"]

        number_of_matches = len(matches)

        if number_of_matches in match_counts:
            match_counts[number_of_matches] += 1
        else:
            match_counts[number_of_matches] = 1

        if matches: 
            id_with_matches = {
                shbd_prepepd["@id"]: [item["@id"] for item in res.json()["items"]]
            }

            if number_of_matches == 1:
                perfect_matches.append(id_with_matches)

            match_file.write(
                f"{shbd_prepepd['@id']}\t{number_of_matches}\t{query_string}\t{json.dumps(id_with_matches)}\n"
            )
            return matches
    except requests.exceptions.HTTPError as he:
        print(he, query_string)


### Main action ###
if __name__ == "__main__":

    argp = argparse.ArgumentParser()
    argp.add_argument("env")
    argp.add_argument("shbd_file")
    argp.add_argument("match_file")
    argp.add_argument("report")
    args = argp.parse_args()

    start = time.time()

    perfect_matches = []
    match_counts = {}

    env_path = "" if args.env == "prod" else f"-{args.env}"
    base_url = f"http://libris{env_path}.kb.se"

    print(f"Getting started! Matching against records in {base_url}")

    # Create a simple dictionary of minimal match records from Libris
    # with open(args.libris_file) as lf:
    #    libris_prepepd_list: list = []
    #    for line in lf:
    #        libris_instance = json.loads(line)["@graph"]["@graph"][1]
    #        libris_prepepd_list.append(prepare(libris_instance))

    with open(args.shbd_file, "r") as sf, open(
        args.match_file, "w"
    ) as match_file, open(args.report, "w", encoding="utf-8") as report:
        match_file.write("id\tnumber_of_matches\tquery_string\tperfect_matches\n")

        for idx, line in enumerate(sf):

            if idx % 500 == 0:
                match_file.flush()
                report.flush()
                elapsed = idx / (time.time() - start)
                elapsed_formatted = "{0:.4g}".format(elapsed)
                if match_counts:
                    print(
                        f"{idx + 1} records processed\t\t{elapsed_formatted} records/sec."
                    )
                    print(match_counts)

            shbd_prepepd = prepare(json.loads(line), match_counts, report)

            if shbd_prepepd:
                matches = find_matches(shbd_prepepd, perfect_matches, match_counts)

            # for libris_prepped in libris_prepepd_list:
            #    score = compare(libris_prepped, shbd_prepepd)
            #    if score > 0.9:
            #        matches[shbd_instance['@id']] = libris_prepped['@id']
            #
            # if len(matches) == 1:
            #    good_matches.append(good_matches)
            # elif len(matches) > 1:
            #    print(f"Too many matches for {shbd_instance['@id']}: {matches}")
            # else:
            #    print(f"No matches for {shbd_instance['@id']}: {matches}")

    print(f"\nTotal matches:\n{match_counts}")
