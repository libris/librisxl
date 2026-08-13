import argparse
import json
import re
import requests
import time
import sys
import doctest
from rapidfuzz import fuzz


def prepare(instance: dict) -> dict:
    # Strip ":" to avoid search syntax bug

    prepped = {
        "@id": "",
        "full_title": "",
        "responsibility_statement": "",
        "place": "",
        "year": "",
        "host_or_series_title": "",
        "host_or_series_issn": "",
    }

    prepped["@id"] = instance["@id"]

    if has_title := instance.get("hasTitle", []):
        prepped["full_title"] = (
            f"{has_title[0].get('mainTitle', '')} {has_title[0].get('subtitle', '')}"
        )
    else:
        report.write(
            f"{instance['@id']}\tNo title\t{json.dumps(instance, ensure_ascii=False)}\n"
        )
        return None

    prepped["responsibility_statement"] = instance.get("responsibilityStatement", "")

    prepped["extent"] = instance.get("extent", "")
    prepped["part"] = instance.get("part", "")

    if publication := instance.get("publication"):
        prepped["place"] = publication[0].get("place", [])[0].get("label", "")
        prepped["year"] = publication[0].get("year", "")
        prepped["part"] = instance.get("part", "")

    if instance.get("isPartOf"):
        host_or_series = instance["isPartOf"]
    elif instance.get("seriesMembership"):
        host_or_series = instance["seriesMembership"]
    else:
        host_or_series = []

    if host_or_series:
        prepped["host_or_series_title"] = (
            host_or_series[0].get("hasTitle", {}).get("mainTitle", "")
        )
        prepped["host_or_series_issn"] = (
            host_or_series[0].get("identifiedBy", {}).get("value", "")
        )

    # Don't try to match if the instance has only one property
    if len(prepped) < 2:
        report.write(
            f"{instance['@id']}\tNot enough properties to match on\t{json.dumps(instance, ensure_ascii=False)}\n"
        )
        return None

    return prepped


def analyze_matches(prepped_shb, matches: list) -> tuple[dict, float]:

    scores_and_matches = []

    for match in matches:
        prepped_match = prepare(match["@reverse"]["instanceOf"][0])

        score = compare(prepped_shb, prepped_match)

        scores_and_matches.append({"score": score, "match": match})

    best = get_best_match(scores_and_matches)

    if best:
        return best["score"], best["match"]
    else:
        return None


def compare(prepped_shb: dict, prepped_match: dict):
    title_score = 0
    author_score = 0
    year_score = 0
    host_or_series_issn_score = 0
    host_or_series_title_score = 0

    title_score = fuzz.ratio(prepped_shb["full_title"], prepped_match["full_title"]) / 100

    author_score = fuzz.ratio(prepped_shb["responsibility_statement"], prepped_match["responsibility_statement"]) / 100

    year_score = 1.0 if prepped_shb["year"] == prepped_match["year"] else 0.0

    host_or_series_issn_score = 1.0 if prepped_shb["host_or_series_issn"] == prepped_match["host_or_series_issn"] else 0.0

    host_or_series_title_score = fuzz.ratio(prepped_shb["host_or_series_title"], prepped_match["host_or_series_title"]) / 100

    overall_score = 0.6 * title_score + 0.3 * author_score + 0.3 * year_score + 0.3 * host_or_series_issn_score + 0.3 * host_or_series_title_score

    return overall_score

def get_best_match(matches):

    highest_score = max(m["score"] for m in matches)
    winners = [m for m in matches if m["score"] == highest_score]

    if len(winners) > 1:
        report.write(f"Unable to identify best match: {len(winners)} matches have high score {highest_score}")
        return None

    return winners[0]


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
    query_string = f"instanceType:PhysicalResource title:({remove_problematic_punctuation(shbd_prepepd.get('full_title'))})"

    if shbd_prepepd.get("responsibility_statement"):
        query_string = f"{query_string} contributor:({remove_problematic_punctuation(shbd_prepepd.get('responsibility_statement'))}*)"
    if shbd_prepepd.get("year"):
        query_string = f"{query_string} {remove_problematic_punctuation(shbd_prepepd.get('year'))}"
    if shbd_prepepd.get("host_or_series_title"):
        query_string = f"{query_string} {remove_problematic_punctuation(shbd_prepepd.get('host_or_series_title'))}"
    if shbd_prepepd.get("host_or_series_issn"):
        query_string = f"{query_string} {remove_problematic_punctuation(shbd_prepepd.get('host_or_series_issn'))}"

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
        report.write(f"{he}\t{query_string}\n")


def remove_problematic_punctuation(text: str) -> str:
    # Remove punctuation that might cause a 400 Client Error
    text = text.replace("(", "").replace(")", "").replace('"', "").replace("'", "")

    return text


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

            instance = json.loads(line)["@graph"][1]

            shbd_prepepd = prepare(instance)

            if shbd_prepepd:
                matches = find_matches(shbd_prepepd, perfect_matches, match_counts)

            if matches:
                best_match, score = analyze_matches(shbd_prepepd, matches)

            if match_counts == 1 and score > 0.8:
                report.write(" Good match!")

    print(f"\nTotal matches:\n{match_counts}")
