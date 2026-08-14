import argparse
import json
import re
import requests
import time
from rapidfuzz import fuzz


# Search in Libris #
def find_matches(shbd_prepepd: dict, match_counts: dict) -> tuple:
    """
    Given an SHBD record, finds matching records already in LIBRIS.
    """
    headers = {"Accept": "application/ld+json"}

    # Match on full title and contributor
    query_string = f"type:PhysicalResource title:({remove_problematic_punctuation(shbd_prepepd.get('full_title'))})"

    if shbd_prepepd.get("responsibility_statement"):
        # TODO Possibly complement with contributor if needed - after bug preventing search across instance and work fields is fixed
        query_string = f"{query_string} responsibilityStatement:({remove_problematic_punctuation(shbd_prepepd.get('responsibility_statement'))}*)"
    if shbd_prepepd.get("year"):
        query_string = (
            f"{query_string} {remove_problematic_punctuation(shbd_prepepd.get('year'))}"
        )
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

        # Add stats about number of matches
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

            search_result_file.write(
                f"{shbd_prepepd['@id']}\t{number_of_matches}\t{query_string}\t{json.dumps(id_with_matches)}\n"
            )
            return matches
    except requests.exceptions.HTTPError as he:
        report.write(f"{he}\t{query_string}\n")


# Analyze search results #
def analyze_matches(prepped_shb, matches: list, match_map: dict) -> tuple[dict, float]:

    scores_and_matches = []

    for match in matches:
        prepped_match = prepare_record(match)

        score = get_match_score(prepped_shb, prepped_match)

        scores_and_matches.append({"score": score, "id": match["@id"], "record": match})

    best_match = get_best_match(scores_and_matches)

    match_map[shbd_prepepd["@id"]] = {
        "best_match": best_match,
        "all_matches": scores_and_matches,
    }

    if best_match:
        return best_match["score"], best_match["id"]
    else:
        return None


def get_match_score(prepped_shb: dict, prepped_match: dict):
    title_score = 0
    author_score = 0
    year_score = 0
    host_or_series_issn_score = 0
    host_or_series_title_score = 0

    title_score = (
        fuzz.ratio(prepped_shb["full_title"], prepped_match["full_title"]) / 100
    )

    author_score = (
        fuzz.ratio(
            prepped_shb["responsibility_statement"],
            prepped_match["responsibility_statement"],
        )
        / 100
    )

    if prepped_shb["year"] and prepped_match["year"]:
        year_score = (
            1.0
            if normalize_numeric(prepped_shb["year"])
            == normalize_numeric(prepped_match["year"])
            else -1
        )
    else:
        year_score = -0.5

    if prepped_shb["host_or_series_issn"] and prepped_match["host_or_series_issn"]:
        host_or_series_issn_score = (
            1.0
            if normalize_numeric(prepped_shb["host_or_series_issn"])
            == normalize_numeric(prepped_match["host_or_series_issn"])
            else -1
        )
    else:
        host_or_series_issn_score = -0.5

    host_or_series_title_score = (
        fuzz.ratio(
            prepped_shb["host_or_series_title"], prepped_match["host_or_series_title"]
        )
        / 100
    )

    overall_score = (
        0.6 * title_score
        + 0.3 * author_score
        + 0.3 * year_score
        + 0.3 * host_or_series_issn_score
        + 0.3 * host_or_series_title_score
    )

    return overall_score


def get_best_match(scores_and_matches):

    highest_score = max(m["score"] for m in scores_and_matches)
    winners = [m for m in scores_and_matches if m["score"] == highest_score]

    if len(winners) > 1:
        report.write(
            f"Unable to identify best match: {len(winners)} matches have high score {highest_score}"
        )
        return None

    return winners[0]


# Prepare records for matching #
def prepare_record(instance: dict) -> dict:
    # Strip ":" to avoid search syntax bug

    prepped = {
        "@id": instance["@id"],
        "full_title": "",
        "responsibility_statement": "",
        "place": "",
        "year": "",
        "host_or_series_title": "",
        "host_or_series_issn": "",
    }

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
        if place := publication[0].get("place"):
            prepped["place"] =place[0].get("label", "")
        if year := publication[0].get("year", ""):
            prepped["year"] = year
        if part := publication[0].get("part"):
            prepped["part"] = part

    if instance.get("isPartOf"):
        host_or_series = instance["isPartOf"]
    elif instance.get("seriesMembership"):
        host_or_series = instance["seriesMembership"]
    else:
        host_or_series = []

    if host_or_series:
        if host_has_title := host_or_series[0].get("hasTitle", []):
            prepped["host_or_series_title"] = host_has_title[0].get("mainTitle", "")
        if host_has_issn :=  host_or_series[0].get("identifiedBy", []):
            prepped["host_or_series_issn"] = host_has_issn[0].get("value", "")
        

    # Don't try to match if the instance has only one property
    if len(prepped) < 2:
        report.write(
            f"{instance['@id']}\tNot enough properties to match on\t{json.dumps(instance, ensure_ascii=False)}\n"
        )
        return None

    return prepped


def normalize_text(value: str):
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


def normalize_numeric(year):
    if not year:
        return None
    return re.sub(r"\D", "", str(year))


def remove_problematic_punctuation(text: str) -> str:
    # Remove punctuation that might cause a 400 Client Error
    text = text.replace("(", "").replace(")", "").replace('"', "").replace("'", "")

    return text


### Main action ###
if __name__ == "__main__":

    argp = argparse.ArgumentParser()
    argp.add_argument("env")
    argp.add_argument("shbd_file")
    argp.add_argument("search_result_file")
    argp.add_argument("match_map_file")
    argp.add_argument("report")
    args = argp.parse_args()

    start = time.time()

    perfect_matches = []
    match_counts = {}
    match_map = {}

    env_path = "" if args.env == "prod" else f"-{args.env}"
    base_url = f"http://libris{env_path}.kb.se"

    print(f"Getting started! Matching against records in {base_url}")

    with open(args.shbd_file, "r") as source_file, open(
        args.search_result_file, "w"
    ) as search_result_file, open(
        args.match_map_file, "w", encoding="utf-8"
    ) as match_map_file, open(
        args.report, "w", encoding="utf-8"
    ) as report:
        search_result_file.write(
            "id\tnumber_of_matches\tquery_string\tperfect_matches\n"
        )

        for idx, line in enumerate(source_file):

            if idx % 500 == 0:
                search_result_file.flush()
                report.flush()
                elapsed = idx / (time.time() - start)
                elapsed_formatted = "{0:.4g}".format(elapsed)
                if match_counts:
                    print(
                        f"{idx + 1} records processed\t\t{elapsed_formatted} records/sec."
                    )
                    print(match_counts)

            instance = json.loads(line)["@graph"][1]

            shbd_prepepd = prepare_record(instance)

            if shbd_prepepd:
                matches = find_matches(shbd_prepepd, match_counts)

            if matches:
                best_match, score = analyze_matches(shbd_prepepd, matches, match_map)

            if match_counts == 1 and score > 0.8:
                report.write(" Good match!")

    json.dump(match_map, match_map_file, ensure_ascii=False)

    print(f"\nTotal matches:\n{match_counts}")
