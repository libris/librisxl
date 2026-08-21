import argparse
import json
import re
import requests
import time
from rapidfuzz import fuzz
import traceback


# Search in Libris #
def find_matches(shbd_prepepd: dict, match_counts: dict) -> tuple:
    """
    Given an SHBD record, finds matching records already in LIBRIS.
    """
    headers = {"Accept": "application/ld+json"}

    # Title - free text with search code
    if search_codes == "none":
        query_string = f"type:PhysicalResource {remove_problematic_punctuation(shbd_prepepd.get('full_title'))}"
    else:
        query_string = f"type:PhysicalResource title:({remove_problematic_punctuation(shbd_prepepd.get('full_title'))})"

    # Contributors - free text with search code
    if shbd_prepepd.get("responsibility_statement"):
        if search_codes == "title_and_contributor":
            # TODO Possibly complement responsibilityStatement with contributor if needed - after bug preventing search across instance and work fields is fixed
            query_string = f"{query_string} responsibilityStatement:({remove_problematic_punctuation(shbd_prepepd.get('responsibility_statement'))}*)"
        else:
            query_string = f"{query_string} {remove_problematic_punctuation(shbd_prepepd.get('responsibility_statement'))}*"

    # Year - free text
    if shbd_prepepd.get("year"):
        query_string = (
            f"{query_string} {remove_problematic_punctuation(shbd_prepepd.get('year'))}"
        )

    # Series title - free text
    if shbd_prepepd.get("host_or_series_title"):
        query_string = f"{query_string} {remove_problematic_punctuation(shbd_prepepd.get('host_or_series_title'))}"

    # ISSN - free text
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
        else:
            id_with_matches = {}

        search_result_file.write(
            f"{shbd_prepepd['@id']}\t{number_of_matches}\t{query_string}\t{json.dumps(id_with_matches)}\n"
        )
        return matches

    except requests.exceptions.HTTPError as he:
        report.write(f"\n{he}\t{query_string}\n")


# Analyze search results #
def analyze_matches(shbd_prepepd, matches: list, match_map: dict) -> tuple[dict, float]:

    scores_and_matches = []

    for match in matches:

        match_prepped = prepare_record(match)

        score = get_match_score(shbd_prepepd, match_prepped)

        if len(matches) == 1:
            score += 0.5

        scores_and_matches.append(
            {
                "score": score,
                "libris_id": match["@id"],
                "libris_match_record": match_prepped,
            }
        )

    best_match = get_best_match(scores_and_matches, shbd_prepepd["@id"])

    match_map[shbd_prepepd["@id"]] = {
        "shb_match_record": shbd_prepepd,
        "best_match": best_match,
        "all_matches": scores_and_matches,
    }

    return match_map


def get_match_score(shb_prepped: dict, match_prepped: dict):
    title_score = 0
    author_score = 0
    year_score = 0
    host_or_series_issn_score = 0
    host_or_series_title_score = 0

    title_score = (
        fuzz.ratio(shb_prepped["full_title"], match_prepped["full_title"]) / 100
    )

    author_score = (
        fuzz.ratio(
            shb_prepped["responsibility_statement"],
            match_prepped["responsibility_statement"],
        )
        / 100
    )

    if shb_prepped["year"] and match_prepped["year"]:
        year_score = (
            1.0
            if normalize_numeric(shb_prepped["year"])
            == normalize_numeric(match_prepped["year"])
            else -1
        )
    else:
        year_score = -0.5

    if shb_prepped["host_or_series_issn"] and match_prepped["host_or_series_issn"]:
        host_or_series_issn_score = (
            1.0
            if normalize_numeric(shb_prepped["host_or_series_issn"])
            == normalize_numeric(match_prepped["host_or_series_issn"])
            else -1
        )
    else:
        host_or_series_issn_score = -0.5

    host_or_series_title_score = (
        fuzz.ratio(
            shb_prepped["host_or_series_title"], match_prepped["host_or_series_title"]
        )
        / 100
    )

    overall_score = (
        0.6 * title_score
        + 0.6 * author_score
        + 0.3 * year_score
        + 0.3 * host_or_series_issn_score
        + 0.3 * host_or_series_title_score
    )

    return overall_score


def get_best_match(scores_and_matches: list, shb_id: str):

    highest_score = max(m["score"] for m in scores_and_matches)
    winners = [m for m in scores_and_matches if m["score"] == highest_score]

    if len(winners) > 1:
        report.write(
            f"\n{shb_id}\tUnable to identify best match: {len(winners)} matches have high score {highest_score}\t{[match["libris_id"] for match in winners]}"
        )
        return None

    return winners[0]


# Prepare records for matching #
def prepare_record(instance: dict) -> dict:
    try:
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
                f"\n{instance['@id']}\tNo title\t{json.dumps(instance, ensure_ascii=False)}\n"
            )
            return None

        prepped["responsibility_statement"] = instance.get(
            "responsibilityStatement", ""
        )

        prepped["extent"] = instance.get("extent", "")

        if publication := instance.get("publication"):
            if place := publication[0].get("place"):
                prepped["place"] = place[0].get("label", "")
            if year := publication[0].get("year", ""):
                prepped["year"] = year

        if instance.get("isPartOf"):
            host_or_series = instance["isPartOf"][0]
            prepped["part"] = instance.get("part", "")

        elif series := instance.get("seriesMembership"):
            host_or_series = series[0].get("inSeries")
            if part := series[0].get("seriesEnumeration"):
                prepped["part"] = part
        else:
            host_or_series = []

        # Get title from instance or work
        if host_or_series:
            if host_has_issn := host_or_series.get("identifiedBy", []):
                prepped["host_or_series_issn"] = host_has_issn[0].get("value", "")

            title = None
            if has_title := host_or_series.get("hasTitle"):
                title = has_title[0].get("mainTitle")

            elif instance_of := host_or_series.get("instanceOf", {}):
                has_title = instance_of.get("hasTitle", [{}])
                title = has_title[0].get("mainTitle")

            if title:
                prepped["host_or_series_title"] = title

        # Don't try to match if the instance has only one property
        if len(prepped) < 2:
            report.write(
                f"\n{instance['@id']}\tNot enough properties to match on\t{json.dumps(instance, ensure_ascii=False)}\n"
            )
            return None

        return prepped

    except KeyError as ke:
        report.write(f"\nKeyError (details below) while processing instance: \t{instance}\t{traceback.format_exc()}")
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
    argp.add_argument(
        "search_codes",
        choices=["title", "title_and_contributor", "none"],
    )
    args = argp.parse_args()

    start = time.time()

    perfect_matches = []
    match_counts = {}
    match_map = {}
    search_codes = args.search_codes

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
        search_result_file.write("id\tnumber_of_matches\tquery_string\tmatches\n")

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
                analyze_matches(shbd_prepepd, matches, match_map)

        json.dump(match_map, match_map_file, ensure_ascii=False)

    print(f"\nTotal matches:\n{match_counts}")
