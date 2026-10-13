import argparse
import json
import re
import requests
import time
from rapidfuzz import fuzz
import traceback
from urllib.parse import urlparse


THRESHOLD = 0.80


### Search in Libris ###
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
        "computedLabel": "sv",
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


### Analyze search results ###
def analyze_matches(shbd_prepepd, matches: list) -> dict:

    scores_and_matches = []

    for i, match in enumerate(matches):

        match_prepped = prepare_record(match)

        score, partial_scores = get_match_score(shbd_prepepd, match_prepped)

        scores_and_matches.append(
            {
                "api_ranking": i + 1,
                "total_score": score,
                "parital_scores": partial_scores,
                "libris_id": match["@id"],
                "libris_match_record": match_prepped,
            }
        )

    best_match = get_best_match(scores_and_matches, shbd_prepepd["@id"])

    match_summary = {
        "shb_match_record": shbd_prepepd,
        "best_match": best_match,
        "all_matches": scores_and_matches,
    }

    if best_match and best_match["api_ranking"] != 1:
        report.write(
            f"\nMATCHING\t{shbd_prepepd['@id']}\tBest match is not the highest ranked result from the API\t{best_match['libris_id']}\tAPI ranking: {best_match['api_ranking']}\tMatch score: {best_match['total_score']}"
        )

    return match_summary


def get_match_score(shb_prepped: dict, match_prepped: dict):
    weighted_scores = {}
    weights = []
    title_score = 0
    host_or_series_title_score = 0
    contributor_score = 0
    place_score = 0
    year_score = 0
    issn_score = 0
    extent_score = 0
    part_score = 0

    # Ttile should match almost verbatim
    title_score = (
        fuzz.ratio(shb_prepped["full_title"], match_prepped["full_title"]) / 100
    )

    # Host or series title should match almost verbatim
    if shb_prepped.get("host_or_series_title") and match_prepped.get(
        "host_or_series_title"
    ):
        host_or_series_title_score = (
            fuzz.ratio(
                shb_prepped["host_or_series_title"],
                match_prepped["host_or_series_title"],
            )
            / 100
        )

    # Auhtor names are often inverted on one or the other side
    if shb_prepped.get("responsibility_statement") and match_prepped.get(
        "responsibility_statement"
    ):
        contributor_score = (
            fuzz.token_sort_ratio(
                shb_prepped["responsibility_statement"],
                match_prepped["responsibility_statement"],
            )
            / 100
        )

    # Part should have similar content, but not necessarily the same length or order
    if shb_prepped.get("part") and match_prepped.get("part"):
        part_score = (
            fuzz.token_set_ratio(
                shb_prepped["part"],
                match_prepped["part"],
            )
            / 100
        )

    # Place should have similar content, but not necessarily the same length or order
    if shb_prepped.get("place") and match_prepped.get("place"):
        place_score = (
            fuzz.ratio(
                shb_prepped["place"],
                match_prepped["place"],
            )
            / 100
        )

    # Extent should have similar content, but not necessarily the same length or order
    if shb_prepped.get("extent") and match_prepped.get("extent"):
        extent_score = (
            fuzz.token_set_ratio(
                shb_prepped["extent"],
                match_prepped["extent"],
            )
            / 100
        )

    # Year and ISSN (normalized to numeric only) are dealbreakers
    # If they are present and don't match, it's not the same record

    if shb_prepped.get("year") and match_prepped.get("year"):
        if normalize_numeric(shb_prepped["year"]) != normalize_numeric(
            match_prepped["year"]
        ):
            return 0, {"Dealbreaker": "Year present and not a match"}
        else:
            year_score = 1
    else:
        year_score = 0

    if shb_prepped.get("host_or_series_issn") and match_prepped.get(
        "host_or_series_issn"
    ):
        if normalize_numeric(shb_prepped["host_or_series_issn"]) != normalize_numeric(
            match_prepped["host_or_series_issn"]
        ):
            return 0, {"Dealbreaker": "ISSN present and not a match"}
        else:
            issn_score = 1
    else:
        issn_score = 0

    # Calculate the overall score
    ## Properites that describe the thign itself
    weighted_scores["title"] = 0.40 * title_score
    weights.append(0.40)

    weighted_scores["contributor"] = 0.30 * contributor_score
    weights.append(0.30)

    weighted_scores["year"] = 0.125 * year_score
    weights.append(0.125)

    weighted_scores["place"] = 0.075 * place_score
    weights.append(0.075)

    weighted_scores["extent"] = 0.025 * extent_score
    weights.append(0.025)

    ## Properites that describe the thing itself
    weighted_scores["host_or_series_issn"] = 0.025 * issn_score
    weights.append(0.025)

    weighted_scores["host_or_series_title"] = 0.025 * host_or_series_title_score
    weights.append(0.025)

    weighted_scores["part"] = 0.025 * part_score
    weights.append(0.025)

    assert sum(weights) == 1, f"Weights must add up to 1. Actual sum: {sum(weights)}"

    overall_score = sum(weighted_scores.values()) / sum(weights)
    return overall_score, weighted_scores


def get_best_match(scores_and_matches: list, shb_id: str) -> dict | None:

    highest_score = max(m["total_score"] for m in scores_and_matches)
    winners = [m for m in scores_and_matches if m["total_score"] == highest_score]

    if len(winners) > 1:
        report.write(
            f"\nMATCHING\t{shb_id}\tUnable to identify best match: {len(winners)} matches have high score {highest_score}\t{[match['libris_id'] for match in winners]}"
        )
        return None

    return winners[0]


### Prepare records for matching ###
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
        elif "instanceOf" in instance:
            # No hasTitle in instance, only in work - should only happen for serials
            print(instance)
            report.write(
                f"\nDATA ISSUE\t{instance['@id']}\tNo title\t{json.dumps(instance, ensure_ascii=False)}\n"
            )
            return None

        # For some Libris records, we need to get the contributor from the agent entity instead
        responsibility_statement = instance.get("responsibilityStatement", "")

        if not responsibility_statement and instance.get("instanceOf", {}).get(
            "contribution"
        ):
            agent = instance["instanceOf"]["contribution"][0].get("agent", {})
            responsibility_statement = (
                agent.get("familyName", "") + ", " + agent.get("givenName", "")
            )

        prepped["responsibility_statement"] = responsibility_statement

        prepped["extent"] = instance.get("extent", [{}])[0].get("label", [""])[0]

        if publication := instance.get("publication"):
            place = publication[0].get("place")
            if place:
                if isinstance(place, list):
                    prepped["place"] = place[0].get("label", [])[0]
                elif isinstance(place, dict):
                    prepped["place"] = place.get("label", {})
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

        # Get ISSN and title from host instance or work
        if host_or_series:
            if host_has_issn := host_or_series.get("identifiedBy", []):
                prepped["host_or_series_issn"] = host_has_issn[0].get("value", "")

            title = None
            if has_title := host_or_series.get("hasTitle"):
                title = has_title[0].get("mainTitle")

            elif instance_of := host_or_series.get("instanceOf", {}):
                # Get series title from work
                if isinstance(instance_of, dict):
                    has_title = (
                        host_or_series["instanceOf"]
                        .get("hasTitle", [{}])[0]
                        .get("mainTitle")
                    )
                else:
                    report.write(
                        f"\nDATA ISSUE\t{instance['@id']}\tUnexpected instanceOf type\t{json.dumps(instance_of, ensure_ascii=False)}\n"
                    )

            if title:
                prepped["host_or_series_title"] = title

        # Don't try to match if the instance has only one property
        if len(prepped) < 2:
            report.write(
                f"\nDATA ISSUE\t{instance['@id']}\tNot enough properties to match on\t{json.dumps(instance, ensure_ascii=False)}\n"
            )
            return None

    except KeyError:
        report.write(
            f"\nCODE ISSUE\t{instance['@id']}\tKeyError while processing instance: \t{instance}\t{traceback.format_exc()}\n"
        )
    except AttributeError:
        report.write(
            f"\nCODE ISSUE\t{instance['@id']}\tAttributeError while processing instance: \t{instance}\t{traceback.format_exc()}\n"
        )

    return prepped


### Helper function ###


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

def write_instance_to_json_lines(record, file):
        json.dump(record, file, ensure_ascii=False)
        file.write("\n")


### Main action ###
if __name__ == "__main__":

    argp = argparse.ArgumentParser()
    argp.add_argument("env")
    argp.add_argument("shbd_file")
    argp.add_argument("results_folder")
    argp.add_argument("reports_folder")
    argp.add_argument(
        "search_codes",
        choices=["title", "title_and_contributor", "none"],
    )
    args = argp.parse_args()

    start = time.time()
    date = time.strftime("%Y%m%d_%H%M%S")

    matched_shb_path = (
        f"{args.results_folder}/{date}_{args.search_codes}_matched_for_update.jsonl"
    )

    unmatched_shb_path = (
        f"{args.results_folder}/{date}_{args.search_codes}_unmatched_for_create.jsonl"
    )

    search_result_path = (
        f"{args.reports_folder}/{date}_api_search_result_{args.search_codes}.tsv"
    )
    match_map_path = (
        f"{args.reports_folder}/{date}_match_map_{args.search_codes}.json"
    )
    report_path = f"{args.reports_folder}/{date}_report_{args.search_codes}.tsv"

    perfect_matches = []
    match_counts = {}
    match_map = {}
    search_codes = args.search_codes

    env_path = "" if args.env == "prod" else f"-{args.env}"
    base_url = f"http://libris{env_path}.kb.se"

    print(f"Getting started! Matching against records in {base_url}")

    with open(args.shbd_file, "r") as source_file, open(
        matched_shb_path, "w", encoding="utf-8") as matched_shb_file, open(
        unmatched_shb_path, "w", encoding="utf-8") as unmatched_shb_file, open(
        search_result_path, "w") as search_result_file, open(
        match_map_path, "w", encoding="utf-8") as match_map_file, open(
        report_path, "w", encoding="utf-8") as report:
        search_result_file.write("id\tnumber_of_matches\tquery_string\tmatches\n")

        match_summary = {}

        # Loop through the SHB records
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

            shb_graph = json.loads(line)
            shb_instance = shb_graph["@graph"][1]

            shbd_prepepd = prepare_record(shb_instance)

            if shbd_prepepd:
                api_matches = find_matches(shbd_prepepd, match_counts)

            # If the API returns no results, save full SHB graph to file of unmatched
            if not api_matches:
                write_instance_to_json_lines(shb_graph, unmatched_shb_file)
            else:
                match_summary = analyze_matches(shbd_prepepd, api_matches)
                match_map[shbd_prepepd["@id"]] = match_summary

                best_match = match_summary.get("best_match")
                # If there is a best match and the score reaches the threshold, save full SHB graph to file of matched
                if best_match and best_match["total_score"] >= THRESHOLD:
                        libris_short_id = urlparse(best_match["libris_id"]).path.rsplit("/", 1)[-1]
                        match_dict = {
                            libris_short_id: shb_graph,
                        }
                        write_instance_to_json_lines(match_dict, matched_shb_file)

                # If the the score does not reach the threshold, or there is no best match at all,
                # save full SHB graph to file of
                else:
                    write_instance_to_json_lines(shb_graph, unmatched_shb_file)
                        
                    if best_match:
                            report.write(
                            f"\nMATCHING\t{shbd_prepepd['@id']}\tBest match score below threshold {THRESHOLD}\t{best_match['libris_id']}\tMatch score: {best_match['total_score']}"
                        )


        # Finally, print full match maps
        json.dump(match_map, match_map_file, ensure_ascii=False)

    print(f"\nTotal matches:\n{match_counts}")


