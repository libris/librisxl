from io import TextIOBase
import csv
import json
import re
import sys
from pathlib import Path
from collections import Counter

# Formatting of isPartOf: https://metadatabyran.kb.se/arbetsfloden/bidrag/instans---bidrag#h-Vardpublikationsomlokalentitet

USE_ANNOT = False

SYNTAX_ERAS = {
    "1771-1874": "early",
    "1875-1900": "early",
    "1901-1920": "early",
    "1921-1935": "parenthesized",  # Serietillhörighet och källpublikation anges inom parentes. Sidor anges efter Ort/år
    "1936-1950": "parenthesized",  # -||- . -||-
    "1951-1960": "parenthesized",  # Serietillhörighet och källpublikation anges inom parentes. Sidor anges före Ort/år
    "1961-1970": "dash_style",  # Serietillhörighet och källpublikation anges efter ". -". Sidor anges efter Ort/år
    "1971-1975": "isbd_transition",  # Kolon ":" mellan huvudtitel och undertitel. Serietillhörighet anges inom parentes efter ". -". Bidrag: Källpublikation anges efter ". - I: "
    "1976": "isbd",  # -||- ". - " anges före nytt avsnitt (utgivning, omfång, serietillhörighet). -||- . -||- . ISSN anges
}

NON_TERMINATING_ABBREVIATIONS = [
    "co.",  # 13
    "m.fl.",  # 116
    "m. fl.",  # 231
    "m.m.",  # 15
    "m. m.",  # 287
    "o. s. v.",  # 2
    "s.k.",
    "s. k.",
    "bl.a.",
    "bl. a.",
    "ill.",
    "illustr.",
    "portr.",
    "o.",
    "s.",
    "bl.",
    "pl.",
    "kartbl.",
    "pl.-bl."
]

PARENTHESIS_ERAS = ["transition", "parenthesized", "isbd_transition", "isbd"]
DASH_ERAS = ["dash_style", "isbd_transition", "isbd"]

### Extreme regex ###
EXTENT_MARKER_RE = re.compile(r"\b(?:s|bl)<DOT>", re.I)
COMPONENT_EXTENT = re.compile(
    r"\b((?:s|bl|pl)(<DOT>)?)\s*(\d+)(?:-(\d+))?(?:\s*:?\s+(ill(<DOT>)?))?"
)
MONOGRAPH_EXTENT_RE = re.compile(
    r"""
    (?:
        (?:
            \[\d+\]              # [2]
            |\(\d+\)              # (2)
            |\d+                  # 806
            |\b[ivxlc]{2,}\b      # Roman numerals - mostly used for shorter sections
        )
        (?:\s*(?:,|\+)\s*)?
    )+
    \s*(?:s|bl|pl)(<DOT>)?

    (?:
        \s*(?:\+|,)\s*
        (?:
            (?:
                \[\d+\]
                |\(\d+\)
                |\d+
                |\b[ivxlc]{2,}\b
            )
            (?:,\s*)?
        )+
        \s*(?:kartbl|pl<DOT>-bl|pl|bl|s)(<DOT>)?
    )*

    (?:\s*:?\s*ill(<DOT>)?)?
    """,
    re.I | re.X,
)

counters = {
    "properties": Counter(),
    "subjects": Counter(),
    "extents": Counter(),
    "various": Counter(),
}
anomalies = []

### Function that gathers the action (parsing data, cleaning and enriching records) ###


def convert(data, bibliographies: dict, subject_mappings) -> dict | None:
    """Convert a single SHB record into a KBV instance.
    Returns the converted record as a dictionary, or None if the record is invalid or cannot be processed.
    """

    # Set start_year and publ_year to None
    start_year: str | None = None
    publ_year: str | None = None

    # Get the record and main entity
    graph = data["@graph"]
    rec, instance, *remainder = graph

    # Get work and store as a local entity in the instance
    if remainder:
        work = remainder[0]
        assert work["@id"].endswith("#work")
        del work["@id"]
        del graph[2]
    else:
        work = {"@type": "Work"}

    instance["instanceOf"] = work

    # Check which historical syntax group the record belongs to
    syntax_era = identify_syntax_era(instance)

    # Prep for adding SAO
    if "marc:primaryProvisionActivity" in instance and "publication" in instance:
        publ = instance["publication"][0]
        start_year = publ.get("startYear")
        publ_year = publ.get("endYear") or publ.get("year")
        if publ_year == instance["marc:primaryProvisionActivity"]["year"]:
            del instance["marc:primaryProvisionActivity"]

    ### Some initial cleanup ###
    # TODO Review this section - what are we doing and do we want to?

    if "bibliography" in rec:
        rec["bibliography"] = [{"@id": it["@id"]} for it in rec["bibliography"]]
    del rec["marc:catalogingSource"]  # "Annan verksamhet"

    # Remove category ("componentPart") - new categories will be added after parsing
    instance.pop("category", None)

    # Add a Link to the SHB volume which includes the description
    shb_host_num = instance.pop("part")[0]

    link_to_shb_volume(instance, rec, shb_host_num, bibliographies)

    ### Enrich the instance ###

    # Only proceed if there is a note
    if "hasNote" not in instance:
        anomalies.append(
            f"SKIPPING Missing hasNote property - no information to parse\t{instance['@id']}\t{instance}"
        )
        return None
    else:
        assert len(instance["hasNote"]) == 1
        note = instance.pop("hasNote")[0]["label"]
        if note == "TABORT":
            return None

    # Parse notes for structured bibliographic data
    structured_record = parse_note(note, syntax_era)

    # Get subject headings
    structured_record["sao_headings"] = add_sao_headings(
        shb_host_num, start_year, publ_year, subject_mappings
    )
    # TODO Add SAB as well?

    # Enrich the instance with the parsed data

    instance, work = enrich_instance(instance, work, structured_record)
    instance["instanceOf"] = work

    # Count all properties, icnluding nested, in the final instance ###
    counters["properties"].update(walk_keys(instance))

    converted_data = {"@graph": [rec, instance]}

    return converted_data


### Functions for enriching the instance


def enrich_instance(instance: dict, work: dict, structured_record: dict) -> dict:
    # We can assume all titles are published and printed
    instance["category"] = [{"@id": "https://id.kb.se/term/saobf/Print"}]

    # Title
    title = {"@type": "Title"}
    if structured_record.get("title"):
        title["mainTitle"] = structured_record["title"]
    if structured_record.get("subtitle"):
        title["subtitle"] = structured_record["subtitle"]
    instance["hasTitle"] = [title]

    # Responsibility statement and contributors
    responsibility_statement = ""
    if structured_record.get("primary_contributors"):
        # TODO Add structured contributor property as well?
        responsibility_statement = structured_record["primary_contributors"]
        """ "contribution": [
          {
            "@type": "PrimaryContribution",
            "agent": {
              "@type": "Person",
              "familyName": "enamn",
              "givenName": "fnamn"
            },
            "role": [
              {
                "@id": "https://id.kb.se/relator/author"
              }
            ]
          },"""
    if structured_record.get("other_contributors"):
        if responsibility_statement:
            responsibility_statement = (
                responsibility_statement
                + "; "
                + structured_record["other_contributors"]
            )
        else:
            responsibility_statement = structured_record["other_contributors"]

    if responsibility_statement:
        instance["responsibilityStatement"] = responsibility_statement

    # Publication
    publication = {}

    if structured_record.get("place"):
        publication["place"] = [{"@type": "Place", "label": structured_record["place"]}]

    if structured_record.get("year"):
        publication["year"] = structured_record["year"]

    if publication:
        instance["publication"] = [
            {
                "@type": "PrimaryPublication",
                **publication,
            }
        ]

    # Extent
    if structured_record.get("extent"):
        instance["extent"] = [{"@type": "Extent", "label": structured_record["extent"]}]

    # Information about the host publication or series membership
    if structured_record.get("host"):
        part = {
            "@type": "PhysicalResource",
            "category": [{"@id": "https://id.kb.se/term/saobf/Print"}],
        }

        if structured_record["host"].get("title"):
            part["hasTitle"] = [
                {
                    "@type": "Title",
                    "mainTitle": structured_record["host"]["title"],
                }
            ]

        if structured_record["host"].get("publisher"):
            part["responsibilityStatement"] = structured_record["host"]["publisher"]

        if structured_record["host"].get("issn"):
            part["identifiedBy"] = [
                {
                    "@type": "ISSN",
                    "value": structured_record["host"]["issn"],
                }
            ]

        # Part of series
        if not structured_record["is_component_part"]:
            if structured_record["host"].get("remainder"):
                part["seriesStatement"] = structured_record["host"].get("remainder")

            series = {}
            series["inSeries"] = part

            if structured_record["host"].get("part_number"):
                series["seriesEnumeration"] = structured_record["host"].get(
                    "part_number"
                )

            instance["seriesMembership"] = [series]

        # Part of host publication
        else:
            part_statement = ""
            instance["isPartOf"] = [part]
            instance["category"].append(
                {"@id": "https://id.kb.se/term/saobf/ComponentPart"}
            )
            if structured_record["host"].get("part_number"):
                part_statement = structured_record["host"]["part_number"]

            if structured_record["host"].get("extent"):
                if part_statement:
                    part_statement = (
                        part_statement + ", " + structured_record["host"]["extent"]
                    )
                else:
                    part_statement = structured_record["host"]["extent"]

            if structured_record["host"].get("remainder"):
                if part_statement:
                    part_statement = (
                        part_statement + ", " + structured_record["host"]["remainder"]
                    )
                else:
                    part = structured_record["host"]["remainder"]
            if part_statement:
                instance["part"] = [part_statement]

    ### Store remaining unstructured information as a note on the instance ###

    if structured_record.get("remaining_note"):
        instance.setdefault("hasNote", []).append(
            {"@type": "Note", "label": structured_record["remaining_note"]}
        )

    ### Store the full original OCR'd note as an instance note ###
    instance.setdefault("hasNote", []).append(
        {
            "@type": "Note",
            "label": f"Fullständig beskrivning (OCR) ur SHBD: {structured_record["original_note"]}",
        }
    )

    ### Add SAO headings
    if structured_record.get("sao_headings"):
        work["subject"] = structured_record["sao_headings"]
        counters["various"].update(["SAO added"])

    if structured_record["is_component_part"]:
        counters["various"].update(["Component part"])
    else:
        counters["various"].update(["Regular monograph"])

    return instance, work


### Functions for parsing the SHB descriptions ###


def parse_note(note: dict, syntax_era: str) -> tuple:
    """Extract information about the main entity from the note, including name, title, subtitle, extent, ISSN, and any remaining unstructured information.
    Returns a dictionary containing the extracted values.
    """
    is_component_part = False
    structured_record = {}
    host_or_series = ""

    # Some inital cleanup of OCR messiness
    note = normalize_spacing_and_punctuation(note)
    note = normalize_special_cases(note)
    structured_record["original_note"] = note

    # Extract information about reviews and dissertation notes
    review_or_diss_note, remainder = extract_review_diss_or_content_note(note)

    also_in_note, remainder = extract_also_in(remainder)

    # Distinguish part/article from publication/series
    if syntax_era in ["isbd", "isbd_transition"] and " - I:" in remainder:
        remainder, host_or_series = remainder.split(" - I:", 1)
        is_component_part = True
    elif syntax_era in PARENTHESIS_ERAS and remainder.rstrip(".").endswith(")"):
        remainder, host_or_series = extract_partof_from_parenthesis(
            remainder, syntax_era
        )
    elif syntax_era in (
        "dash_style",
        "isbd_transition",
    ):  # Excluding "isbd", where ". -" is a generic delimiter
        host_or_series, remainder = extract_dash_style_host_or_series(
            remainder, syntax_era
        )

    # Extract extent (pages, leaves)
    extent, remainder, is_component_part = extract_extent(
        remainder, is_component_part, is_main_entity_note=True
    )

    place, year, remainder = extract_place_year(remainder)

    # If there is not already a host note, check remaining parentheses for information about series/publichation membership
    if remainder and not host_or_series:
        remainder, host_or_series = extract_partof_from_parenthesis(
            remainder, syntax_era
        )
        # TODO Possibly look at interpreting the two rightmost remainder.split(".") as series/publication for the pre-parenthesis era
        # See tests for syntax and examples

    # Extract contributors
    primary_contributors, remainder = extract_primary_contributors(remainder)

    other_contributors, remainder = extract_other_contributors(remainder)

    if syntax_era == "early":
        host_or_series, remainder, is_component_part = extract_early_host_or_series(
            remainder, extent, is_component_part
        )

    # Extract title and subtitle
    title, subtitle, remainder = extract_title_and_subtitle(
        remainder, syntax_era, True, is_component_part
    )

    remaining_note = (
        ". ".join(
            part
            for part in (
                remainder,
                also_in_note,
                review_or_diss_note,
            )
            if part
        )
        or None
    )

    # Store the information parsed in a dictionary

    if primary_contributors:
        structured_record["primary_contributors"] = primary_contributors.strip()
    if other_contributors:
        structured_record["other_contributors"] = strip_trailing_separators(
            other_contributors
        )
    if title:
        structured_record["title"] = strip_trailing_separators(title)
    else:
        anomalies.append(f"MISSING TITLE\t{structured_record}")
    if subtitle:
        structured_record["subtitle"] = strip_trailing_separators(subtitle)
    if place:
        structured_record["place"] = place.strip()
    if year:
        structured_record["year"] = year.strip()
    if remaining_note:
        structured_record["remaining_note"] = remaining_note.strip().rstrip(".,-;")

    if extent:
        structured_record["extent"] = extent.strip()
    else:
        # Assume descriptions without any extent info are component parts
        is_component_part = True

    # Parse and store information about host publication or series membership
    if host_or_series:
        structured_record["host"] = extract_host_or_series_values(
            host_or_series, syntax_era, is_component_part
        )

    structured_record["is_component_part"] = is_component_part
    return structured_record


def extract_host_or_series_values(
    host_note: str, syntax_era, is_component_part
) -> tuple[str, str, str]:
    """Extract information about the host publication or series membership from the host note.
    Returns a tuple of (title, subtitle, issn)."""

    host = {}
    title, issn, part_remainder = None, None, None

    # Extract ISSN
    issn, remainder = extract_issn(host_note)

    # Extract extent (pages, leaves)
    extent, remainder, is_component_part = extract_extent(
        remainder, is_component_part, is_main_entity_note=False
    )

    part_number, remainder = extract_part_number(remainder)

    publisher, remainder = extract_other_contributors(remainder)

    # Extract title and subtitle
    title, _subtitle, remainder = extract_title_and_subtitle(
        remainder, syntax_era, False, is_component_part
    )

    if part_remainder:
        title = ". ".join(
            part
            for part in [
                title.strip().removesuffix(".").removesuffix(","),
                part_remainder.strip(),
            ]
        )

    if title:
        host["title"] = title
    if publisher:
        host["publisher"] = publisher.strip()
    if issn:
        host["issn"] = issn.strip()
    if part_number:
        host["part_number"] = part_number.strip()

    if extent:
        host["extent"] = extent.strip()
    else:
        # Assume descriptions without any extent info are component parts
        is_component_part = True

    return host


def extract_early_host_or_series(remainder: str, extent: str, is_component_part: bool):
    # ". " coulmay be followed by either subtitle or series/host publication
    # Perhaps we can assume there is a publication title if is_component_part == Ttue?

    # Exclude initials while splitting....
    remainder = exclude_abbreviations_before_split(remainder)

    parts = re.split(r"(?<=\.|\])\s+", remainder)

    # Reintroduce initals...
    parts = reinclude_abbreviations_after_split(parts)

    # If there's no extent, it's likely a newspaper article
    # Title. Newspaper, Number.
    if len(parts) > 1 and not extent:
        host_or_series = parts[-1]
        remainder = ". ".join(part.strip(".") for part in parts[:-1])
        is_component_part = True
        return host_or_series, remainder, is_component_part

    elif len(parts) > 2:
        if is_component_part:
            # Other component parts
            # Title. Journal. Number.
            host_or_series = ". ".join(part.strip(".") for part in parts[-2:])
            remainder = ". ".join(part.strip(".") for part in parts[:-2])

            return host_or_series, remainder, is_component_part

        else:
            # Monographs
            # Title. Place year. Series. Number.
            # Similar structure with series info in the two furthest right positions
            # However not all monographs are part of series
            host_or_series = ". ".join(part.strip(".") for part in parts[-2:])

            # Only treat as publication/series info it contains
            # At least 2 alphabetic characters (series title)
            # At least one numeric character (part number)
            alpha_chars = [char for char in host_or_series if char.isalpha()]
            num_chars = [char for char in host_or_series if char.isnumeric()]
            if len(alpha_chars) > 1 and len(num_chars) > 0:
                remainder = ". ".join(part.strip(".") for part in parts[:-2])

                return host_or_series, remainder, is_component_part

    # Or else
    remainder = ". ".join(part.strip(".") for part in parts)

    return None, remainder, is_component_part


def extract_dash_style_host_or_series(
    remainder: str, syntax_era: str
) -> tuple[str | None, str]:

    DASH_HOST_RE = re.compile(r"(?<=\.|\])\s-\s")

    matches = list(DASH_HOST_RE.finditer(remainder))

    if matches:
        match = matches[-1]

        host_or_series = remainder[match.end() :].strip().lstrip("(").rstrip(")")
        remainder = remainder[: match.start()].strip()

        return host_or_series, remainder

    return None, remainder


def extract_other_contributors(remainder: str) -> tuple[str, str]:
    # If the the title is followed by a " / ", signalling the contributor is next
    if " / " in remainder:
        counters["various"].update(["STRUCTURE\t Title / Author"])
        remainder, contributors = remainder.split(" / ", 1)

        return contributors, remainder
    else:
        return None, remainder


def extract_part_number(remainder: str) -> str:
    """Extract part number from the remainder of the host note, if present.
    Returns the part number as a string, or None if not found."""

    PART_INFO_RE = re.compile(
        r"""
        ^(?P<title>.*?\D)
        \s*[,;.-]?\s*
        (?P<part>\d.*)
        $
        """,
        re.VERBOSE,
    )

    if not (match := PART_INFO_RE.match(remainder)):
        return None, remainder

    number = match["part"].rstrip(" ,.;-")
    remainder = match["title"].rstrip(" ,.;-")

    return number, remainder


def extract_place_year(remainder: str) -> tuple[str, str, str]:
    """Extract place and year from the remainder of the note, if present.
    Returns a tuple of (place, year, remainder)."""

    place, year = None, None
    PLACE_YEAR_RE = re.compile(
        r"""
        \-?\s*
        (?P<place>
            (?:\[[^\]]+\]\s*)?
            [A-ZÅÄÖ][A-Za-zÅÄÖåäö.\- ]*?
        )
        [, ]+
        (?P<year>
            \d{4}
            (?:-\d{2,4})?
            (?:,\s*(?:\d{4}(?:-\d{2,4})?|\d{2}(?:-\d{2})?))*
        )
        \.?
        $
        """,
        re.X,
    )

    remainder = strip_trailing_separators(remainder)

    # Split by period followed by space and thereafter dash or capital letter
    parts = re.split(r"(?<=\.) -?\s?", remainder)

    for i in range(len(parts) - 1, 0, -1):
        candidate = parts[i]

        if match := PLACE_YEAR_RE.fullmatch(candidate):
            place = match["place"]
            year = match["year"]
            left_parts = ". ".join(
                [strip_trailing_separators(part) for part in parts[:i]]
            )
            right_parts = ". ".join(
                [strip_trailing_separators(part) for part in parts[i + 1 :]]
            )
            remainder = f"{left_parts}. {right_parts}".strip()

            return place, year, remainder

    return None, None, remainder


def extract_review_diss_or_content_note(note: str) -> tuple[str, str]:
    """Extract review information from the note, if present.
    Returns a tuple of (review_note, remainder)."""

    match = re.search(
        r"Rec\.\s+i\b|\bSummary:\s*|\s*-\s*Diss\b|\s*-\s*Oiss\b|Innehåller b",
        note,
    )

    if match:
        remainder = note[: match.start()].rstrip("- ")
        review_or_diss_note = (
            note[match.start() :].replace("- Oiss", "- Diss").lstrip("- ")
        )
    else:
        remainder = note
        review_or_diss_note = None

    return review_or_diss_note, remainder


def extract_also_in(note: str) -> tuple[str, str]:
    """Extract "Also published in..." information from the note, if present.
    Returns a tuple of (also_in_note, remainder)."""

    match = re.search(
        r"Även utg\.\s*i\b|Även publ\.\s*i\b|Även i\s+i\b",
        note,
    )

    if match:
        remainder = note[: match.start()].rstrip("- ")
        also_in_note = note[match.start() :].lstrip("- ")
    else:
        remainder = note
        also_in_note = None

    return also_in_note, remainder


def extract_primary_contributors(remainder: dict) -> tuple[str, str]:
    """Extract contributor names from the remainder of the note.
    Returns a tuple of (contributors, remainder)."""

    PERSON_START_RE = re.compile(
        r"""
        (?:
            [A-ZÅÄÖ][a-zåäö]+(?:-[A-ZÅÄÖa-zåäö]+)?   # Arne / Carl-Gösta
            |
            [A-ZÅÄÖ]\.                              # A.
        )
        """,
        re.X,
    )

    # First - look at whatevers to the left of the first period (excluding initials).
    # If there is only one comma there - it's not a full name. Let's assume there is no author.
    #  (Author last name, Author firstname, Title) vs (Title part, Title part.)

    # Exclude initials and abbreviations before splitting
    temp_remainder = exclude_abbreviations_before_split(remainder)
    parts = re.split(
        r"(?<!\b[A-Z])\.\s+",
        temp_remainder,
    )
    if parts[0].count(",") < 2:
        counters["various"].update(["Work without primary contributor"])
        return None, remainder

    comma_separated = remainder.split(",")
    contributor_names = comma_separated.pop(0).strip()

    # If the suspected "name" is longer than 40 characters, or contains a ".", it's probably title
    if len(contributor_names) > 40 or not re.fullmatch(r"[\w\s'-]+", contributor_names):
        comma_separated.insert(0, contributor_names)
        contributor_names = ""
    elif len(contributor_names) < 40:
        if comma_separated:
            first_name = comma_separated[0]

            if (
                looks_like_initial(first_name.strip())
                or re.fullmatch(r"[\w\s'-.]+", first_name)
                and re.fullmatch(r"[\w\s'-]+", contributor_names)
            ):
                contributor_names += ", " + comma_separated.pop(0).strip()
            else:
                comma_separated.insert(0, contributor_names)
                contributor_names = ""

            if (
                len(comma_separated) > 2
                and comma_separated[0].lstrip().startswith("&")
                and looks_like_initial(comma_separated[1].strip())
            ):
                surname = comma_separated.pop(0).strip()
                initials = comma_separated.pop(0).strip()

                contributor_names += f", {surname}, {initials}"
    # If name contains more than letters and certain punctuation, it's probably not a name
    if not re.fullmatch(r"[A-Za-zÀ-ÖØ-öø-ÿ\s',.&\-\[\]]+", contributor_names.strip()):
        contributor_names = None
    else:
        remainder = ",".join(comma_separated).strip()

    # There needs to be a remainder to get the title from
    # It could be that the "," separating name from title has been misread as "." by the OCR
    if not remainder:
        name_parts = contributor_names.split(". ")
        contributor_names = ". ".join(name_parts[:-1])
        remainder = name_parts[-1]

    return contributor_names, remainder


def extract_title_and_subtitle(
    remainder: dict, syntax_era, is_main_entity_note: bool, is_component_part: bool
) -> tuple[dict, dict, dict]:
    """Extract title and subtitle from the remainder of the note.
    Returns a tuple of (title, subtitle, remainder).
    """

    subtitle = ""
    remainder = strip_trailing_separators(remainder)

    # At this point the remainder should only be containign title information
    # In publication/series info, we can't quite interpret subtitles the same way
    if not is_main_entity_note:
        title = remainder.strip(".")
        return title, subtitle, remainder

    # This record is ISBD-like
    # TODO Check if this is necessary at this point?
    if syntax_era in DASH_ERAS and ". -" in remainder:
        title_and_contributor_area, remainder = remainder.split(". -", 1)

        # If the title is directly followed by a ". -", signalling other publication information is next
        if re.search(r"[0-9].?\(", title_and_contributor_area):
            counters["various"].update(
                ["STRUCTURE\t note doesn't start with author/title'"]
            )
            title = title_and_contributor_area + ". - " + remainder
        else:
            counters["various"].update(
                ["STRUCTURE\t'. - ' between title and next area"]
            )
            title = title_and_contributor_area

    # Divide title into title and subtitle
    elif syntax_era in ["isbd", "isbd_transition"] and " : " in remainder:
        title, subtitle = remainder.split(" : ", 1)
        remainder = ""

    # All earlier syntax styles: title. subtitle
    else:
        # Exclude initials and abbreviations during the splitting steå....
        remainder = exclude_abbreviations_before_split(remainder)

        parts = re.split(
            r"(?<!\b[A-Z])\.\s+",
            remainder,
        )

        # Reintroduce abbreviations...
        parts = reinclude_abbreviations_after_split(parts)

        if len(parts) < 2:
            title = ". ".join(part for part in parts)
            remainder = ""
        else:
            title = parts[0]
            subtitle = ". ".join(part for part in parts[1:])
            remainder = ""

    return title, subtitle, remainder


def extract_partof_from_parenthesis(remainder, syntax_era) -> tuple[dict]:
    """Extract partOf information, i.e. information about the host publication, from balanced parentheses present in the note.
    If the description comes from an SHB volume where parentheses were used for host publication,
    return the note with the parentheses removed, and the extracted part-of information.
    """

    end = remainder.rfind(")")
    if end == -1 or syntax_era not in PARENTHESIS_ERAS:
        return remainder, None

    counters["various"].update(["STRUCTURE\tAuthor, title . (publication)"])

    depth = 0
    start = None

    for i in range(end, -1, -1):
        if remainder[i] == ")":
            depth += 1
        elif remainder[i] == "(":
            depth -= 1
            if depth == 0:
                start = i
                break

    if start is not None:
        host_or_series = remainder[start + 1 : end]
        remainder = (remainder[:start] + remainder[end + 1 :]).strip()

        # Only treat as publication/series info it contains at least 2 alphabetic characters
        alpha_chars = [char for char in host_or_series if char.isalpha()]
        if len(alpha_chars) > 1:
            return remainder.replace(" .", "").rstrip(" -"), host_or_series

    return remainder, None


def extract_extent(
    remainder: str, is_component_part: bool, is_main_entity_note: bool
) -> tuple[str]:
    """Extract extent information (pages, leaves) from the note, if present.
    Returns a tuple of (extent, remainder, is_component_part)."""

    extent = ""
    probable_title_author_area = ""
    probable_publication_area = ""

    # Protect abbreviations by replacing . with <DOT>ß
    temp_remainder = exclude_abbreviations_before_split(remainder)

    # Exclude everything left of first period to avoid confusion with main title words that look like Roman numerals ("vi").
    if is_main_entity_note and ". " in temp_remainder:
        probable_title_author_area, probable_publication_area = temp_remainder.split(". ", 1)
    else:
        probable_publication_area = temp_remainder

    if not EXTENT_MARKER_RE.search(probable_publication_area):
        return "", remainder, is_component_part

    else:
        # s. NN(-NN) - probably a component part
        if page_match := COMPONENT_EXTENT.search(
            probable_publication_area,
        ):
            counters["extents"].update(["Part extent (e.g. 's. 23-31')"])
            is_component_part = True

        # NN s. - probably a monograph
        elif page_match := MONOGRAPH_EXTENT_RE.search(probable_publication_area):
            counters["extents"].update(["Monographic extent"])
        else:
            page_match = None

    if page_match:
        left_part = probable_publication_area[: page_match.start()].rstrip(" ,")
        right_part = probable_publication_area[page_match.end() :]

        extent = page_match.group(0)

        # A matched extent may incorrectly start with a publication year,
        # e.g. "Sthlm 1947, 28 s.". Move the year back to the imprint.
        year_before_extent = re.match(r"(^1[7-9]\d{2})(?:,|$)", extent)
        if year_before_extent:
            left_part = f"{left_part} {year_before_extent.group(0)}".rstrip()
            extent = extent[year_before_extent.end() :].lstrip(",; ")
            if extent == "s." or extent == "s":
                left_part = left_part + " " + extent
                extent = ""

        # A year immediately after the extent probably belongs to the imprint,
        # e.g. "155 s. 1955." -> extent "155 s.", imprint "1955."
        year_after_extent = re.search(r"\s+1[7-9]\d{2}\.?$", extent)
        if year_after_extent:
            year = year_after_extent.group(0).strip()
            extent = extent[: year_after_extent.start()].rstrip()
            right_part = f"{year} " + right_part
            if extent == "s." or extent == "s":
                right_part = extent + " " + right_part
                extent = ""

        # Pick up special illustration info 
        EXTRA_EXTENT_INFO_RE = r"(?:\[Med\b.*?(?:i texten\.)?\]|\bMed\b.*?(?:i texten\.)?\.)"
        extra_match = re.search(EXTRA_EXTENT_INFO_RE, right_part)

        if extra_match:
            extra_extent_info = extra_match.group()
            right_part = right_part[:extra_match.start()] + right_part[extra_match.end():]
            extent = extent.replace("<DOT>", "").strip() + ". " + extra_extent_info

        # Put everything back together
        probable_publication_area = (
            (left_part + right_part).strip(",;- ").replace("- -", "-")
        )

        if probable_title_author_area:
            remainder = (
                f"{probable_title_author_area}. {probable_publication_area}".strip()
            )
        else:
            remainder = probable_publication_area.strip()

    extent, remainder = reinclude_abbreviations_after_split([extent, remainder])
    return extent, remainder, is_component_part


def extract_issn(note: str) -> str:
    """Extract ISSN from the given value, if present and valid.
    Returns the ISSN as a string, or None if not found or invalid."""

    ISSN_RE = re.compile(r"\b(?:ISSN\s*:?\s*)?(\d{4}-\d{3}[\dX])\b")
    for match in ISSN_RE.finditer(note):
        issn = match.group(1)
        if valid_issn(issn):
            remainder = note[: match.start()].rstrip(" ,") + note[match.end() :]
            return issn, remainder

    return None, note


### Functions for enriching the records ###


def add_sao_headings(shb_host_num, start_year, publ_year, subject_mappings) -> list:
    """Add subject headings to the work based on the SHB part number and publication years.
    Returns a list of subject references, or None if no subjects found."""

    years_key = f"{start_year}-{publ_year}" if start_year else publ_year
    if rownummap := subject_mappings.get(years_key):
        if subjectrefs := rownummap.get(shb_host_num):  # TODO: opt + 'a' ...
            work_subjects = [{"@id": s} for s in subjectrefs]
            counters["subjects"].update(subjectrefs)

            return work_subjects
    else:
        counters["subjects"].update(["Missing SHB reference!"])
        anomalies.append(
            f"Missing SHB reference\tSHB part num: {shb_host_num} Start year: {start_year} Publ year: {publ_year}"
        )

        # if USE_ANNOT and iri:
        #    for s in work_subjects:
        #        s["@annotation"] = {"source": source}
        # else:
        #    print(f"{partnum} not in {list(rownummap)} for {years_key}", file=sys.stderr)


def link_to_shb_volume(
    thing: dict, rec: dict, partnum: str, bibliographies: dict
) -> None:
    """Moves information about SHB as bibliography, as local and linked entitites, from thing (instance) to record.
    Updates the instance and record in place with local and linked entities."""
    # TODO Review this function to make sure it does what we want and complies with the current KBV data model

    if hosts := thing.pop("isPartOf"):
        assert len(hosts) == 1
        host = hosts[0]

        if "associatedMedia" in thing:
            host["associatedMedia"] = [
                {
                    "@id": am["uri"][0].replace(
                        "http://regina.kb.se/shb/", "https://shb.kb.se/"
                    )
                }
                for am in thing.pop("associatedMedia")
            ]

        if "publication" in thing:
            host["hasTemporalCoverage"] = thing.pop("publication")[0]
            host["hasTemporalCoverage"]["@type"] = "TemporalCoverage"

        hostrecs = host.pop("describedBy")
        assert len(hostrecs) == 1

        if ctrlnr := hostrecs[0].get("controlNumber"):
            iri = f"http://libris.kb.se/resource/bib/{ctrlnr}"
            host["@id"] = iri
            if iri in bibliographies:
                existing_host_desc = bibliographies[iri]
                has_biblio_repr = json.dumps(existing_host_desc, sort_keys=True)
                new_biblio_repr = json.dumps(host, sort_keys=True)
                if has_biblio_repr != new_biblio_repr:
                    bigslice = int(len(has_biblio_repr) * 0.8)
                    assert (
                        has_biblio_repr[:bigslice] == new_biblio_repr[:bigslice]
                    ), f"Mismatch:\n{has_biblio_repr}\n{new_biblio_repr}"
            else:
                bibliographies[iri] = host

            # Alternative to bibliography annotation:
            # thing['cataloguedIn'] = {'@id': iri, "@annotation": {"part": thing.pop("part")}} # in the work...
            for biblioref in rec["bibliography"]:
                bibliography_shb = "https://libris.kb.se/library/SHB"
                if biblioref["@id"] == bibliography_shb:
                    source_id = f"{rec['@id']}#{partnum}"
                    source = {
                        "@id": source_id,
                        "@type": "Source",
                        "isPartOf": {"@id": iri},
                        "item": partnum,
                    }
                    # biblios[source_id] = source
                    rec["describes"] = source
                    annot: dict = {"source": {"@id": source_id}}
                    if USE_ANNOT:
                        biblioref["@annotation"] = annot
                    else:
                        annot["_object"] = {"@id": bibliography_shb}
                        rec.setdefault("_statementBy", {})["bibliography"] = annot
                    break


### Helper functions ###


def normalize_special_cases(text: str) -> str:
    """
    Clean up recurring anomalies in OCR'd text. Return the cleaned up text.
    """

    # Book formats quarto and octavo
    text = text.replace("4:0", "4:o").replace("8:0", "8:o")

    return text


def normalize_spacing_and_punctuation(text: str) -> str:
    """
    Clean up punctuation and spacing issues common in OCR'd text. Return the cleaned up text.
    """

    # Remove punctuation at the very beginning of the note
    text = text.lstrip(". ")

    # Replace dash with hyphen
    text = text.replace("—", "-")

    # Repalce non-breaking space with regular space
    text = text.replace(" ", " ").replace("  ", " ")

    # Repalce double space with single space
    text = text.replace("  ", " ")

    # Repalce unidiomatic comma-followed-by-period with ","
    text = text.replace(",.", ",").replace(", .", ",").replace(", . .", ",")

    # Always have a space between "." and any uppercase letter
    text = re.sub(r"\.(?=[A-ZÅÄÖ])", ". ", text)

    # Always have a space between "," and any letter
    text = re.sub(r",(?=[A-Za-zÅÄÖåäö])", ", ", text)

    # Always have a space between two lowercase letters and a digit
    text = re.sub(r"([a-zåäö]{2,})(\d)", r"\1 \2", text)

    # Always have a space between a digit and two lowercase letters
    text = re.sub(r"(\d)([a-zåäö]{2,})", r"\1 \2", text)

    # \\ Double backslashes, aka one backslash escaped with another, seem to be a common misreading of brackets
    # Get the opening and closing
    text = re.sub(r"\\([a-zåäöA-ZÅÄÖ]+)\\", r"[\1]", text)

    # Pragmatically and empirically assume the rest are closing
    text = re.sub(r"\\", r"]", text)

    # Remove all occurences of characters, which appear be OCR dust rather than syntax
    text = text.replace("=", "").replace(">", "")

    return text


def valid_issn(issn: str) -> bool:
    """Check if the given ISSN is valid according to the ISSN check digit algorithm.
    Returns True if valid, False otherwise."""

    digits = issn.replace("-", "")

    total = sum(int(digits[i]) * (8 - i) for i in range(7))

    remainder = total % 11
    check = 11 - remainder

    if check == 10:
        expected = "X"
    elif check == 11:
        expected = "0"
    else:
        expected = str(check)

    return digits[-1] == expected


def walk_keys(obj, prefix=""):
    """Recursively walk through a nested dictionary or list and yield all keys with their paths."""

    if isinstance(obj, dict):
        for key, value in obj.items():
            path = f"{prefix}.{key}" if prefix else key
            yield path
            yield from walk_keys(value, path)

    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            yield from walk_keys(item, f"{prefix}[{i}]")


def identify_syntax_era(instance) -> str:
    """Identify the "syntax era" (syntactical characteristics typical to a volume/set of volumes) based on the SHB volume title.
    Returns a string representing the syntax era, e.g., "era_1", "era_2", etc., or fallback ""era_1" if the era cannot be determined.
    >>> identify_syntax_era({"isPartOf": [{"hasTitle": [{"mainTitle": "Svensk historisk bibliografi 1921-1935"}]}]})
    'parenthesized'
    >>> identify_syntax_era({"isPartOf": [{"hasTitle": [{"mainTitle": "Svensk historisk bibliografi 1976"}]}]})
    'isbd'
    >>> identify_syntax_era({"isPartOf": [{"hasTitle": [{"mainTitle": "Svensk historisk bibliografi 1901-1920"}]}]})
    'early'
    """

    # Extract information about the source SHB volume
    shb_volume_title = (
        instance.get("isPartOf", [])[0]
        .get("hasTitle", [])[0]
        .get("mainTitle", "")
        .replace("Svensk historisk bibliografi", "")
        .strip()
    )
    counters["various"].update([f"VOLUME\t{shb_volume_title}"])

    return SYNTAX_ERAS.get(shb_volume_title, "early")


def looks_like_initial(s: str) -> bool:
    """
    Check if the given string looks like an initial (e.g., "A." or "A.-B.").
    Returns True if it looks like an initial, False otherwise.

    >>> looks_like_initial("")
    False
    >>> looks_like_initial("A")
    False
    >>> looks_like_initial("A.")
    True
    >>> looks_like_initial("Ab.")
    False
    >>> looks_like_initial("A.-B.")
    True
    """
    if not s:
        return False
    return s[0].isupper() and s.endswith(".") and (len(s) == 2 or s[-2].isupper())


def strip_trailing_separators(text: str) -> str:
    text = text.strip()

    while len(text) > 1 and text[-1] in ".,;:–—-":
        if text.endswith("..."):
            break
        text = text[:-1].rstrip()

    return text


def exclude_abbreviations_before_split(remainder: str) -> str:
    # Common abbreviations
    for abbr in NON_TERMINATING_ABBREVIATIONS:
        remainder = re.sub(
            rf'(?<![^\W\d_]){re.escape(abbr)}',
            lambda m: m.group().replace(".", "<DOT>"),
            remainder,
            flags=re.IGNORECASE
        )
    # Initials
    remainder = re.sub(r"\b([A-Z])\.\s+(?=[A-Z])", r"\1<DOT> ", remainder)

    return remainder


def reinclude_abbreviations_after_split(parts: list) -> str:
    # Reintroduce initials and abbreviations
    parts = [p.replace("<DOT>", ".") for p in parts]
    return parts


def make_subject_mappings(subject_mapping_sheets: list[str]) -> dict:
    """Load subject mappings from the given CSV files and return a dictionary of mappings."""
    subject_mappings: dict = {}

    for sheet_file in subject_mapping_sheets:
        _load_subject_mappings(subject_mappings, Path(sheet_file))

    return subject_mappings


def _load_subject_mappings(subject_mappings: dict, sheet_file: Path) -> None:
    """Load subject mappings from a single CSV file and update the given subject_mappings dictionary."""
    years_key = sheet_file.with_suffix("").name.split("-", 1)[-1]
    assert years_key not in subject_mappings
    rownummap = subject_mappings[years_key] = {}

    with sheet_file.open() as f:
        for i, row in enumerate(csv.reader(f)):
            subjects: list[str] = []

            startnum: str | None = None
            endnum: str | None = None

            for x in row[::-1]:
                if not subjects and not x:
                    continue

                if x.startswith("https://id.kb.se/term/"):
                    subjects.append(x)
                elif endnum is None:
                    endnum = x
                elif startnum is None:
                    startnum = x
                else:
                    break

            subjects.reverse()

            if subjects:
                if not startnum:
                    anomalies.append(f"Missing startnum in {sheet_file} row {i} {row}")
                    continue

                assert startnum

                startnum = startnum.strip()
                if startnum.endswith("a"):
                    startnum = startnum[:-1]

                if endnum:
                    endnum = endnum.strip()
                    if endnum.endswith("a"):
                        endnum = endnum[:-1]

                    for n in range(int(startnum), int(endnum) + 1):
                        rownummap[f"{n}"] = subjects
                else:
                    rownummap[f"{startnum}+"] = subjects


def pretty_print_sample(lines, biblios, subject_mappings, context_file):
    """Pretty print a sample of converted records for inspection."""

    with open(context_file) as f:
        ctx = json.load(f)

    results = []

    x = 0
    for i, l in enumerate(lines):
        x += 1
        if x > 19:
            if i % 10_000 != 0:
                continue
            else:
                x = 0
        if data := convert(json.loads(l), biblios, subject_mappings):
            results.append(data)

    results = list(biblios.values()) + results

    print(
        json.dumps({"@context": ctx["@context"], "@graph": results}, ensure_ascii=False)
    )


def write_reports(
    counters: dict, anomalies: list, report_file: TextIOBase, anomalies_file: TextIOBase
) -> None:
    """Write reports of the counts of properties, curiosities, extents, and subjects to the given report file.
    Also write a list of encountered anomalies, with record IDs and records, to the anomalies file.
    """

    report_file.write("# Egenskaper\n\n")
    report_file.write("| Egenskap | Antal |\n")
    report_file.write("|----------|-------:|\n")
    report_file.writelines(
        f"| {prop} | {count} |\n"
        for prop, count in counters["properties"].most_common()
    )

    report_file.write("\n\n# Kuriositeter\n\n")
    report_file.write("| Kuriositet | Antal |\n")
    report_file.write("|----------|-------:|\n")
    report_file.writelines(
        f"| {curiosity} | {count} |\n"
        for curiosity, count in counters["various"].most_common()
    )

    report_file.write("\n\n# Omfång\n\n")
    report_file.write("| Omfång | Antal |\n")
    report_file.write("|----------|-------:|\n")
    report_file.writelines(
        f"| {extent} | {count} |\n"
        for extent, count in counters["extents"].most_common()
    )

    report_file.write("\n\n# Ämnesord\n\n")
    report_file.write("| Ämne | Antal |\n")
    report_file.write("|----------|-------:|\n")
    report_file.writelines(
        f"| {subject} | {count} |\n"
        for subject, count in counters["subjects"].most_common()
    )

    print(*anomalies, sep="\n", file=anomalies_file)


### Main action ###

if __name__ == "__main__":
    import argparse

    argp = argparse.ArgumentParser()
    argp.add_argument("-t", "--test", action="store_true", default=False)
    argp.add_argument("--sample-pretty-with")
    argp.add_argument("infile")
    argp.add_argument("subject_mapping_sheets")
    argp.add_argument("outfile")
    argp.add_argument("report_file")
    argp.add_argument("info_and_errors_file")
    args = argp.parse_args()

    if args.test:
        import doctest

        doctest.testmod()
        sys.exit(0)

    print("Getting started!")

    # Load subject mappings from CSV files
    subject_files = list(Path(args.subject_mapping_sheets).glob("*.csv"))
    if not subject_files:
        raise FileNotFoundError("No CSV files found")
    subject_mappings = make_subject_mappings(subject_files)

    bibliographies: dict = {}

    # Open files
    with open(args.infile) as infile, open(
        args.outfile, "w", encoding="utf-8"
    ) as outfile, open(args.report_file, "w", encoding="utf-8") as report_file, open(
        args.info_and_errors_file, "w", encoding="utf-8"
    ) as anomalies_file:

        # Optionally pretty print a sample of converted records for inspection
        if args.sample_pretty_with:
            pretty_print_sample(
                infile, bibliographies, subject_mappings, args.sample_pretty_with
            )
            sys.exit()

        # Process each line in the input file
        for i, line in enumerate(infile):
            if i % 1000 == 0:
                print(f"Processing record {i+1}")
            if data := convert(json.loads(line), bibliographies, subject_mappings):
                json.dump(data, outfile, ensure_ascii=False)
                outfile.write("\n")

        # Write some reports
        write_reports(counters, anomalies, report_file, anomalies_file)
