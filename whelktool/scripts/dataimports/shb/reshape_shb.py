from io import TextIOBase
import csv
import json
import re
import sys
from pathlib import Path
from collections import Counter

USE_ANNOT = False
PARENTHESIS_ERAS = ["era_2", "era_3", "era_5", "era_6"]

### Extreme regex ###
EXTENT_MARKER_RE = re.compile(r"\b(?:s|bl)\.", re.I)
COMPONENT_EXTENT = re.compile(r"\b((?:s|bl)\.?)\s*(\d+)(?:-(\d+))?(?:\s+(ill\.?))?")
MONOGRAPH_EXTENT_RE = re.compile(
    r"""
    (?:
        (?:
            \[\d+\]              # [2]
            |\(\d+\)              # (2)
            |\d+                  # 806
            |\b[ivxlc]{2,}\b      # Roman numerals - mostly used for shorter sections
        )
        (?:,\s*)?
    )+
    \s*(?:s|bl)\.?

    (?:
        \s*\+\s*
        (?:
            (?:
                \[\d+\]
                |\(\d+\)
                |\d+
                |\b[ivxlc]{2,}\b
            )
            (?:,\s*)?
        )+
        \s*(?:s|bl)\.?
    )*

    (?:\s*:\s*ill\.?)?
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

    # Check which historical syntax group the record belongs to
    syntax_era = identify_syntax_era(instance)

    shb_part_num = instance.pop("part")[0]
    iri: str | None = None
    source: dict | None = None

    # Prep for adding SAO and SAB
    # TODO Add SAB as well
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

    ### Store work as a local entity in the instance ###
    if remainder:
        work = remainder[0]
        assert work["@id"].endswith("#work")
        del work["@id"]
        del graph[2]
    else:
        work = {"@type": "Work"}

    instance["instanceOf"] = work

    ### Try to link local entities ###
    link_local_entities(instance, rec, shb_part_num, bibliographies)

    ### Parse notes ###
    parse_note(instance, syntax_era)

    ### Add subject headings to the instance ###
    sao_headings = add_sao_headings(
        shb_part_num, start_year, publ_year, subject_mappings
    )

    if sao_headings:
        work["subject"] = sao_headings

    ### Wrap up ###

    # Count all properties, icnluding nested, in the final instance ###
    counters["properties"].update(walk_keys(instance))

    return {"@id": graph[0]["@id"], "@graph": data}


### Functions for parsing the SHB records ###


def parse_note(instance: str, syntax_era: str) -> None:
    """Parse the note of the instance, extracting information about the main entity and its host publication or series membership.
    Updates the instance in place with the extracted information."""

    # Assume it's a monograph until otherwise indicated
    is_component_part = False

    partof_note = None

    if "hasNote" not in instance:
        anomalies.append(
            f"Missing hasNote property - no information to parse\t{instance['@id']}\t{instance}"
        )
        return None
    else:
        assert len(instance["hasNote"]) == 1
        original_note = instance.pop("hasNote")[0]["label"]

        if original_note == "TABORT":
            return None

        note = original_note.replace("—", "-").replace(" ", " ").replace("  ", " ")

        ### Distinguish part/article from publication/series ###

        # Denoted by "I:" syntax (publication)
        partof_mark = " - I:"
        if partof_mark in note:
            note, partof_note = note.split(partof_mark, 1)
            is_component_part = True

        # Denoted by parenthesis syntax (series or publication) at the end
        elif note.endswith(")"):
            note, partof_note = extract_partof_from_parenthesis(note, syntax_era)

        ### Parse and store information about the main entity (instance) ###
        name, title, subtitle, extent, issn, note_remainder, is_component_part = (
            extract_structured_values(note, is_component_part)
        )

        instance["hasTitle"] = {"@type": "Title", "mainTitle": title}

        if subtitle:
            instance["hasTitle"]["subtitle"] = subtitle

        if name:
            instance["responsibilityStatement"] = name

        if extent:
            instance["extent"] = [{"@type": "Extent", "label": extent}]

        if issn:
            instance["identifiedBy"] = {"@type": "ISSN", "value": issn}

        ### Check remaining parentheses for information about series/publichation membership ###
        if note_remainder and not partof_note:
            note_remainder, partof_note = extract_partof_from_parenthesis(
                note_remainder, syntax_era
            )

        ### Parse and store information from the "part of" note (publication OR series as local entity) ###
        if partof_note:
            (
                part_name,
                part_title,
                part_subtitle,
                part_extent,
                part_issn,
                part_remainder,
                is_component_part,
            ) = extract_structured_values(
                partof_note, is_component_part, is_main_entity_note=False
            )

            part = {"@type": "Instance", "label": part_title}

            if part_name:
                part["responsibilityStatement"] = part_name
            if part_title:
                part["hasTitle"] = {"@type": "Title", "mainTitle": part_title}
            if part_subtitle:
                instance["hasTitle"]["subtitle"] = part_subtitle
            if part_issn:
                part["identifiedBy"] = {"@type": "ISSN", "value": part_issn}
            if part_extent:
                part["extent"] = [{"@type": "Extent", "label": part_extent}]
            if part_remainder:
                if USE_ANNOT:
                    part["@annotation"] = {"comment": part_remainder}
                else:
                    part["label"] += part_remainder
                    # raise NotImplementedError  # TODO

            if is_component_part:
                instance["partOf"] = [part]
            else:
                instance["seriesMembership"] = [part]

        ### Clean up categories ###

        # We can assume all titles are published and printed
        instance["category"].append({"@id": "https://id.kb.se/term/saobf/Print"})

        # Remove the category "componentPart" if there is nothing indicating it
        if not is_component_part:
            instance["category"].remove(
                {"@id": "https://id.kb.se/term/saobf/ComponentPart"}
            )
            counters["various"].update(["ComponentPart - TRUE"])
        else:
            counters["various"].update(["ComponentPart - FALSE"])

        ### Store remaining unstructured information as a note on the instance ###
        if note_remainder:
            instance.setdefault("hasNote", []).append(
                {"@type": "Note", "label": note_remainder}
            )

        ### Finally, for backup, store the full original OCR'd note as an instance note ###
        if original_note:
            instance.setdefault("hasNote", []).append(
                {
                    "@type": "Note",
                    "label": f"Fullständig beskrivning (OCR) ur SHBD: {original_note}",
                }
            )


def extract_structured_values(
    note: dict, is_component_part: bool, is_main_entity_note: bool = True
) -> tuple:
    """Extract information about the main entity from the note, including name, title, subtitle, extent, ISSN, and any remaining unstructured information.
    Returns a tuple of (name, title, subtitle, extent, ISSN, remainder, is_component_part).
    """

    # Extrct extent (pages, leaves)
    extent, remainder, is_component_part = extract_extent(note, is_component_part)

    # Extract ISSN - but only from embedded series/publication info
    if not is_main_entity_note:
        issn = extract_issn(remainder)
    else:
        issn = ""

    # Extract contributors
    contributors, remainder = extract_contributors(remainder)

    # Extract title and subtitle
    title, subtitle, remainder = extract_title_and_subtitle(remainder, is_main_entity_note)

    return (
        contributors,
        title.strip(),
        subtitle.strip() or None,
        extent or None,
        issn or None,
        remainder.strip() or None,
        is_component_part,
    )


def extract_contributors(remainder: dict) -> tuple[str, str]:
    """Extract contributor names from the remainder of the note.
    Returns a tuple of (contributors, remainder)."""

    personnameparts = []
    initial = -1
    comma_separated = remainder.split(",")
    contributors = comma_separated.pop(0).strip()

    # A last name containing a period is probably a title
    if not re.fullmatch(r"[\w\s'-]+", contributors):
        comma_separated.insert(0, contributors)
        contributors = ""
    else:
        if comma_separated:
            first = comma_separated[0]
            if (
                looks_like_initial(first.strip())
                or re.fullmatch(r"[\w\s'-.]+", first)
                and re.fullmatch(r"[\w\s'-]+", contributors)
            ):
                contributors += ", " + comma_separated.pop(0).strip()
            else:
                comma_separated.insert(0, contributors)
                contributors = ""

            if (
                len(comma_separated) > 2
                and comma_separated[0].lstrip().startswith("&")
                and looks_like_initial(comma_separated[1].strip())
            ):
                surname = comma_separated.pop(0).strip()
                initials = comma_separated.pop(0).strip()

                contributors += f", {surname}, {initials}"

    # If name contains more than letters and certain punctuation, it's probably not a name
    if not re.fullmatch(r"[A-Za-zÀ-ÖØ-öø-ÿ\s',.&\-\[\]]+", contributors.strip()):
        contributors = None
    else:
        remainder = ",".join(comma_separated).strip()

    return contributors, remainder


def extract_title_and_subtitle(
    remainder: dict, is_main_entity_note: bool
) -> tuple[dict, dict, dict]:
    """Extract title and subtitle from the remainder of the note.
    Returns a tuple of (title, subtitle, remainder).
    """

    subtitle = ""

    # This record is ISBD-like
    if ". -" in remainder:
        title_and_author_area, remainder = remainder.split(". -", 1)
        # If the the title is followed by a " / ", signalling the contributor is next
        if " / " in title_and_author_area:
            counters["various"].update(["STRUCTURE\t Title / Author. - Publication"])
            title, author_area = title_and_author_area.split(" / ", 1)
            # TODO Do we want to fetch the author(s) for responsibilityStatememnt from here?
            remainder = author_area + ". -" + remainder

        # If the title is directly followed by a ". -", signalling other publication information is next
        elif re.search(r"[0-9].?\(", title_and_author_area):
            counters["various"].update(
                ["STRUCTURE\t note doesn't start with author/title'"]
            )
            title = title_and_author_area + ". - " + remainder
        else:
            counters["various"].update(
                ["STRUCTURE\t'. - ' between title and next area"]
            )
            title = title_and_author_area

    # Another way to get the title
    else:
        counters["various"].update(["STRUCTURE\tOther structure"])
        x = int(remainder.count(". ") * 0.3) or 1
        title, *remainder = (
            remainder.rsplit(". ", x)
            if is_main_entity_note and ". " in remainder
            else (remainder, "")
        )
        remainder = ". ".join(remainder)
        remainder_is_pages = all(s.strip().isdigit() for s in remainder.split("-"))
        if len(title.strip()) == 1:
            moretitle, remainder = (
                remainder.split(".", 1) if "." in remainder else (remainder, "")
            )
            title += "." + moretitle
        elif remainder_is_pages:
            title, pre_remainder = (
                title.rsplit(". ", 1) if ". " in title else (title, "")
            )
            if not pre_remainder:
                title, posttitle = title.rsplit(" ", 1)
                pre_remainder = posttitle.strip()
            remainder = pre_remainder + ". " + remainder

    # Divide title into title and subtitle
    if " : " in title:
        title, subtitle = title.split(" : ", 1)

    title = title.strip().removesuffix(".").removesuffix(",")

    return title, subtitle, remainder


def extract_partof_from_parenthesis(note, syntax_era) -> tuple[dict]:
    """Extract partOf information, i.e. information about the host publication, from balanced parentheses present in the note.
    If the description comes from an SHB volume where parentheses were used for host publication,
    return the note with the parentheses removed, and the extracted part-of information.
    """

    end = note.rfind(")")
    if (end == -1) or syntax_era not in PARENTHESIS_ERAS:
        return note, None

    counters["various"].update(["STRUCTURE\tAuthor, title . (publication)"])

    depth = 0

    for i in range(end, -1, -1):
        if note[i] == ")":
            depth += 1
        elif note[i] == "(":
            depth -= 1
            if depth == 0:
                partof_note = note[i + 1 : end]
                remainder = (note[:i] + note[end + 1 :]).strip()
                # If it doesn't contain more than one charatcer and at least one is alphabetic, it's probbably not a title
                if len(partof_note) > 1 and any(char.isalpha() for char in partof_note):
                    return remainder, partof_note

    # Unbalanced parentheses
    return note, None


def extract_extent(remainder: str, is_component_part: bool) -> tuple[str]:
    """Extract extent information (pages, leaves) from the note, if present.
    Returns a tuple of (extent, remainder, is_component_part)."""

    pages = ""

    if not EXTENT_MARKER_RE.search(remainder):
        return "", remainder, is_component_part

    else:
        # s. NN(-NN) - probably a component part
        if page_match := COMPONENT_EXTENT.search(
            remainder,
        ):
            counters["extents"].update(["Part extent (e.g. 's. 23-31')"])
            is_component_part = True

        # NN s. - probably a monograph
        elif page_match := MONOGRAPH_EXTENT_RE.search(remainder):
            counters["extents"].update(["Monographic extent"])
        else:
            page_match = None

    if page_match:
        left_part = remainder[: page_match.start()].rstrip(" ,")
        right_part = remainder[page_match.end() :]

        pages = page_match.group(0)

        # A matched extent may incorrectly start with a publication year,
        # e.g. "Sthlm 1947, 28 s.". Move the year back to the imprint.
        year_before_extent = re.match(r"(^1[7-9]\d{2})(?:,|$)", pages)
        if year_before_extent:
            left_part = f"{left_part} {year_before_extent.group(0)}".rstrip()
            pages = pages[year_before_extent.end() :].lstrip(",; ")
            if pages == "s." or pages == "s":
                left_part = left_part + " " + pages
                pages = ""

        # A year immediately after the extent probably belongs to the imprint,
        # e.g. "155 s. 1955." -> extent "155 s.", imprint "1955."
        year_after_extent = re.search(r"\s+1[7-9]\d{2}\.?$", pages)
        if year_after_extent:
            year = year_after_extent.group(0).strip()
            pages = pages[: year_after_extent.start()].rstrip()
            right_part = f"{year} " + right_part
            if pages == "s." or pages == "s":
                right_part = pages + " " + right_part
                pages = ""

        remainder = (left_part + right_part).strip(",;- ").replace("- -", "-")

        if "-" in pages:
            is_component_part = True

    return pages, remainder, is_component_part


def extract_issn(value: str) -> str:
    """Extract ISSN from the given value, if present and valid.
    Returns the ISSN as a string, or None if not found or invalid."""

    ISSN_PATTERN = re.compile(r"\d{4}-\d{3}[\dX]")

    issn = re.findall(ISSN_PATTERN, value)

    issn = [n for n in issn if valid_issn(n)]
    if len(issn) == 1:
        return issn[0]


### Functions for enriching the records ###

def add_sao_headings(shb_part_num, start_year, publ_year, subject_mappings) -> list:
    """Add subject headings to the work based on the SHB part number and publication years.
    Returns a list of subject references, or None if no subjects found."""

    years_key = f"{start_year}-{publ_year}" if start_year else publ_year
    if rownummap := subject_mappings.get(years_key):
        if subjectrefs := rownummap.get(shb_part_num):  # TODO: opt + 'a' ...
            work_subjects = [{"@id": s} for s in subjectrefs]
            counters["subjects"].update(subjectrefs)

            return work_subjects
    else:
        counters["subjects"].update(["Missing SHB reference!"])
        anomalies.append(
            f"Missing SHB reference\tSHB part num: {shb_part_num} Start year: {start_year} Publ year: {publ_year}"
        )

        # if USE_ANNOT and iri:
        #    for s in work_subjects:
        #        s["@annotation"] = {"source": source}
        # else:
        #    print(f"{partnum} not in {list(rownummap)} for {years_key}", file=sys.stderr)


def link_local_entities(thing: dict, rec: dict, partnum: str, bibliographies: dict) -> None:
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
    Returns a string representing the syntax era, e.g., "era_1", "era_2", etc., or fallback ""era_1" if the era cannot be determined."""

    # Extract information about the source SHB volume
    shb_volume_title = (
        instance.get("isPartOf", [])[0].get("hasTitle", [])[0].get("mainTitle", "")
    )
    counters["various"].update([f"VOLUME\t{shb_volume_title}"])

    # Monografi: Efternamn, Förnamn., Titel. undertitel. sid. Ort år. Serietillhörighet. numrering.
    # Monografi utan författare: Titel. undertitel. sid. Ort år. Serietillhörighet. numrering.
    # Bidrag: Efternamn, Förnamn., Titel. undertitel. Publ-titel. nr, sid.
    # Bidrag utan författare: Titel. undertitel. Publ-titel. nr, sid.
    # Bidrag (tidningsartikel): Efternamn, Förnamn., Titel. Publ-titel YYYY, nr (DD/MM).
    if shb_volume_title in [
        "Svensk historisk bibliografi 1771-1874",
        "Svensk historisk bibliografi 1875-1900",
        "Svensk historisk bibliografi 1901-1920",
    ]:
        return "era_1"

    ### This is where "parenthesis" syntax first shows up
    # Monografi: Efternamn, Förnamn., Titel. undertitel. sid. Ort år. Serietillhörighet. numrering.
    # Monografi utan författare: Titel. undertitel. sid. Ort år. Serietillhörighet. numrering.
    # Bidrag:  Efternamn, Förnamn., Titel. undertitel. Publ-titel. nr, sid.
    # Bidrag utan författare: Titel. undertitel. Publ-titel. nr, sid.
    # Bidrag (tidningsartikel): Efternamn, Förnamn., Titel. Publ-titel YYYY, nr.
    elif shb_volume_title in [
        "Svensk historisk bibliografi 1921-1935",
        "Svensk historisk bibliografi 1936-1950",
    ]:
        return "era_2"

    # Monografi: Efternamn, Förnamn, Titel. undertitel. sid. Ort år. (Serietillhörighet. numrering.)
    # Monografi utan författare: Titel. undertitel. sid. Ort år. (Serietillhörighet. numrering.)
    # Bidrag: Efternamn, Förnamn, Titel. undertitel. (Publ-titel nr (årtal), sid.)
    # Bidrag (tidningsartikel): Efternamn, Förnamn, Titel. undertitel. (Publ-titel DD/MM YYYY.)
    elif shb_volume_title in ["Svensk historisk bibliografi 1951-1960"]:
        return "era_3"

    ### Taking a break from parentheses
    ### Sidor now comes after "Ort år"
    ### Introducing dashes as separator before series/publication
    # Monografi: Efternamn, Förnamn, Titel : undertitel. Ort år. sid. - Serietillhörighet. numrering.
    # Monografi utan författare: Titel. undertitel. Ort år. sid. - Serietillhörighet. numrering.
    # Bidrag: Efternamn, Förnamn, Titel. - Publ-titel årg (årtal):numrering, sid.
    # Bidrag utan författare: Titel. - Publ-titel årg (årtal):numrering, sid.
    # Bidrag (tidningsartikel): Efternamn, Förnamn, Titel. - Publ-titel DD.MM YYYY.
    elif shb_volume_title in ["Svensk historisk bibliografi 1961-1970"]:
        return "era_4"

    ### This is where the "I:" syntax starts
    ### The two below are very similar, apart from the presence of ISSN and the order of the values in "I:"
    # Monografi: Efternamn, Förnamn, Titel : undertitel. Ort år. sid. - (Serietillhörighet ; numrering)
    # Monografi utan författare: Titel : undertitel / Upphov. Ort år. sid. - (Serietillhörighet ; numrering)
    # Bidrag: Efternamn, Förnamn, Titel : undertitel. - "I:" Publ-titel årg(årtal):numrering, sid.
    # Bidrag utan författare: Titel : undertitel / Upphov. - "I:" Publ-titel årg(årtal):numrering, sid.
    # Bidrag (tidningsartikel): Efternamn, Förnamn, Titel : undertitel. - "I:" Publ-titel DD.MM YYYY
    elif shb_volume_title in ["Svensk historisk bibliografi 1971-1975"]:
        return "era_5"

    # Monografi: Efternamn, Förnamn, Titel : undertitel. - Ort, år. - sid. - (Serietillhörighet, ISSN)
    # Monografi utan författare: Titel : undertitel / Upphov. - Ort, år. - sid.
    # Bidrag: Efternamn, Förnamn, Titel : undertitel. - "I:" Publ-titel, ISSN, numrering, årg, sid.
    # Bidrag utan författare: Titel : undertitel / Upphov. - "I:" Publ-titel, ISSN, numrering, årg, sid.
    # Bidrag (tidningsartikel) Efternamn, Förnamn, Titel : undertitel. - "I:" Publ-titel DD.MM.YYYY
    elif shb_volume_title in ["Svensk historisk bibliografi 1976"]:
        return "era_6"

    # Fallbck
    else:
        return "era_1"


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
    Also write a list of encountered anomalies, with record IDs and records, to the anomalies file."""

    report_file.write("# Egenskaper\n\n")
    report_file.write("| Egenskap | Antal |\n")
    report_file.write("|----------|-------:|\n")

    for prop, count in counters["properties"].most_common():
        report_file.write(f"| {prop} | {count} |\n")

    report_file.write("\n\n")

    report_file.write("# Kuriositeter\n\n")
    report_file.write("| Kuriositet | Antal |\n")
    report_file.write("|----------|-------:|\n")

    for curiosity, count in counters["various"].most_common():
        report_file.write(f"| {curiosity} | {count} |\n")

    report_file.write("\n\n")

    report_file.write("# Omfång\n\n")
    report_file.write("| Omfång | Antal |\n")
    report_file.write("|----------|-------:|\n")

    for extent, count in counters["extents"].most_common():
        report_file.write(f"| {extent} | {count} |\n")

    report_file.write("\n\n")

    report_file.write("# Ämnesord\n\n")
    report_file.write("| Ämne | Antal |\n")
    report_file.write("|----------|-------:|\n")

    for subject, count in counters["subjects"].most_common():
        report_file.write(f"| {subject} | {count} |\n")

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
