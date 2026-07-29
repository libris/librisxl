from io import TextIOBase
import csv
import json
import os
import re
import sys
from pathlib import Path
from collections import Counter

USE_ANNOT = False
PARENTHESIS_ERAS = ["era_2", "era_3", "era_5", "era_6"]

EXTENT_MARKER_RE = re.compile(r"\b(?:s|bl)\.", re.I)
COMPONENT_EXTENT = re.compile(r"\b((?:s|bl)\.?)\s*(\d+)(?:-(\d+))?(?:\s+(ill\.?))?")
MONOGRAPH_EXTENT_RE = re.compile(
    r"""
    (?:
        (?:
            \[\d+\]              # [2]
            |\(\d+\)              # (2)
            |\d+                  # 806
            |\b[ivxlcdm]{2,}\b      # Roman numerals
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
                |\b[ivxlcdm]{2,}\b
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


def convert(data, biblios: dict, subject_mappings) -> dict | None:

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
    link_local_entities(instance, rec, shb_part_num)

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


def parse_note(instance: str, syntax_era: str):
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

        ### Parse and store information about the main entity (instance)) ###
        name, title, subtitle, extent, note_remainder, is_component_part = (
            extract_properties_and_values(note, is_component_part)
        )

        instance["hasTitle"] = {"@type": "Title", "mainTitle": title}

        if subtitle:
            instance["hasTitle"]["subtitle"] = subtitle

        if name:
            instance["responsibilityStatement"] = name

        if extent:
            instance["extent"] = [{"@type": "Extent", "label": extent}]

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
                part_remainder,
                is_component_part,
            ) = extract_properties_and_values(
                partof_note, is_component_part, dotnote=False
            )
            part_issn = extract_issn(part_title)
            part = {"@type": "Instance", "label": part_title}

            if part_name:
                part["responsibilityStatement"] = part_name
            if part_title:
                part["hasTitle"] = {"@type": "Title", "mainTitle": part_title}
            if part_issn:
                part["identifiedBy"] = {"@type": "ISSN", "value": part_issn}
            if part_extent:
                part["extent"] = [{"@type": "Extent", "label": extent}]
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


def extract_properties_and_values(note: dict, is_component_part, dotnote: bool = True):
    """
    >>> parse_note("Surname, G.-N., Anything")
    ('Surname, G.-N.', 'Anything', None, None, None)

    >>> parse_note("Anything. Surname, G.-N., Stuff.")
    (None, 'Anything', None, None, 'Surname, G.-N., Stuff.')

    >>> parse_note("Schuck, A., H. Schücks enka & Co. AB 150 år. [Stockholm.] Sthlm 1947, 28 s.")
    ('Schuck, A.', 'H. Schücks enka & Co. AB 150 år', None, '28 s.', '[Stockholm.] Sthlm 1947')

    >>> parse_note("Meyerson, Å., Ett besök vid Stora Kopparberget och Sala gruva år 1662. (BBV 23 (1938), s. 325-343.)")
    ('Meyerson, Å.', 'Ett besök vid Stora Kopparberget och Sala gruva år 1662', None, 's. 325-343', 'BBV 23 (1938).')

    >>> parse_note('Davidsson, Åke, "En hoop Discantzböcker i godt förhwar...". Nyköping, 1976, s. 48-62')
    ('Davidsson, Åke', '"En hoop Discantzböcker i godt förhwar..."', None, 's. 48-62', 'Nyköping, 1976')

    >>> parse_note('Davidsson, Åke, "En hoop Discantzböcker i godt förhwar..." : någotom Strängnäsgymnasiets musiksamling under 1600-talet. - I: Frånbiskop Rogge till Roggebiblioteket. Nyköping, 1976, s. 48-62')
    ('Davidsson, Åke', '"En hoop Discantzböcker i godt förhwar..."', 'någotom Strängnäsgymnasiets musiksamling under 1600-talet', 's. 48-62', 'I: Frånbiskop Rogge till Roggebiblioteket. Nyköping, 1976')

    >>> parse_note('The Swedish pioneer, ISSN0039-7326, 27, 1976:3, s. 215-221')
    (None, 'The Swedish pioneer, ISSN0039-7326, 27, 1976:3', None, 's. 215-221', None)

    >>> parse_note('Jonsson, Inge, Swedenborg : sökaren i naturens och andens värld :hans verk och efterföljd / Inge Jonsson, Olle Hjern. -Stockholm, 1976. - 187 s.Rec. i SP 21.4.1977 av 6. Hillerdal; i NT-ÖD 29.4.1977 av S.Stolpe; i DN 11.11.1977 av I. Algulin')
    ('Jonsson, Inge', 'Swedenborg', 'sökaren i naturens och andens värld :hans verk och efterföljd', '187 s.', 'Inge Jonsson, Olle Hjern. -Stockholm, 1976. -Rec. i SP 21.4.1977 av 6. Hillerdal; i NT-ÖD 29.4.1977 av S.Stolpe; i DN 11.11.1977 av I. Algulin')

    >>> parse_note('Fries, Elias, Hembygdsperiodika : förteckning över periodiskaskrifter samt skriftserier utgivna t.o.m. 1974 av hembygds- ochfornminnesföreningar samt länsmuseer m.fl. - Borås, 1976. - 40 bl. -(Specialarbete / Bibliotekshögskolan, ISSN 0347-1128 ; 1976:158)')
    ('Fries, Elias', 'Hembygdsperiodika', 'förteckning över periodiskaskrifter samt skriftserier utgivna t.o.m. 1974 av hembygds- ochfornminnesföreningar samt länsmuseer m.fl', '40 bl.', 'Borås, 1976. -(Specialarbete / Bibliotekshögskolan, ISSN 0347-1128 ; 1976:158)')

    >>> parse_note('Edvardsson, Lars, Kyrka och judendom : svensk judemission medsärskild hänsyn till Svenska israelmissionens verksamhet 1875-1975. -Lund, 1976. - 194 s. - (Bibliotheca historico-ecclesiasticaLundensis, ISSN 0346-5438 ; 6). - Diss. Hit deutscher ZusammenfassungRec. i Kyrkohistorisk årsskrift 1976 av I. Brohed')
    ('Edvardsson, Lars', 'Kyrka och judendom', 'svensk judemission medsärskild hänsyn till Svenska israelmissionens verksamhet 1875-1975', '194 s.', 'Lund, 1976. - (Bibliotheca historico-ecclesiasticaLundensis, ISSN 0346-5438 ; 6). - Diss. Hit deutscher ZusammenfassungRec. i Kyrkohistorisk årsskrift 1976 av I. Brohed')

    >>> parse_note('Frithz, Carl-Gösta, Till frågan om det s.k. Kelgeandshusmissaletsliturgihistoriska ställning. - Lund, 1976. - 428 s. - (Bibliothecatheologiae practicae, ISSN 0519-9859 ; 34) - Oiss. Mit deutscherZusammenfassungRec')
    ('Frithz, Carl-Gösta', 'Till frågan om det s.k. Kelgeandshusmissaletsliturgihistoriska ställning', None, '428 s.', 'Lund, 1976. - (Bibliothecatheologiae practicae, ISSN 0519-9859 ; 34) - Oiss. Mit deutscherZusammenfassungRec')

    Testet failar just nu p.g.a. "Slott, Svenska" misstas för titeln. Mödan värt att fixa?
    >>> parse_note('Slott, Svenska, och herresäten vid 1900-talets början. Bd l-5. 4;o. Med många illustr. i texten. Sthlm 1008-14. Arbetet omfattar följande landskap: Blekinge, Halland. Nerike, Skåne, Småland, Södermanland, Uppland, \"Värmland, Västergötland, Västmanland o. Östergötland. För öfrigt hänvisas till de årliga bibliografierna som upptaga fullständiga förteckningar öfver alla slotten och dem som författat uppsatserna. Rec. i Nord. tidskr. (Letterst.) 1910, s. 75-80 af L. Looström. - Ny följd. H. 1-13. Sthlm 1918-20. De utkomna häftena omfatta följande landskap: Nerike, Skåne, Småland, Södermanland, Uppland, Västmanland o. Östej götland.')
    (None, 'Slott, Svenska, och herresäten vid 1900-talets början.', None, None, 'Bd l-5. 4;o. Med många illustr. i texten. Sthlm 1008-14. Arbetet omfattar följande landskap: Blekinge, Halland. Nerike, Skåne, Småland, Södermanland, Uppland, \"Värmland, Västergötland, Västmanland o. Östergötland. För öfrigt hänvisas till de årliga bibliografierna som upptaga fullständiga förteckningar öfver alla slotten och dem som författat uppsatserna. Rec. i Nord. tidskr. (Letterst.) 1910, s. 75-80 af L. Looström. - Ny följd. H. 1-13. Sthlm 1918-20. De utkomna häftena omfatta följande landskap: Nerike, Skåne, Småland, Södermanland, Uppland, Västmanland o. Östej götland.')
    """

    # Extrct extent (pages, leaves)
    extent, remainder, is_component_part = extract_extent(note, is_component_part)

    personnameparts = []
    initial = -1
    comma_separated = remainder.split(",")
    name = comma_separated.pop(0).strip()

    # A last name containing a period is probably a title
    if not re.fullmatch(r"[\w\s'-]+", name):
        comma_separated.insert(0, name)
        name = ""
    else:
        if comma_separated:
            first = comma_separated[0]
            if (
                looks_like_initial(first.strip())
                or re.fullmatch(r"[\w\s'-.]+", first)
                and re.fullmatch(r"[\w\s'-]+", name)
            ):
                name += ", " + comma_separated.pop(0).strip()
            else:
                comma_separated.insert(0, name)
                name = ""

    # If name contains more than letters and certain punctuation, it's probably not a name
    if not re.fullmatch(r"[A-Za-zÀ-ÖØ-öø-ÿ\s',.\-\[\]]+", name.strip()):
        name = None
    else:
        remainder = ",".join(comma_separated).strip()

    # Extract title and subtitle
    title, subtitle, remainder = extract_title_and_subtitle(remainder, dotnote)

    return (
        name,
        title.strip(),
        subtitle.strip() or None,
        extent or None,
        remainder.strip() or None,
        is_component_part,
    )


def extract_title_and_subtitle(
    remainder: dict, dotnote: bool
) -> tuple[dict, dict, dict]:
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
            if dotnote and ". " in remainder
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
    """
    >>> extract_partof_from_parenthesis('isborgs slott. (Antikvariska studier. 4. Sthlm 1950, s. 221-287.) Stockholm', 'era_2')
    ('isborgs slott.  Stockholm', 'Antikvariska studier. 4. Sthlm 1950, s. 221-287.')
    >>> extract_partof_from_parenthesis('isborgs slott. (Antikvariska studier. 4. Sthlm 1950, s. 221-287. (VHAAH 71.))', 'era_2')
    ('isborgs slott.', 'Antikvariska studier. 4. Sthlm 1950, s. 221-287. (VHAAH 71.)')
    >>> extract_partof_from_parenthesis('isborgs slott. (Antikvariska studier. 4. Sthlm 1950, s. 221-287. VHAAH 71.))', 'era_2')
    ('isborgs slott. (Antikvariska studier. 4. Sthlm 1950, s. 221-287. VHAAH 71.))', None)
    >>> extract_partof_from_parenthesis('isborgs slott.', 'era_2')
    ('isborgs slott.', None)
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
        remainder = (left_part + right_part).strip(",;-").replace("- -", "-")

        pages = page_match.group(0)

        if "-" in pages:
            is_component_part = True

    return pages, remainder, is_component_part


def extract_issn(value: str) -> str:
    ISSN_PATTERN = re.compile(r"\d{4}-\d{3}[\dX]")

    issn = re.findall(ISSN_PATTERN, value)

    if len(issn) == 1 and valid_issn(issn[0]):
        return issn[0]


### Functions for enriching the records ###


def add_sao_headings(shb_part_num, start_year, publ_year, subject_mappings) -> list:
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


def link_local_entities(thing: dict, rec: dict, partnum: str) -> None:
    # TODO Review that this is doing what we want it to
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
            if iri in biblios:
                existing_host_desc = biblios[iri]
                has_biblio_repr = json.dumps(existing_host_desc, sort_keys=True)
                new_biblio_repr = json.dumps(host, sort_keys=True)
                if has_biblio_repr != new_biblio_repr:
                    bigslice = int(len(has_biblio_repr) * 0.8)
                    assert (
                        has_biblio_repr[:bigslice] == new_biblio_repr[:bigslice]
                    ), f"Mismatch:\n{has_biblio_repr}\n{new_biblio_repr}"
            else:
                biblios[iri] = host

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
    if isinstance(obj, dict):
        for key, value in obj.items():
            path = f"{prefix}.{key}" if prefix else key
            yield path
            yield from walk_keys(value, path)

    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            yield from walk_keys(item, f"{prefix}[{i}]")


def identify_syntax_era(instance) -> str:

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


def pretty_print_sample(lines, biblios, subject_mappings, context_file):
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


def make_subject_mappings(subject_mapping_sheets: list[str]) -> dict:
    subject_mappings: dict = {}

    for sheet_file in subject_mapping_sheets:
        _load_subject_mappings(subject_mappings, Path(sheet_file))

    return subject_mappings


def _load_subject_mappings(subject_mappings: dict, sheet_file: Path) -> None:
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


def write_reports(
    counters: dict, anomalies: list, report_file: TextIOBase, anomalies_file: TextIOBase
) -> None:
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

    subject_files = list(Path(args.subject_mapping_sheets).glob("*.csv"))
    if not subject_files:
        raise FileNotFoundError("No CSV files found")
    subject_mappings = make_subject_mappings(subject_files)

    biblios: dict = {}

    with open(args.infile) as infile, open(
        args.outfile, "w", encoding="utf-8"
    ) as outfile, open(args.report_file, "w", encoding="utf-8") as report_file, open(
        args.info_and_errors_file, "w", encoding="utf-8"
    ) as anomalies_file:

        if args.sample_pretty_with:
            pretty_print_sample(
                infile, biblios, subject_mappings, args.sample_pretty_with
            )
            sys.exit()
        for i, line in enumerate(infile):
            if i % 1000 == 0:
                print(f"Processing record {i+1}")
            #if i + 1 == 12316:
            #    print(f"Processing record {i+1}")
            if data := convert(json.loads(line), biblios, subject_mappings):
                json.dump(data, outfile, ensure_ascii=False)
                outfile.write("\n")

        # Write some reports
        write_reports(counters, anomalies, report_file, anomalies_file)
