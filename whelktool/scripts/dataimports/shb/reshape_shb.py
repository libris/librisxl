import csv
import json
import re
import sys
from pathlib import Path
from collections import Counter

USE_ANNOT = False

property_counts = Counter()
subject_counts = Counter()
extent_counts = Counter()
curiosity_counts = Counter()
oddities = []


def convert(data, biblios: dict, subject_mappings) -> dict | None:

    # Set start_year and publ_year to None
    start_year: str | None = None
    publ_year: str | None = None

    # Get the record and main entity
    graph = data["@graph"]
    rec, instance, *remainder = graph

    # Extract information about the source SHB volume
    if "marc:primaryProvisionActivity" in instance and "publication" in instance:
        publ = instance["publication"][0]
        start_year = publ.get("startYear")
        publ_year = publ.get("endYear") or publ.get("year")
        if publ_year == instance["marc:primaryProvisionActivity"]["year"]:
            del instance["marc:primaryProvisionActivity"]

    shb_part_num = instance.pop("part")[0]
    iri: str | None = None
    source: dict | None = None

    if "bibliography" in rec:
        rec["bibliography"] = [{"@id": it["@id"]} for it in rec["bibliography"]]

    del rec["marc:catalogingSource"]  # "Annan verksamhet"
    # del rec['marc:typeOfControl']

    # Extract work data from the graph and place in instanceOf
    if remainder:
        work = remainder[0]
        assert work["@id"].endswith("#work")
        del work["@id"]
        del graph[2]
    else:
        work = {"@type": "Work"}

    instance["instanceOf"] = work

    ### Try to link local entities
    link_local_entities(instance, rec, shb_part_num)

    #### Parse notes
    if "hasNote" in instance:
        assert len(instance["hasNote"]) == 1
        original_note = instance.pop("hasNote")[0]["label"]
        note = original_note.replace("—", "-").replace(" ", " ").replace("  ", " ")

        if note == "TABORT":
            return None

        # Separate information concerning the thing itself
        # from information concerning a publication/series it is part of
        PARTOF_MARK = " - I:"
        if PARTOF_MARK in note:
            note, partof_note = note.split(PARTOF_MARK, 1)
        else:
            partof_note = None

        # Parse the "main" note
        name, title, subtitle, extent, note_remainder = parse_note(note)

        instance["hasTitle"] = {"@type": "Title", "mainTitle": title}

        ### Add information from the parsed note to the instance ###

        if subtitle:
            instance["hasTitle"]["subtitle"] = subtitle

        if name:
            instance["responsibilityStatement"] = name

        if extent:
            instance["extent"] = [{"@type": "Extent", "label": extent}]

            if "-" in extent:
                extent_counts.update(["Part extent (e.g. 's. 23-31')"])
            else:
                extent_counts.update(["Monographic extent (e.g. 47)"])
                # Remove category "componentPart"
                del instance["category"]

        if note_remainder:
            issn = extract_issn(note_remainder)
            if issn:
                instance["issn_from_note"] = issn

        if original_note:
            instance["hasNote"] = [
                {
                    "@type": "Note",
                    "label": f"Fullständig beskrivning (OCR) ur SHBD: {original_note}",
                }
            ]
            instance["hasNote"].append({"@type": "Note", "label": note_remainder})


        # Parse the "part of" note (eg series, containing publication)
        if partof_note:
            part_name, part_title, part_subtitle, part_extent, part_remainder = (
                parse_note(partof_note, dotnote=False)
            )
            # assert not subtitle
            # TODO Deceide whether the thing in partOf is issue or series
            issue = {"@type": "Instance", "label": part_title}
            if part_name:
                issue["responsibilityStatement"] = part_name
            if part_title:
                part_issn = extract_issn(part_title)
                if part_issn:
                    issue["identifiedBy"] = {"@type": "ISSN", "value": part_issn}
            if part_remainder:
                if USE_ANNOT:
                    issue["@annotation"] = {"comment": part_remainder}
                else:
                    issue["label"] += " {part_remainder}"
                    # raise NotImplementedError  # TODO
            instance["partOf"] = issue

        # TODO: remove 'ComponentPart', only re-add for obviously paginated,
        # partOf:s and/or "newspaper reference"?

    ### Add subject headings to the instance ###
    # TODO Add SAB as well
    sao_headings = add_sao_headings(shb_part_num, start_year, publ_year)
    if sao_headings:
        work["subjects"] = sao_headings

    else:
        curiosity_counts.update(["Missing SHB reference!"])
        oddities.append(f"Missing SHB reference\t{instance}")

    property_counts.update(walk_keys(instance))

    return {"@id": graph[0]["@id"], "@graph": data}


def add_sao_headings(shb_part_num, start_year, publ_year) -> list:
    years_key = f"{start_year}-{publ_year}" if start_year else publ_year
    if rownummap := subject_mappings.get(years_key):
        if subjectrefs := rownummap.get(shb_part_num):  # TODO: opt + 'a' ...
            work_subjects = [{"@id": s} for s in subjectrefs]
            subject_counts.update(subjectrefs)

            return work_subjects

            # if USE_ANNOT and iri:
            #    for s in work_subjects:
            #        s["@annotation"] = {"source": source}
        # else:
        #    print(f"{partnum} not in {list(rownummap)} for {years_key}", file=sys.stderr)


def link_local_entities(thing: dict, rec: dict, partnum: str) -> None:
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


def parse_note(note: dict, dotnote: bool = True):
    """
    >>> parse_note("Surname, G.-N., Anything")
    ('Surname, G.-N.', 'Anything', None, None, None)

    >>> parse_note("Anything. Surname, G.-N., Stuff.")
    (None, 'Anything', None, None, 'Surname, G.-N., Stuff.')

    >>> parse_note("Schuck, A., H. Schücks enka & Co. AB 150 år. [Stockholm.] Sthlm 1947, 28 s.")
    ('Schuck, A.', 'H. Schücks enka & Co. AB 150 år', None, '28 s.', '[Stockholm.] Sthlm 1947')

    >>> parse_note("Meyerson, Å., Ett besök vid Stora Kopparberget och Sala gruva år 1662. (BBV 23 (1938), s. 325-343.)")
    ('Meyerson, Å.', 'Ett besök vid Stora Kopparberget och Sala gruva år 1662', None, 's. 325-343', 'BBV 23 (1938)')

    >>> parse_note('Davidsson, Åke, "En hoop Discantzböcker i godt förhwar...". Nyköping, 1976, s. 48-62')
    ('Davidsson, Åke', '"En hoop Discantzböcker i godt förhwar..."', None, 's. 48-62', 'Nyköping, 1976')

    >>> parse_note('Davidsson, Åke, "En hoop Discantzböcker i godt förhwar..." : någotom Strängnäsgymnasiets musiksamling under 1600-talet. - I: Frånbiskop Rogge till Roggebiblioteket. Nyköping, 1976, s. 48-62')
    ('Davidsson, Åke', '"En hoop Discantzböcker i godt förhwar..."', 'någotom Strängnäsgymnasiets musiksamling under 1600-talet', 's. 48-62', 'I: Frånbiskop Rogge till Roggebiblioteket. Nyköping, 1976')

    >>> parse_note('The Swedish pioneer, ISSN0039-7326, 27, 1976:3, s. 215-221')
    (None, 'The Swedish pioneer, ISSN0039-7326, 27, 1976:3', None, 's. 215-221', None)

    >>> parse_note('Jonsson, Inge, Swedenborg : sökaren i naturens och andens värld :hans verk och efterföljd / Inge Jonsson, Olle Hjern. -Stockholm, 1976. - 187 s.Rec. i SP 21.4.1977 av 6. Hillerdal; i NT-ÖD 29.4.1977 av S.Stolpe; i DN 11.11.1977 av I. Algulin')
    ('Jonsson, Inge', 'Swedenborg', 'sökaren i naturens och andens värld :hans verk och efterföljd', '187 s.', 'Inge Jonsson, Olle Hjern. -Stockholm, 1976. - Rec. i SP 21.4.1977 av 6. Hillerdal; i NT-ÖD 29.4.1977 av S.Stolpe; i DN 11.11.1977 av I. Algulin')

    >>> parse_note('Fries, Elias, Hembygdsperiodika : förteckning över periodiskaskrifter samt skriftserier utgivna t.o.m. 1974 av hembygds- ochfornminnesföreningar samt länsmuseer m.fl. - Borås, 1976. - 40 bl. -(Specialarbete / Bibliotekshögskolan, ISSN 0347-1128 ; 1976:158)')
    ('Fries, Elias', 'Hembygdsperiodika', 'förteckning över periodiskaskrifter samt skriftserier utgivna t.o.m. 1974 av hembygds- ochfornminnesföreningar samt länsmuseer m.fl', '40 bl.', 'Borås, 1976. -(Specialarbete / Bibliotekshögskolan, ISSN 0347-1128 ; 1976:158)')

    >>> parse_note('Edvardsson, Lars, Kyrka och judendom : svensk judemission medsärskild hänsyn till Svenska israelmissionens verksamhet 1875-1975. -Lund, 1976. - 194 s. - (Bibliotheca historico-ecclesiasticaLundensis, ISSN 0346-5438 ; 6). - Diss. Hit deutscher ZusammenfassungRec. i Kyrkohistorisk årsskrift 1976 av I. Brohed')
    ('Edvardsson, Lars', 'Kyrka och judendom', 'svensk judemission medsärskild hänsyn till Svenska israelmissionens verksamhet 1875-1975', '194 s.', 'Lund, 1976. - (Bibliotheca historico-ecclesiasticaLundensis, ISSN 0346-5438 ; 6). - Diss. Hit deutscher ZusammenfassungRec. i Kyrkohistorisk årsskrift 1976 av I. Brohed')

    >>> parse_note('Frithz, Carl-Gösta, Till frågan om det s.k. Kelgeandshusmissaletsliturgihistoriska ställning. - Lund, 1976. - 428 s. - (Bibliothecatheologiae practicae, ISSN 0519-9859 ; 34) - Oiss. Mit deutscherZusammenfassungRec')
    ('Frithz, Carl-Gösta', 'Till frågan om det s.k. Kelgeandshusmissaletsliturgihistoriska ställning', None, '428 s.', 'Lund, 1976. - (Bibliothecatheologiae practicae, ISSN 0519-9859 ; 34) - Oiss. Mit deutscherZusammenfassungRec')

    """

    # Extrct extent (pages, leaves)
    extent, remainder = extract_extent(note)

    personnameparts = []
    initial = -1
    comma_separated = remainder.split(",")
    name = comma_separated.pop(0).strip()

    if comma_separated:
        first = comma_separated[0]
        if looks_like_initial(first.strip()) or re.fullmatch(r"[\w\s'.-]+", first):
            name += ", " + comma_separated.pop(0).strip()
        else:
            comma_separated.insert(0, name)
            name = ""

    # If name contains more than a certain set of characters, it's probably not a name
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
        remainder.strip(" ,.") or None,
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
            curiosity_counts.update(["STRUCTURE\t Title / Author. - Publication"])
            title, author_area = title_and_author_area.split(" / ", 1)
            # TODO Do we want to fetch the author(s) for responsibilityStatememnt from here?
            remainder = author_area + ". -" + remainder

        # If the title is directly followed by a ". -", signalling other publication information is next
        elif re.search(r"[0-9].?\(", title_and_author_area):
            curiosity_counts.update(
                ["STRUCTURE\t note doesn't start with author/title'"]
            )
            title = title_and_author_area + ". - " + remainder
        else:
            curiosity_counts.update(
                ["STRUCTURE\t'Title. - Publication' between title and next area"]
            )
            title = title_and_author_area

    # If author and title are followed by publication information in parenthesis
    elif remainder.endswith(")"):
        curiosity_counts.update(["STRUCTURE\tAuthor, title . (publication)"])
        opencount = 0
        for i, c in enumerate(remainder[::-1]):
            if c == ")":
                opencount += 1
            if c == "(":
                opencount -= 1
                if opencount == 0:
                    break
        title, remainder = remainder[: -i - 1], remainder[-i:-1]

    # Another way to get the title
    else:
        curiosity_counts.update(["STRUCTURE\tOther structure"])
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


def extract_extent(remainder: str) -> tuple[str]:
    pages = ""
    page_match = re.search(
        r"(\d+)(?:-(\d+))?\s*((?:s|bl)\.?)(?:\s+(: ill\.))?", remainder
    )

    if not page_match:
        page_match = re.search(
            r"\b((?:s|bl)\.?)\s*(\d+)(?:-(\d+))?(?:\s+(ill\.?))?", remainder
        )

    if page_match:
        # Remove the matched part
        remainder = (
            (remainder[: page_match.start()] + remainder[page_match.end() :])
            .strip(" ,;-")
            .replace("-  -", "-")
        )
        pages = page_match.group(0)

    return pages, remainder


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
                    print(
                        "Missing startnum in",
                        sheet_file,
                        "row",
                        i,
                        row,
                        file=sys.stderr,
                    )
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


def extract_issn(value: str) -> str:
    ISSN_PATTERN = re.compile(r"\d{4}-\d{3}[\dX]")

    issn = re.findall(ISSN_PATTERN, value)

    if len(issn) == 1 and valid_issn(issn[0]):
        return issn[0]


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


def walk_keys(obj: dict, prefix: str = ""):
    if isinstance(obj, dict):
        for key, value in obj.items():
            path = f"{prefix}.{key}" if prefix else key
            yield path
            yield from walk_keys(value, path)


if __name__ == "__main__":
    import argparse

    argp = argparse.ArgumentParser()
    argp.add_argument("-t", "--test", action="store_true", default=False)
    argp.add_argument("--sample-pretty-with")
    argp.add_argument("infile")
    argp.add_argument("subject_mapping_sheets")
    argp.add_argument("report_file")
    argp.add_argument("info_and_errors_file")
    args = argp.parse_args()

    if args.test:
        import doctest

        doctest.testmod()
        sys.exit(0)

    subject_files = list(Path(args.subject_mapping_sheets).glob("*.csv"))
    if not subject_files:
        raise FileNotFoundError("No CSV files found")

    subject_mappings = make_subject_mappings(subject_files)

    biblios: dict = {}

    with open(args.infile) as f, open(
        args.report_file, "w", encoding="utf-8"
    ) as report, open(args.info_and_errors_file, "w", encoding="utf-8") as info:

        if args.sample_pretty_with:
            pretty_print_sample(f, biblios, subject_mappings, args.sample_pretty_with)
            sys.exit()
        for l in f:
            if data := convert(json.loads(l), biblios, subject_mappings):
                print(json.dumps(data, ensure_ascii=False))

        # Write some reports
        report.write("# Egenskaper\n\n")
        report.write("| Egenskap | Antal |\n")
        report.write("|----------|-------:|\n")

        for prop, count in property_counts.most_common():
            report.write(f"| {prop} | {count} |\n")

        report.write("\n\n")

        report.write("# Kuriositeter\n\n")
        report.write("| Kuriositet | Antal |\n")
        report.write("|----------|-------:|\n")

        for curiosity, count in curiosity_counts.most_common():
            report.write(f"| {curiosity} | {count} |\n")

        report.write("\n\n")

        report.write("# Omfång\n\n")
        report.write("| Omfång | Antal |\n")
        report.write("|----------|-------:|\n")

        for extent, count in extent_counts.most_common():
            report.write(f"| {extent} | {count} |\n")

        report.write("\n\n")

        report.write("# Ämnesord\n\n")
        report.write("| Ämne | Antal |\n")
        report.write("|----------|-------:|\n")

        for subject, count in subject_counts.most_common():
            report.write(f"| {subject} | {count} |\n")

        print(*oddities, sep="\n", file=info)
