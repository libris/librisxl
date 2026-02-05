import csv
import json
import re
import sys
from pathlib import Path

USE_ANNOT = True


def convert(data, biblios: dict, subject_mappings) -> dict | None:
    graph = data['@graph']
    rec, thing, *rest = graph

    if 'bibliography' in rec:
        rec['bibliography'] = [{'@id': it['@id']} for it in rec['bibliography']]

    del rec['marc:catalogingSource']  # "Annan verksamhet"
    del rec['marc:typeOfControl']

    if rest:
        work = rest[0]
        assert work['@id'].endswith('#work')
        del work['@id']
        del graph[2]
    else:
        work = {'@type': 'Work'}

    thing['instanceOf'] = work

    startyear: str | None = None
    publ_year: str | None = None

    if 'marc:primaryProvisionActivity' in thing and 'publication' in thing:
        publ = thing['publication'][0]
        startyear = publ.get('startYear')
        publ_year = publ.get('endYear') or publ.get('year')
        if publ_year == thing['marc:primaryProvisionActivity']['year']:
            del thing['marc:primaryProvisionActivity']

    partnum = thing.pop("part")[0]
    iri: str | None = None
    source: dict | None = None

    if hosts := thing.pop('isPartOf'):
        assert len(hosts) == 1
        host = hosts[0]

        if 'associatedMedia' in thing:
            host['associatedMedia'] = [
                {
                    "@id": am['uri'][0].replace(
                        'http://regina.kb.se/shb/', 'https://shb.kb.se/'
                    )
                }
                for am in thing.pop('associatedMedia')
            ]

        if 'publication' in thing:
            host['hasTemporalCoverage'] = thing.pop('publication')[0]
            host['hasTemporalCoverage']['@type'] = 'TemporalCoverage'

        hostrecs = host.pop('describedBy')
        assert len(hostrecs) == 1

        if ctrlnr := hostrecs[0].get('controlNumber'):
            iri = f"http://libris.kb.se/resource/bib/{ctrlnr}"
            host['@id'] = iri
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
            for biblioref in rec['bibliography']:
                bibliography_shb = 'https://libris.kb.se/library/SHB'
                if biblioref['@id'] == bibliography_shb:
                    source_id = f"{rec['@id']}#{partnum}"
                    source = {
                        "@id": source_id,
                        "@type": "Source",
                        "isPartOf": {'@id': iri},
                        "item": partnum,
                    }
                    # biblios[source_id] = source
                    rec['describes'] = source
                    annot: dict = {"source": {"@id": source_id}}
                    if USE_ANNOT:
                        biblioref['@annotation'] = annot
                    else:
                        annot["_object"] = {'@id': bibliography_shb}
                        rec.setdefault('_statementBy', {})['bibliography'] = annot
                    break

    if 'hasNote' in thing:
        assert len(thing['hasNote']) == 1
        note = thing.pop('hasNote')[0]['label']

        note = note.replace('—', '-').replace(' ', ' ').replace('  ', ' ')

        if note == 'TABORT':
            return None

        PARTOFMARK = ' - I:'
        if PARTOFMARK in note:
            note, partofnote = note.split(PARTOFMARK, 1)
        else:
            partofnote = None

        name, title, subtitle, rest = parse_note(note)

        thing['hasTitle'] = {"@type": "Title", "mainTitle": title}
        if subtitle:
            thing['hasTitle']['subtitle'] = subtitle

        thing['responsibilityStatement'] = name
        if rest:
            thing['hasNote'] = [{"@type": "Note", "label": rest}]

        if partofnote:
            pname, ptitle, subtitle, prest = parse_note(partofnote, dotnote=False)
            # assert not subtitle
            issue = {
                "@type": "Issue",
                "label": ptitle,
            }
            if pname:
                issue["responsibilityStatement"] = pname
            if prest:
                if USE_ANNOT:
                    issue["@annotation"] = {"comment": prest}
                else:
                    raise NotImplementedError  # TODO

            thing['partOf'] = issue

        # TODO: remove 'ComponentPart', only re-add for obviously paginated,
        # partOf:s and/or "newspaper reference"?

    if partnum and publ_year:
        years_key = f'{startyear}-{publ_year}' if startyear else publ_year
        if rownummap := subject_mappings.get(years_key):
            if subjectrefs := rownummap.get(partnum):  # TODO: opt + 'a' ...
                work_subjects = work.setdefault('subject', [])
                work_subjects += [{'@id': s} for s in subjectrefs]
                if USE_ANNOT and iri:
                    for s in work_subjects:
                        s['@annotation'] = {"source": source}
            # else:
            #    print(f"{partnum} not in {list(rownummap)} for {years_key}", file=sys.stderr)

    return {'@id': graph[0]['@id'], '@graph': data}


def parse_note(note, dotnote=True):
    """
    >>> parse_note("Surname, G.-N., Anything")
    ('Surname, G.-N.', 'Anything', None, None)

    >>> parse_note("Anything. Surname, G.-N., Stuff.")
    (None, 'Anything', None, 'Surname, G.-N., Stuff.')

    >>> parse_note("Schuck, A., H. Schücks enka & Co. AB 150 år. [Stockholm.] Sthlm 1947, 28 s.")
    ('Schuck, A.', 'H. Schücks enka & Co. AB 150 år', None, '[Stockholm.] Sthlm 1947, 28 s.')

    >>> parse_note("Meyerson, Å., Ett besök vid Stora Kopparberget och Sala gruva år 1662. (BBV 23 (1938), s. 325-343.)")
    ('Meyerson, Å.', 'Ett besök vid Stora Kopparberget och Sala gruva år 1662', None, 'BBV 23 (1938), s. 325-343.')

    >>> parse_note('Davidsson, Åke, "En hoop Discantzböcker i godt förhwar...". Nyköping, 1976, s. 48-62')
    ('Davidsson, Åke', '"En hoop Discantzböcker i godt förhwar..."', None, 'Nyköping, 1976, s. 48-62')

    >>> parse_note('Davidsson, Åke, "En hoop Discantzböcker i godt förhwar..." : någotom Strängnäsgymnasiets musiksamling under 1600-talet. - I: Frånbiskop Rogge till Roggebiblioteket. Nyköping, 1976, s. 48-62')
    ('Davidsson, Åke', '"En hoop Discantzböcker i godt förhwar..."', None, 'någotom Strängnäsgymnasiets musiksamling under 1600-talet. - I: Frånbiskop Rogge till Roggebiblioteket. Nyköping, 1976, s. 48-62')

    >>> parse_note('The Swedish pioneer, ISSN0039-7326, 27, 1976:3, s. 215-221')
    (None, 'The Swedish pioneer, ISSN0039-7326, 27, 1976:3', None, 's. 215-221')
    """
    ()
    personnameparts = []
    initial = -1
    commaseparated = note.split(',')
    name = commaseparated.pop(0).strip()
    rest = ''

    if ' ' in name.strip():
        name = None
        rest = note

    elif commaseparated:
        first = commaseparated[0]
        if looks_like_initial(first.strip()) or re.match(r'^(\w|\s)+$', first):
            name += ', ' + commaseparated.pop(0).strip()
        else:
            commaseparated.insert(0, name)
            name = None

        rest = ','.join(commaseparated).strip()

    subtitle = ""

    if ' : ' in rest:
        title, rest = rest.split(' : ', 1)
        if ' : ' in rest:
            subtitle, rest = rest.split(' : ', 1)
    elif rest.endswith(')'):
        opencount = 0
        for i, c in enumerate(rest[::-1]):
            if c == ')':
                opencount += 1
            if c == '(':
                opencount -= 1
                if opencount == 0:
                    break
        title, rest = rest[: -i - 1], rest[-i:-1]
        title = title.strip().removesuffix('.')
    else:
        x = int(rest.count('. ') * 0.3) or 1
        title, *rest = rest.rsplit('. ', x) if dotnote and '. ' in rest else (rest, "")
        rest = '. '.join(rest)
        restispages = all(s.strip().isdigit() for s in rest.split('-'))
        if len(title.strip()) == 1:
            moretitle, rest = rest.split('.', 1) if '.' in rest else (rest, "")
            title += '.' + moretitle
        elif restispages:
            title, prerest = title.rsplit('. ', 1) if '. ' in title else (title, "")
            if not prerest:
                title, posttitle = title.rsplit(' ', 1)
                prerest = posttitle.strip()
                title = title.strip().removesuffix(',')

            rest = prerest + '. ' + rest

    return name, title.strip(), subtitle.strip() or None, rest.strip() or None


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
    return s[0].isupper() and s.endswith('.') and (len(s) == 2 or s[-2].isupper())


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
        json.dumps({'@context': ctx['@context'], '@graph': results}, ensure_ascii=False)
    )


def make_subject_mappings(subject_mapping_sheets: list[str]) -> dict:
    subject_mappings: dict = {}

    for sheet_file in subject_mapping_sheets:
        _load_subject_mappings(subject_mappings, Path(sheet_file))

    return subject_mappings


def _load_subject_mappings(subject_mappings: dict, sheet_file: Path) -> None:
    years_key = sheet_file.with_suffix('').name.split('-', 1)[-1]
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

                if x.startswith('https://id.kb.se/term/'):
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
                if startnum.endswith('a'):
                    startnum = startnum[:-1]

                if endnum:
                    endnum = endnum.strip()
                    if endnum.endswith('a'):
                        endnum = endnum[:-1]

                    for n in range(int(startnum), int(endnum) + 1):
                        rownummap[f"{n}"] = subjects
                else:
                    rownummap[f"{startnum}+"] = subjects


if __name__ == '__main__':
    import argparse

    argp = argparse.ArgumentParser()
    argp.add_argument('-t', '--test', action='store_true', default=False)
    argp.add_argument('--sample-pretty-with')
    argp.add_argument('infile')
    argp.add_argument('subject_mapping_sheets', nargs='*')
    args = argp.parse_args()

    if args.test:
        import doctest

        doctest.testmod()
        sys.exit(0)

    subject_mappings = make_subject_mappings(args.subject_mapping_sheets)

    biblios: dict = {}

    with open(args.infile) as f:
        if args.sample_pretty_with:
            pretty_print_sample(f, biblios, subject_mappings, args.sample_pretty_with)
            sys.exit()
        for l in f:
            if data := convert(json.loads(l), biblios, subject_mappings):
                print(json.dumps(data, ensure_ascii=False))
