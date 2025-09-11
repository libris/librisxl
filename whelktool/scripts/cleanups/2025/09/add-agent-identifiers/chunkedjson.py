import json
import csv
import sys

KBV = 'https://id.kb.se/vocab/'

agents: dict[str, dict] = {}

fname = sys.argv[1]

with open(fname) as f:
    reader = csv.DictReader(f)

    for i, row in enumerate(reader):
        agent_iri = row['agent']

        agent = agents.setdefault(agent_iri, {})
        if name := row['name']:
            if given := row['given']:
                agent['family'] = name
                agent['given'] = given
            else:
                agent['name'] = name

        if lifespan := row['lifespan']:
            agent['lifespan'] = lifespan

        if alt_name := row['alt_name']:
            variant: dict[str, str] = {}
            variants = agent.setdefault('variant', []).append(variant)
            if alt_given := row['alt_given']:
                variant['family'] = alt_name
                variant['given'] = alt_given
            elif name:
                variant['name'] = alt_name

        if id := row['id']:
            idtype = row['idtype'].removeprefix(KBV)
            agent.setdefault('idMap', {}).setdefault(idtype, id)

        for title_key in ('work_title', 'orig_title', 'title'):
            if title := row[title_key]:
                agent.setdefault('work', []).append(title)
        if isbn := row['isbn']:
            agent.setdefault('isbn', []).append(isbn)

    json.dump(agents, sys.stdout, indent=2, ensure_ascii=False)
