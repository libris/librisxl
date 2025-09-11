import json
import csv
import sys

KBV = 'https://id.kb.se/vocab/'

agents: dict[str, dict] = {}

fname = sys.argv[1]

with open(fname) as f:
    reader = csv.DictReader(f)

    for i, row in enumerate(reader):
        (agent_iri, name, given, lifespan, alt_name, alt_given, id, idtype, work_title, orig_title, title, isbn) = [
            row[key] for key in
            ['agent', 'name', 'given', 'lifespan', 'alt_name', 'alt_given', 'id', 'idtype', 'work_title', 'orig_title', 'title', 'isbn']
        ]

        agent = agents.setdefault(agent_iri, {})
        if given:
            agent['family'] = name
            agent['given'] = given
        elif name:
            agent['name'] = name

        if lifespan:
            agent['lifespan'] = lifespan

        if alt_name:
            variant: dict[str, str] = {}
            variants = agent.setdefault('variant', []).append(variant)
            if alt_given:
                variant['family'] = alt_name
                variant['given'] = alt_given
            elif name:
                variant['name'] = alt_name

        if id:
            idtype = idtype.removeprefix(KBV)
            agent.setdefault('idMap', {}).setdefault(idtype, id)

        for title in (work_title, orig_title, title):
            if title:
                agent.setdefault('work', []).append(title)
        if isbn:
            agent.setdefault('isbn', []).append(isbn)

    json.dump(agents, sys.stdout, indent=2, ensure_ascii=False)
