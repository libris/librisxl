from __future__ import unicode_literals, print_function
import json

with open('/tmp/work.json') as f:
    workmap = json.load(f)

print("""<!DOCTYPE html>
<title>Verkskluster</title>
<h1>Verkskluster</h1>
<div>""")
for work_key, instances in sorted(workmap.items(), key=lambda it: it[1][0][0]):
    if not work_key.startswith('A '):
        continue
    print(f"""<p>""")
    print(f""" <i>{work_key}</i><br>""")
    for title, authname, link, itype, level in instances:
        print(f""" <a href="{link}">[{itype}] {title} ({authname})</a><br>""")
    print(f"""</p>""")

print(f"""</div>""")
