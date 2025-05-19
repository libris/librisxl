import json
import sys

with open(sys.argv[1]) as normalized_with_category:
    records = []
    for r in normalized_with_category:
        records.append(json.loads(r))
for n, rec in enumerate(records, 1):
    print(f"\n{n}.\t", rec["@graph"][1]["instanceOf"]["hasTitle"][0]["mainTitle"])
