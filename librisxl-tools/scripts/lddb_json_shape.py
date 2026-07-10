"""Compute JSON shape statistics for LDDB records fed as JSON lines on stdin.

Any collection can be piped in (bib, auth, hold, none): every record
contributes its top-level thing to the by-type index, which is written to
instance_shapes_by_type.json regardless of collection (the bib-centric file
name is legacy). The instance/work split behind instance_shapes.json and
work_shapes*.json only applies to bib records, whose things carry instanceOf;
for auth and hold input those files are legitimately empty ({}), and a missing
instanceOf is not counted as an anomaly.
"""
from __future__ import annotations
from datetime import datetime
import json
import os


MAX_STATS = int(os.environ.get('MAX_STATS', '512'))
HARD_MAX_STATS = 8192
STATS_FOR_ALL = {
        # auth
        "hasBiographicalInformation",
        "marc:hasBiographicalOrHistoricalData",
        # "shouldn't" be too many...
        "marc:displayText",
        "part",
}


def unwrap_single(value):
    # instanceOf is normally a single embedded/link object. Some real data
    # has it as a singleton list, which is safe to unwrap, while a multi
    # value list is ambiguous for the instance/work split used below.
    if isinstance(value, list):
        if len(value) == 1 and isinstance(value[0], dict):
            return value[0]
        return None
    return value


def reshape(data):
    if '@graph' in data:
        graph = data['@graph']
        thing = graph[1]
        thing['meta'] = graph[0]

        work = unwrap_single(thing.get('instanceOf'))

        # Framed dumps may keep the work as a bare {"@id": ...} reference in
        # the instance and include the actual work node later in @graph.
        # Replace the reference with the full work node so work shapes include
        # the real properties, not just an @id count.
        if isinstance(work, dict) and len(work) == 1 and '@id' in work:
            for item in graph[2:]:
                if item.get('@id') == work['@id']:
                    work = item
                    break

        # Only expose a work shape when we have an object. Scalar or ambiguous
        # instanceOf values are data-shape anomalies and should not crash the
        # whole stats run.
        if isinstance(work, dict):
            thing['instanceOf'] = work
        else:
            work = None

        return thing, work

    # Non-framed input can still have the same singleton-list oddity. Normalize
    # it here too so plain JSON-LD and @graph dumps are treated consistently.
    work = unwrap_single(data.get('instanceOf'))
    if not isinstance(work, dict):
        work = None
    return data, work


def compute_shape(node, index, type_key=None):
    # The shape walker is intentionally forgiving. It is used for forensic
    # inventory over large dumps, so unexpected list/scalar values should be
    # counted when possible or skipped, not abort the full run.
    if isinstance(node, list):
        for item in node:
            if isinstance(item, dict):
                compute_shape(item, index, type_key=type_key)
        return

    if not isinstance(node, dict):
        return

    if len(node) == 1 and '@id' in node:
        count_value('@id', node['@id'], index)
        return

    rtype = type_key or node.get('@type')
    if isinstance(rtype, list):
        rtype = '+'.join(rtype)
    # A path can be observed first as a scalar counter and later as an object
    # branch. Turn non-dict placeholders into a nested shape rather than
    # failing on mixed data, keeping an already-collapsed literal count under
    # the reserved '@collapsed' key instead of discarding it.
    shape = index.get(rtype)
    if not isinstance(shape, dict):
        shape = {'@collapsed': shape} if isinstance(shape, int) else {}
        index[rtype] = shape

    for k, vs in node.items():
        if not isinstance(vs, list):
            vs = [vs] # Ignoring dict/list difference for now

        for v in vs:
            if isinstance(v, dict):
                # Same mixed-shape problem at property level: a property can be
                # scalar in some records and structured in others. A collapsed
                # literal count is kept under '@collapsed', not thrown away.
                subindex = shape.get(k)
                if not isinstance(subindex, dict):
                    subindex = {'@collapsed': subindex} if isinstance(subindex, int) else {}
                    shape[k] = subindex
                compute_shape(v, subindex)
            else:
                count_value(k, v, shape)


def count_value(k, v, shape):
    stats = shape.get(k)
    if stats is None:
        stats = {}
        shape[k] = stats

    if isinstance(stats, dict):
        # Literal values are prefixed so they cannot collide with structural
        # keys such as "@id" or object-type buckets in the same property stats.
        if not k.startswith('@') and isinstance(v, (str, bool, int, float)):
            v = f'@value {v}'
        # Unexpected composite values are serialized into a stable string so
        # the stats remain JSON-serializable and comparable between runs.
        elif isinstance(v, (dict, list)):
            v = json.dumps(v, ensure_ascii=False, sort_keys=True)
        elif not isinstance(v, (str, bool, int, float, type(None))):
            v = repr(v)

        # If a property value already created a nested branch with the same key
        # as a literal value, keep both by forcing the literal into @value form.
        if isinstance(stats.get(v), dict):
            v = f'@value {v}'

        # Most properties only keep a bounded sample of values. A few selected
        # text-heavy fields need a larger cap because their variety is the
        # interesting signal in the audit.
        if (k in STATS_FOR_ALL and len(stats) < HARD_MAX_STATS) or len(stats) < MAX_STATS:
            curr = stats.get(v, 0)
            if isinstance(curr, int):
                stats[v] = curr + 1
            else:
                stats[v] = 1
        else:
            # Once the sample cap is reached, collapse the literal samples into
            # a single count. Nested object-shape branches under the same key
            # must survive the collapse, so when any exist, keep the dict and
            # roll the literal counts up under the reserved '@collapsed' key.
            total = sum(x for x in stats.values() if isinstance(x, int)) + 1
            branches = {sk: sv for sk, sv in stats.items() if isinstance(sv, dict)}
            if branches:
                branches['@collapsed'] = total
                shape[k] = branches
            else:
                shape[k] = total
    else:
        shape[k] = stats + 1


def isodatetime(s):
    # NOTE: fromisoformat with zulu time requires Python 3.11+
    if s.endswith('Z'):
        s = s[:-1] + '+00:00'
    return datetime.fromisoformat(s)


if __name__ == '__main__':
    from pathlib import Path
    from time import time
    import argparse
    import sys

    argp = argparse.ArgumentParser()
    argp.add_argument('-d', '--debug', action='store_true', default=False)
    argp.add_argument('-c', '--min-created')  # inclusive
    argp.add_argument('-C', '--max-created')  # exclusive
    argp.add_argument('--allow-empty-input', action='store_true', default=False,
                      help='Allow writing output files even if stdin had no lines '
                           'or no records could be processed')
    argp.add_argument('--error-log', help='Path to JSONL error log; when set explicitly, an existing non-empty '
                                          'file is refused, not overwritten. The default path '
                                          '(OUT_DIR/shape_errors.jsonl) is rewritten each run, like the shape outputs')
    argp.add_argument('--max-error-samples', type=int, default=20,
                      help='How many individual errors to print to stderr before only counting')
    argp.add_argument('outdir', metavar='OUT_DIR')

    args = argp.parse_args()

    SUFFIX = '.json'

    outpath: Path|None = Path(args.outdir)
    assert outpath

    if outpath.suffix == SUFFIX:
        outdir = outpath.parent
    else:
        outdir = outpath
        outpath = None

    if not outdir.is_dir():
        outdir.mkdir(parents=True, exist_ok=True)

    error_log_path: Path = Path(args.error_log) if args.error_log else outdir / 'shape_errors.jsonl'
    if args.error_log and error_log_path.exists() and error_log_path.stat().st_size > 0:
        print(f'ERROR: --error-log path {error_log_path} already exists and is non-empty. '
              'Remove it or pick another path.', file=sys.stderr)
        raise SystemExit(2)
    if error_log_path.parent and not error_log_path.parent.is_dir():
        error_log_path.parent.mkdir(parents=True, exist_ok=True)
    error_log = error_log_path.open('w', encoding='utf-8')

    min_inc_created: datetime | None = isodatetime(args.min_created) if args.min_created else None
    max_ex_created: datetime | None = isodatetime(args.max_created) if args.max_created else None
    if min_inc_created:
        print(f"Filter - min created (inclusive): {min_inc_created}", file=sys.stderr)
    if max_ex_created:
        print(f"Filter - max created (exclusive): {max_ex_created}", file=sys.stderr)

    index: dict = {}
    work_by_type_index: dict = {}
    instance_index: dict = {}
    work_index: dict = {}
    anomaly_counts: dict[str, int] = {}
    anomaly_log_count: dict[str, int] = {}
    MAX_ANOMALY_LOG_PER_KIND = 20
    error_counts: dict[str, int] = {}
    printed_error_samples = [0]

    def write_log(entry: dict):
        error_log.write(json.dumps(entry, ensure_ascii=False) + '\n')
        error_log.flush()

    def note_anomaly(kind: str, line_number: int, detail: dict | None = None):
        # Anomalies are suspicious-but-processable shapes. Keep aggregate counts
        # for the final summary, but cap detailed log samples so one common
        # issue does not create an enormous sidecar log.
        anomaly_counts[kind] = anomaly_counts.get(kind, 0) + 1
        anomaly_log_count[kind] = anomaly_log_count.get(kind, 0) + 1
        if anomaly_log_count[kind] <= MAX_ANOMALY_LOG_PER_KIND:
            payload = {
                'kind': 'anomaly',
                'anomaly': kind,
                'line': line_number,
            }
            if detail:
                payload['detail'] = detail
            write_log(payload)

    def note_error(exc: Exception, line_number: int | None = None, data_line: str | None = None, stage: str = 'process'):
        # Errors are written to JSONL so a long batch can finish and still leave
        # machine-readable evidence of every failure. Stderr is sampled only for
        # human feedback during the run.
        key = f'{stage}:{type(exc).__name__}'
        error_counts[key] = error_counts.get(key, 0) + 1

        payload = {
            'kind': 'error',
            'stage': stage,
            'line': line_number,
            'errorType': type(exc).__name__,
            'error': str(exc),
        }
        if data_line is not None:
            payload['data'] = data_line[:20000]
        write_log(payload)

        if printed_error_samples[0] < args.max_error_samples:
            print(f'ERROR {stage} at line {line_number}: {type(exc).__name__}: {exc}', file=sys.stderr)
            if data_line:
                print(data_line[:2000], file=sys.stderr)
            printed_error_samples[0] += 1

    t_last = 0.0
    total_lines = 0
    processed_count = 0
    filtered_count = 0
    cr = '\r'
    for total_lines, l in enumerate(sys.stdin, start=1):
        if not l.rstrip():
            continue
        if isinstance(l, bytes):
            l = l.decode('utf-8')

        t_now = time()
        if t_now - t_last > 2:
            t_last = t_now
            print(f'{cr}At: {total_lines:,}', end='', file=sys.stderr)

        try:
            data = json.loads(l)

            if (min_inc_created or max_ex_created) and '@graph' in data:
                try:
                    created = isodatetime(data['@graph'][0]['created'])
                    if min_inc_created and created < min_inc_created:
                        filtered_count += 1
                        continue
                    if max_ex_created and created >= max_ex_created:
                        filtered_count += 1
                        continue
                except (KeyError, ValueError):
                    pass

            # Record common instance/work shape anomalies before reshape()
            # normalizes or suppresses them. These counts tell us how often the
            # input deviates from the expected single-work model.
            graph = data.get('@graph') if isinstance(data, dict) else None
            if isinstance(graph, list) and len(graph) > 1 and isinstance(graph[1], dict):
                # graph[0] is the Record: its @id is the canonical, directly
                # look-up-able identifier for the record (graph[1]'s @id is
                # the same shortId with a '#it' fragment, so it adds nothing).
                record_id = graph[0].get('@id') if isinstance(graph[0], dict) else None
                raw_work = graph[1].get('instanceOf')
                if isinstance(raw_work, list):
                    note_anomaly('instanceOf_list', total_lines, {'length': len(raw_work), 'id': record_id})
                elif raw_work is not None and not isinstance(raw_work, dict):
                    note_anomaly('instanceOf_non_object', total_lines,
                                 {'type': type(raw_work).__name__, 'id': record_id})

            thing, work = reshape(data)
            compute_shape(thing, index)
            if work:
                compute_shape(thing, instance_index, type_key='Instance')
                compute_shape(work, work_by_type_index)
                compute_shape(work, work_index, type_key='Work')
            processed_count += 1

        except Exception as e:
            note_error(e, line_number=total_lines, data_line=l, stage='process')

    print(f'{cr}Total: {total_lines:,} lines ({processed_count:,} processed, {filtered_count:,} filtered)',
          file=sys.stderr)

    def print_summaries():
        if anomaly_counts:
            print('Anomaly summary:', file=sys.stderr)
            for kind in sorted(anomaly_counts):
                print(f'  {kind}: {anomaly_counts[kind]}', file=sys.stderr)
        if error_counts:
            print('Error summary:', file=sys.stderr)
            for kind in sorted(error_counts):
                print(f'  {kind}: {error_counts[kind]}', file=sys.stderr)

    # Empty output files are almost always caused by a broken upstream pipe,
    # path, decompression command, or filter. Both "no lines at all" and
    # "lines arrived but none could be processed" are symptoms of a broken
    # pipeline; fail before writing misleading empty shape reports unless
    # explicitly forced. A date filter that excluded every record is a
    # legitimate outcome and only gets a warning.
    fail_reason = None
    if not args.allow_empty_input:
        if total_lines == 0:
            fail_reason = 'No input lines received on stdin.'
        elif processed_count == 0 and filtered_count == 0:
            fail_reason = 'Input lines were received but no records could be processed.'

    if fail_reason:
        note_error(
            RuntimeError(f'{fail_reason} Refusing to overwrite output files. '
                         'Use --allow-empty-input to force writing empty outputs.'),
            line_number=0,
            stage='input'
        )
        print_summaries()
        print(f'Wrote error log: {error_log_path}', file=sys.stderr)
        error_log.close()
        raise SystemExit(2)

    if processed_count == 0 and filtered_count > 0:
        print(f'WARNING: no records were processed ({filtered_count:,} excluded by the '
              'created-date filter); writing empty shape outputs.', file=sys.stderr)

    def output(index, fpath):
        try:
            with fpath.open('w') as f:
                json.dump(index, f, indent=2, ensure_ascii=False)
            print(f'Wrote: {fpath}', file=sys.stderr)
            return True
        except Exception as e:
            note_error(e, stage=f'output:{fpath}')
            return False

    if outpath:
        all_outputs_written = output(index, outpath)
    else:
        to_outfile = lambda name: (outdir / name).with_suffix(SUFFIX)
        # A list, not a generator: all() must not short-circuit and skip
        # writing the remaining files after one failure.
        all_outputs_written = all([
            output(index, to_outfile('instance_shapes_by_type')),
            output(instance_index, to_outfile('instance_shapes')),
            output(work_by_type_index, to_outfile('work_shapes_by_type')),
            output(work_index, to_outfile('work_shapes')),
        ])

    print_summaries()
    print(f'Wrote error log: {error_log_path}', file=sys.stderr)
    error_log.close()

    # Per-record problems are tolerated, but failing to write a shape output
    # is a run failure; exit non-zero so batch pipelines notice missing or
    # stale output files.
    if not all_outputs_written:
        raise SystemExit(3)
