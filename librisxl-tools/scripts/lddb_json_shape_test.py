"""Regression tests for lddb_json_shape.py.

Run from this directory (no dependencies beyond the standard library):

    python3 lddb_json_shape_test.py -v
"""
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import lddb_json_shape as shapes

SCRIPT = Path(__file__).parent / 'lddb_json_shape.py'


def record(thing, extra=None, created='2020-01-01T00:00:00Z'):
    """A framed lddb-style record line: @graph = [meta, thing, *extra]."""
    meta = {'@id': f"meta/{thing.get('@id', 'x')}", 'created': created}
    return json.dumps({'@graph': [meta, thing] + (extra or [])})


def run_script(outdir, stdin_text, *args, env_extra=None):
    env = dict(os.environ, **(env_extra or {}))
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args, str(outdir)],
        input=stdin_text, capture_output=True, text=True, env=env)


class UnwrapSingleTest(unittest.TestCase):

    def test_singleton_list_of_dict_unwraps(self):
        self.assertEqual(shapes.unwrap_single([{'@id': 'w1'}]), {'@id': 'w1'})

    def test_multi_value_list_is_ambiguous(self):
        self.assertIsNone(shapes.unwrap_single([{'@id': 'w1'}, {'@id': 'w2'}]))

    def test_singleton_list_of_scalar_is_rejected(self):
        self.assertIsNone(shapes.unwrap_single(['bogus']))

    def test_non_list_values_pass_through(self):
        self.assertEqual(shapes.unwrap_single({'@id': 'w1'}), {'@id': 'w1'})
        self.assertEqual(shapes.unwrap_single('bogus'), 'bogus')
        self.assertIsNone(shapes.unwrap_single(None))


class ReshapeTest(unittest.TestCase):

    def framed(self, instance_of, extra=None):
        line = record({'@id': 'i1', '@type': 'Instance', 'instanceOf': instance_of},
                      extra=extra)
        return json.loads(line)

    def test_singleton_list_ref_resolves_to_graph_node(self):
        work_node = {'@id': 'w1', '@type': 'Monograph', 'hasTitle': [{'mainTitle': 'Foo'}]}
        thing, work = shapes.reshape(self.framed([{'@id': 'w1'}], extra=[work_node]))
        self.assertEqual(work, work_node)
        self.assertEqual(thing['instanceOf'], work_node)

    def test_dangling_ref_stays_as_id_dict(self):
        thing, work = shapes.reshape(self.framed({'@id': 'nowhere'}))
        self.assertEqual(work, {'@id': 'nowhere'})

    def test_multi_value_list_yields_no_work(self):
        thing, work = shapes.reshape(self.framed([{'@id': 'w1'}, {'@id': 'w2'}]))
        self.assertIsNone(work)
        # The ambiguous raw value is left on the thing for instance shapes.
        self.assertEqual(thing['instanceOf'], [{'@id': 'w1'}, {'@id': 'w2'}])

    def test_scalar_instance_of_yields_no_work(self):
        thing, work = shapes.reshape(self.framed('bogus'))
        self.assertIsNone(work)

    def test_non_framed_input(self):
        thing, work = shapes.reshape(
            {'@id': 'i1', 'instanceOf': [{'@type': 'Monograph'}]})
        self.assertEqual(work, {'@type': 'Monograph'})
        thing, work = shapes.reshape({'@id': 'i1', 'instanceOf': 'bogus'})
        self.assertIsNone(work)


class ComputeShapeTest(unittest.TestCase):

    def setUp(self):
        self._max_stats = shapes.MAX_STATS
        shapes.MAX_STATS = 2

    def tearDown(self):
        shapes.MAX_STATS = self._max_stats

    def test_basic_shape(self):
        index = {}
        shapes.compute_shape(
            {'@type': 'Monograph', 'hasTitle': [{'mainTitle': 'Foo'}]}, index)
        self.assertEqual(index['Monograph']['hasTitle'][None]['mainTitle'],
                         {'@value Foo': 1})

    def test_pure_literal_collapse_to_int(self):
        index = {}
        for i in range(5):
            shapes.compute_shape({'@type': 'T', 'p': f'v{i}'}, index)
        self.assertEqual(index['T']['p'], 5)

    def test_collapse_preserves_object_branches(self):
        # Literals collapse, an object arrives, more literals arrive: neither
        # the rolled-up count nor the object branch may be destroyed.
        index = {}
        for i in range(5):
            shapes.compute_shape({'@type': 'T', 'p': f'v{i}'}, index)
        shapes.compute_shape({'@type': 'T', 'p': {'@type': 'Sub', 'label': 'x'}}, index)
        for i in range(5, 8):
            shapes.compute_shape({'@type': 'T', 'p': f'v{i}'}, index)
        stats = index['T']['p']
        self.assertIn('Sub', stats)
        self.assertEqual(stats['Sub']['label'], {'@value x': 1})
        self.assertEqual(stats['@collapsed'], 8)

    def test_bare_id_reference_counts_at_index_level(self):
        index = {}
        shapes.compute_shape({'@id': 'w1'}, index)
        self.assertEqual(index['@id'], {'w1': 1})

    def test_non_dict_nodes_are_tolerated(self):
        index = {}
        shapes.compute_shape('scalar', index)
        shapes.compute_shape(['scalar', {'@type': 'T', 'p': 'v'}], index)
        self.assertEqual(index, {'T': {'@type': {'T': 1}, 'p': {'@value v': 1}}})

    def test_composite_values_are_serialized(self):
        # A list nested inside a property list is not a node; it is counted
        # as a stable serialized string.
        index = {}
        shapes.compute_shape({'@type': 'T', 'p': [['nested']]}, index)
        self.assertEqual(index['T']['p'], {'["nested"]': 1})


class CliTest(unittest.TestCase):

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.outdir = self.tmp / 'out'

    def tearDown(self):
        self._tmp.cleanup()

    def shape_files(self):
        return sorted(p.name for p in self.outdir.glob('*.json'))

    def test_happy_path(self):
        work = {'@id': 'w1', '@type': 'Monograph', 'hasTitle': [{'mainTitle': 'Foo'}]}
        lines = record({'@id': 'i1', '@type': 'Instance', 'instanceOf': [{'@id': 'w1'}]},
                       extra=[work]) + '\n'
        res = run_script(self.outdir, lines)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertEqual(self.shape_files(),
                         ['instance_shapes.json', 'instance_shapes_by_type.json',
                          'work_shapes.json', 'work_shapes_by_type.json'])
        works = json.loads((self.outdir / 'work_shapes_by_type.json').read_text())
        self.assertEqual(works['Monograph']['hasTitle']['null']['mainTitle'],
                         {'@value Foo': 1})
        self.assertIn('instanceOf_list: 1', res.stderr)

    def test_single_output_file_mode(self):
        target = self.outdir / 'shapes.json'
        res = run_script(target, record({'@id': 'i1', '@type': 'Instance'}) + '\n')
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn('Instance', json.loads(target.read_text()))

    def test_bad_lines_are_logged_not_fatal(self):
        lines = 'not json\n' + record({'@id': 'i1', '@type': 'Instance'}) + '\n'
        res = run_script(self.outdir, lines)
        self.assertEqual(res.returncode, 0, res.stderr)
        errors = [json.loads(l) for l in
                  (self.outdir / 'shape_errors.jsonl').read_text().splitlines()]
        self.assertEqual([e['errorType'] for e in errors], ['JSONDecodeError'])

    def test_empty_stdin_fails_without_writing(self):
        res = run_script(self.outdir, '')
        self.assertEqual(res.returncode, 2)
        self.assertEqual(self.shape_files(), [])

    def test_blank_lines_only_fails(self):
        self.assertEqual(run_script(self.outdir, '\n\n\n').returncode, 2)

    def test_all_garbage_input_fails_without_writing(self):
        res = run_script(self.outdir, 'junk\njunk\n')
        self.assertEqual(res.returncode, 2)
        self.assertEqual(self.shape_files(), [])
        self.assertTrue((self.outdir / 'shape_errors.jsonl').exists())

    def test_allow_empty_input_forces_writing(self):
        res = run_script(self.outdir, '', '--allow-empty-input')
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertEqual(len(self.shape_files()), 4)

    def test_date_filter_excluding_all_warns_but_succeeds(self):
        lines = record({'@id': 'i1', '@type': 'Instance'},
                       created='2019-01-01T00:00:00Z') + '\n'
        res = run_script(self.outdir, lines, '-c', '2020-01-01T00:00:00Z')
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn('WARNING', res.stderr)
        self.assertEqual(len(self.shape_files()), 4)

    def test_anomaly_summary_respects_date_filter(self):
        odd = {'@type': 'Instance', 'instanceOf': [{'@id': 'w1'}, {'@id': 'w2'}]}
        lines = (record(dict(odd, **{'@id': 'i1'}), created='2019-01-01T00:00:00Z') + '\n'
                 + record(dict(odd, **{'@id': 'i2'}), created='2021-01-01T00:00:00Z') + '\n')
        res = run_script(self.outdir, lines, '-c', '2020-01-01T00:00:00Z')
        self.assertIn('instanceOf_list: 1', res.stderr)

    def test_anomaly_log_includes_record_id(self):
        # The Record's own @id (graph[0]), not the thing's (graph[1]), since
        # it's the canonical, directly look-up-able identifier.
        thing = {'@id': 'i1', '@type': 'Instance', 'instanceOf': [{'@id': 'w1'}, {'@id': 'w2'}]}
        res = run_script(self.outdir, record(thing) + '\n')
        self.assertEqual(res.returncode, 0, res.stderr)
        anomalies = [json.loads(l) for l in
                    (self.outdir / 'shape_errors.jsonl').read_text().splitlines()]
        self.assertEqual(anomalies[0]['detail']['id'], 'meta/i1')

    def test_explicit_error_log_refuses_non_empty_existing_file(self):
        log = self.tmp / 'evidence.jsonl'
        log.write_text('{"old": "evidence"}\n')
        res = run_script(self.outdir, record({'@id': 'i1', '@type': 'Instance'}) + '\n',
                         '--error-log', str(log))
        self.assertEqual(res.returncode, 2)
        self.assertEqual(log.read_text(), '{"old": "evidence"}\n')

    def test_default_error_log_is_rewritten_between_runs(self):
        line = record({'@id': 'i1', '@type': 'Instance'}) + '\n'
        self.assertEqual(run_script(self.outdir, line).returncode, 0)
        self.assertEqual(run_script(self.outdir, line).returncode, 0)

    def test_non_bib_collections_yield_empty_work_outputs(self):
        # auth and hold things have no instanceOf: they must count as
        # processed, appear in the by-type index, trigger no anomalies, and
        # leave the instance/work split files legitimately empty.
        lines = (record({'@id': 'p1', '@type': 'Person', 'familyName': 'X'}) + '\n'
                 + record({'@id': 'h1', '@type': 'Item', 'heldBy': {'@id': 'lib/S'}}) + '\n')
        res = run_script(self.outdir, lines)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertNotIn('Anomaly', res.stderr)
        by_type = json.loads((self.outdir / 'instance_shapes_by_type.json').read_text())
        self.assertEqual(sorted(by_type), ['Item', 'Person'])
        for name in ('instance_shapes', 'work_shapes', 'work_shapes_by_type'):
            self.assertEqual(json.loads((self.outdir / f'{name}.json').read_text()), {})

    def test_collapsed_key_in_output(self):
        lines = ''.join(
            record({'@id': f'i{i}', '@type': 'Instance', 'genre': f'g{i}'}) + '\n'
            for i in range(5))
        lines += record({'@id': 'ix', '@type': 'Instance',
                         'genre': {'@type': 'Genre', 'label': 'obj'}}) + '\n'
        res = run_script(self.outdir, lines, env_extra={'MAX_STATS': '2'})
        self.assertEqual(res.returncode, 0, res.stderr)
        index = json.loads((self.outdir / 'instance_shapes_by_type.json').read_text())
        genre = index['Instance']['genre']
        self.assertIn('Genre', genre)
        self.assertEqual(genre['@collapsed'], 5)

    @unittest.skipIf(os.geteuid() == 0, 'root ignores directory permissions')
    def test_unwritable_outdir_exits_nonzero(self):
        self.outdir.mkdir()
        self.outdir.chmod(0o555)
        try:
            res = run_script(self.outdir, record({'@id': 'i1', '@type': 'Instance'}) + '\n',
                             '--error-log', str(self.tmp / 'err.jsonl'))
            self.assertEqual(res.returncode, 3)
        finally:
            self.outdir.chmod(0o755)


if __name__ == '__main__':
    unittest.main()
