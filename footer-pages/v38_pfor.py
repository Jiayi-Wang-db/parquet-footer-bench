"""Frame-PFOR controls on frozen V38 artifacts; requires the existing local harness."""
import argparse
import csv
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import statistics
import subprocess
import sys

HERE = Path(__file__).resolve().parent
POLICIES = {'baseline': (), 'indexes_frame': ('3.1', '3.2'),
            'offsets_frame': ('4.3',), 'both_frame': ('3.1', '3.2', '4.3')}
PREFERRED_POLICY = 'offsets_frame'
SEEDS = (9201, 9202, 9203)


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def prepare(a):
    sys.path[:0] = [str(HERE), str(a.harness)]
    import matched_v21 as m
    from chunk_row_starts import unpack, pack
    from tagged_row_ranges import IntegerDecoder
    from measure_encoding_sweep import decode_pfor
    decoder = IntegerDecoder(m)  # Does not truncate the historical codec replay.
    idl = m.thriftpy2.load(str(HERE/'ExplicitIndexFooter.thrift'), module_name='pfor_v38_thrift')
    frozen = json.loads(gzip.decompress((HERE/'v38-results.json.gz').read_bytes()))
    cache, sizes, pages = {}, [], []

    def reencode(payload, physical):
        key = payload, physical
        if key in cache:
            return cache[key]
        values = decoder.values(payload, 0, physical)
        result, detail, pos, first = bytearray(), [], 0, 0
        while pos < len(payload):
            header, length = m.t.decode_prefix(payload[pos:]); pos += length
            body = payload[pos:pos+m.get(header, 3)]; pos += len(body)
            if m.get(header, 1) == 2:
                continue  # Dictionary values have already been decoded above.
            assert m.get(header, 1) == 3
            data = m.get(header, 8)
            assert m.get(data, 2) == m.get(data, 5) == m.get(data, 6) == 0
            assert not m.get(data, 7)
            count, old_encoding = m.get(data, 1), m.get(data, 4)
            vs = values[first:first+count]; first += count
            raw = m.plain(physical, vs)
            encoded = decoder.native.codec(1, physical, 129, raw, count)
            decoded, modes = decode_pfor(encoded, 4 if physical == 1 else 8, count)
            assert decoded == raw and modes.get('delta', 0) == 0
            m.field(header, 2, m.t.I32, len(encoded))
            m.field(header, 3, m.t.I32, len(encoded))
            m.field(data, 4, m.t.I32, 128)
            result.extend(m.t.encode(header)); result.extend(encoded)
            detail.append(dict(values=count, previous_encoding=old_encoding,
                               previous_value_bytes=len(body), frame_value_bytes=len(encoded), blocks=modes))
        assert pos == len(payload) and first == len(values)
        result = bytes(result)
        assert decoder.values(result, 0, physical) == values
        cache[key] = result, detail
        return result, detail

    for row in frozen['sizes']:
        name, scope, base = row['dataset'], row['scope'], row['base']
        filename = f'{name}.{scope}.raw.par3'
        src = a.baseline/filename
        if not src.exists(): src = a.baseline/'initial'/filename
        original = src.read_bytes()
        assert len(original) == row['bytes'] and digest(src) == row['sha256']
        root, streams = unpack(m, original, base)
        assert m.get(root, 1) == 38 and all(codec == 0 for _, codec in streams.values())
        changed = {}
        for key in POLICIES['both_frame']:
            if key in streams:
                data, info = reencode(streams[key][0], 2 if key == '4.3' else 1)
                changed[key] = data, 0
                pages.append(dict(dataset=name, scope=scope, stream=key, pages=info,
                                  before_bytes=len(streams[key][0]), after_bytes=len(data)))
        for policy, keys in POLICIES.items():
            folder = a.work/policy; folder.mkdir(exist_ok=True)
            path = folder/filename
            replacement = dict(streams, **{k: changed[k] for k in keys if k in changed})
            blob = pack(m, root, replacement, base)
            if policy == 'baseline': assert blob == original
            decoded_root, decoded_streams = unpack(m, blob, base)
            assert decoded_streams == replacement
            assert m.read(idl.ExplicitIndexFooter, m.t.encode(decoded_root)).version == 38
            path.write_bytes(blob)
            sizes.append(dict(dataset=name, scope=scope, variant=policy, bytes=len(blob),
                              sha256=digest(path), source=str(src), source_sha256=row['sha256']))
        print('Prepared', name, scope, flush=True)
    save(a.work/'sizes.json', sizes); save(a.work/'pages.json', pages)


def build(a):
    source = (HERE/'v38_decode.cc').read_text()
    assert source.count('int main(int argc,char** argv)') == 1
    (a.work/'v38_comparison_support.h').write_text(source.split('int main(int argc,char** argv)')[0])
    for tag in ('release', 'sanitize'):
        command = json.loads((a.baseline/f'build-{tag}.json').read_text())['command']
        command = [str(HERE/'v38_pfor_decode.cc') if x == str(HERE/'v38_decode.cc') else x for x in command]
        command.insert(1, '-I'+str(a.work))
        command[command.index('-o')+1] = str(a.work/f'decode-{tag}')
        save(a.work/f'build-{tag}.json', dict(command=command))
        print('Building', tag, flush=True)
        with (a.work/f'build-{tag}.log').open('w') as out:
            subprocess.run(command, stdout=out, stderr=subprocess.STDOUT, check=True)
        print('Built', tag, flush=True)


def run(a, tag, mode, seed, initial):
    cpu = 16 if 16 in os.sched_getaffinity(0) else min(os.sched_getaffinity(0))
    harness = a.baseline/'initial/harness' if initial else a.harness
    request = a.baseline/'initial/request.bin' if initial else HERE/'work/list-statistics/request.bin'
    suite = 'initial' if initial else 'additional-wide'
    path = a.work/f'{mode}-{tag}-{suite}-{seed}.jsonl'
    command = ['taskset', '-c', str(cpu), str(a.work/f'decode-{tag}'), str(harness), str(request),
               str(a.work), mode, str(seed), '33' if mode == 'validate' else '9']
    env = os.environ.copy(); env['UBSAN_OPTIONS'] = 'halt_on_error=1:print_stacktrace=1'
    env['ASAN_OPTIONS'] = 'halt_on_error=1:detect_leaks=1'
    print('Running', tag, mode, suite, seed, flush=True)
    with path.open('w') as out, path.with_suffix('.log').open('w') as err:
        subprocess.run(command, stdout=out, stderr=err, env=env, check=True)
    log = path.with_suffix('.log').read_text()
    assert not any(x in log for x in ('runtime error:', 'ERROR: AddressSanitizer', 'ERROR: LeakSanitizer'))
    save(path.with_suffix('.command.json'), dict(command=command, cpu=cpu,
         request_sha256=digest(request), binary_sha256=digest(a.work/f'decode-{tag}')))
    print('Completed', tag, mode, suite, seed, flush=True)


def report(a):
    sizes = json.loads((a.work/'sizes.json').read_text())
    pages = json.loads((a.work/'pages.json').read_text())
    frozen = json.loads(gzip.decompress((HERE/'v38-results.json.gz').read_bytes()))
    modular_sizes = {(r['dataset'], r['scope']): r['modular'] for r in frozen['baselines']}
    for row in sizes:
        path = a.work/row['variant']/f"{row['dataset']}.{row['scope']}.raw.par3"
        assert path.stat().st_size == row['bytes'] and digest(path) == row['sha256']
        assert digest(row['source']) == row['source_sha256']
    validation = {}
    for tag in ('release', 'sanitize'):
        validation[tag] = []
        for suite in ('additional-wide', 'initial'):
            path = a.work/f'validate-{tag}-{suite}-0.jsonl'
            rows = [json.loads(line) for line in path.read_text().splitlines()]
            assert len(rows) == 1 and rows[0]['cross_encoding_hashes']
            assert rows[0]['validated_queries'] == (3344 if suite == 'additional-wide' else 1280)
            log = path.with_suffix('.log').read_text()
            assert not any(x in log for x in ('runtime error:', 'ERROR: AddressSanitizer', 'ERROR: LeakSanitizer'))
            validation[tag].extend(rows)
    runs = []
    for seed in SEEDS:
        rows = []
        for suite in ('additional-wide', 'initial'):
            path = a.work/f'time-release-{suite}-{seed}.jsonl'
            rows += [json.loads(line) for line in path.read_text().splitlines()]
        assert len(rows) == 1920
        assert all(len(r['samples_ns']) == 7 and statistics.median(r['samples_ns']) == r['median_ns'] for r in rows)
        runs.append({(r['dataset'], r['scope'], r['variant'], r['column']): r for r in rows})
    assert all(len(r) == 1920 and set(r) == set(runs[0]) for r in runs)
    positions = []
    for key in sorted(runs[0]):
        ns = [r[key]['median_ns'] for r in runs]
        positions.append(dict(zip(('dataset', 'scope', 'variant', 'column'), key),
                              median_ns=statistics.median(ns), process_ns=ns))
    summary = []
    names = sorted({r['dataset'] for r in sizes})
    for name in names:
        for scope in range(4):
            for policy in POLICIES:
                for workload in ('one', 'all'):
                    selected = [r for r in positions if (r['dataset'], r['scope'], r['variant']) == (name, scope, policy)
                                and (r['column'] >= 0) == (workload == 'one')]
                    assert len(selected) == (9 if workload == 'one' else 1)
                    summary.append(dict(dataset=name, scope=scope, variant=policy, workload=workload,
                        median_ns=statistics.median(r['median_ns'] for r in selected),
                        process_ns=[statistics.median(r['process_ns'][i] for r in selected) for i in range(3)]))
    timings = {(r['dataset'], r['scope'], r['variant'], r['workload']): r for r in summary}
    byte_sizes = {(r['dataset'], r['scope'], r['variant']): r['bytes'] for r in sizes}
    rows = []
    for r in summary:
        name, scope, variant, workload = (r[k] for k in ('dataset', 'scope', 'variant', 'workload'))
        baseline = timings[name, scope, 'baseline', workload]
        rows.append(dict(dataset=name, scope=scope, variant=variant, workload=workload,
            bytes=byte_sizes[name, scope, variant], baseline_bytes=byte_sizes[name, scope, 'baseline'],
            modular_bytes=modular_sizes[name, scope],
            size_vs_modular_pct=100*(byte_sizes[name, scope, variant]/modular_sizes[name, scope]-1),
            median_us=r['median_ns']/1000, baseline_us=baseline['median_ns']/1000,
            time_change_pct=100*(r['median_ns']/baseline['median_ns']-1),
            size_change_pct=100*(byte_sizes[name, scope, variant]/byte_sizes[name, scope, 'baseline']-1)))
    with (HERE/'v38-pfor-comparison.csv').open('w') as out:
        writer = csv.DictWriter(out, fieldnames=rows[0].keys(), lineterminator='\n')
        writer.writeheader(); writer.writerows(rows)
    hashes = {str(p): digest(p) for p in [HERE/'v38_pfor.py', HERE/'v38_pfor_decode.cc',
        HERE/'v38_decode.cc', HERE/'v38_reader.h', HERE/'v38-results.json.gz',
        *a.baseline.glob('*.h'), a.baseline/'dbp.cc', a.work/'v38_comparison_support.h',
        a.work/'decode-release', a.work/'decode-sanitize']}
    for initial in (False, True):
        harness = a.baseline/'initial/harness' if initial else a.harness
        for suite in ('additional-raincloud', 'wide-raincloud'):
            p = harness/f'work/{suite}-20261022/request.bin'; hashes[str(p)] = digest(p)
    provenance = dict(hashes=hashes, seeds=SEEDS, trials=7,
        cpu_info=Path('/proc/cpuinfo').read_text(),
        builds={tag: json.loads((a.work/f'build-{tag}.json').read_text()) for tag in ('release', 'sanitize')},
        commands=[json.loads(p.read_text()) for p in sorted(a.work.glob('*.command.json'))])
    result = dict(sizes=sizes, pages=pages, summary=summary, per_position=positions,
                  runs=[list(r.values()) for r in runs], validation=validation, provenance=provenance)
    preferred = dict(policy=PREFERRED_POLICY, artifact_directory=str(a.work/PREFERRED_POLICY),
                     artifacts=[r for r in sizes if r['variant'] == PREFERRED_POLICY])
    save(a.work/'preferred.json', preferred)
    result['preferred_configuration'] = preferred
    result['modular_sizes'] = [dict(dataset=n, scope=s, bytes=b) for (n,s),b in sorted(modular_sizes.items())]
    diagnostic_work = HERE/'work/v38-pfor-frame-20261027'
    diagnostics = [*diagnostic_work.glob('time-release-additional-wide-*.jsonl'),
                   diagnostic_work/'identical-control/time.jsonl']
    result['clickbench_sampling_diagnostics'] = {
        str(p): [r for r in map(json.loads, p.read_text().splitlines())
                 if r['dataset'] == 'clickbench-hits' and r['scope'] == 3 and r['column'] == -1]
        for p in diagnostics if p.exists()}
    (HERE/'v38-pfor-results.json.gz').write_bytes(gzip.compress(json.dumps(result).encode(), mtime=0))
    labels = {'baseline': 'V38 baseline', 'indexes_frame': 'Indexes only',
              'offsets_frame': 'Page offsets only', 'both_frame': 'Both'}
    from tagged_row_ranges import NAMES
    names_display = dict(NAMES, **{'us-accidents-00004-of-00007': 'US Accidents',
        'fineweb-10bt-000': 'FineWeb', 'hacker-news-00000-of-00039': 'Hacker News',
        'yellow-tripdata-2025-01': 'NYC Taxi'})
    queries = ['Chunk metadata', 'Chunk statistics', 'Chunk metadata + page locations', 'Chunk + page statistics']
    scope_names = ['Core', 'Core + chunk statistics', 'Core + chunk statistics + page locations', 'Full metadata']
    offset_times = [100*(math.exp(statistics.mean(math.log(
        timings[n,2,'offsets_frame',w]['median_ns']/timings[n,2,'baseline',w]['median_ns']) for n in names))-1)
        for w in ('one', 'all')]
    offset_growth = 100*(sum(byte_sizes[n,2,'offsets_frame'] for n in names)/
                        sum(byte_sizes[n,2,'baseline'] for n in names)-1)
    lines = ['# V38 ordinary-frame PFOR experiment', '',
        '**Preferred benchmark configuration: `offsets_frame`.** Retain ordinary-frame PFOR for relative page offsets and adaptive encoding for the hierarchy indexes. We accept the modest size increase over size-minimized V38 because it buys selective-read speed while remaining smaller than modular on every tested dataset at the page-location and full-metadata scopes. This is a benchmark encoding policy; the proposed format does not mandate PFOR.', '',
        f'Forcing frame PFOR on relative page offsets changes geometric-mean decoding time for chunk metadata plus page locations by **{offset_times[0]:+.1f}% for one column** and **{offset_times[1]:+.1f}% for all columns**. Retained metadata through page locations grows **{offset_growth:+.1f}%**. Negative timing changes mean less time. The tables separate index changes from page-offset changes.', '',
        'Encoding-only controls on all 12 saved V38 datasets. No Zstd. The baseline is remeasured in the same executable and timing processes as the controls; historical proposal timings are not used as the denominator.', '',
        '`indexes_frame` forces ordinary-frame PFOR on `column_chunk_index` and `page_index` when present. `offsets_frame` forces it on relative page offsets (V38 field 4.3). `both_frame` combines them. Every DATA_PAGE_V2 retains its original logical boundaries and value count. PFOR uses the existing 1,024-value blocks; no delta blocks are allowed. A dictionary page is removed if its target stream is converted. All other streams remain byte-identical. Root offsets/lengths are updated.', '',
        'The V38 reader, bulk/selected access policy, datasets and queries are unchanged. Timings include opening/parsing metadata, decoding, materializing outputs and hashing, with resident bytes and fresh readers per operation. Nine distributed single-column positions and an all-column read are measured. Each dataset is measured in three processes, each running seven randomized trials with all four policies interleaved on the same pinned CPU. The two dataset suites run separately, for six timing processes total. Each timed batch follows one untimed warmup and contains at least four operations. Single-column results are medians across positions after taking medians across trials/processes. Small changes may be timing noise.', '',
        'An initial run without per-batch warmup produced an apparent 27% ClickBench full-statistics difference, including in the page-offset-only negative control. A diagnostic using identical footer bytes reproduced the roughly 90/120 ms timing modes. The tables below use the subsequent warmup/four-operation-minimum campaign; the relevant initial and identical-artifact samples are preserved in the result archive.', '',
        'The size tables are cumulative stored metadata. Timing queries read only the named projection: statistics queries do not read relative page offsets. Page-offset-only controls on statistics queries and all scope-0/1 queries are therefore negative controls.', '',
        '## Preferred configuration versus modular size', '',
        'Sizes below use the same saved datasets and cumulative metadata scopes as the modular measurements. Timing improvements above compare with the matched V38 baseline, not with modular timings from a different campaign. Core-only sizes are unchanged by page-offset encoding; some small core footers remain larger than modular.', '',
        '| Dataset | Page-location scope: preferred / modular bytes | Full metadata: preferred / modular bytes |',
        '|---|---:|---:|']
    for name in names:
        cells = [f'{byte_sizes[name,s,PREFERRED_POLICY]:,} / {modular_sizes[name,s]:,}' for s in (2,3)]
        lines.append(f'| {names_display.get(name,name)} | ' + ' | '.join(cells) + ' |')
    lines.append('| **Total** | ' + ' | '.join(
        f'{sum(byte_sizes[n,s,PREFERRED_POLICY] for n in names):,} / {sum(modular_sizes[n,s] for n in names):,}'
        for s in (2,3)) + ' |')
    lines += ['', 'The selected artifact directory and all 48 hashes are recorded in `preferred.json` in the work directory and `preferred_configuration` in the result archive.', '',
        '## Decoding overview', '',
        'Geometric mean time change over 12 datasets. Negative means less time. These aggregates are descriptive, not significance tests.', '',
        '| Query | Read | Indexes only | Page offsets only | Both |', '|---|---|---:|---:|---:|']
    for scope in range(4):
        for workload in ('one', 'all'):
            cells = []
            for policy in list(POLICIES)[1:]:
                ratio = math.exp(statistics.mean(math.log(timings[n,scope,policy,workload]['median_ns']/
                    timings[n,scope,'baseline',workload]['median_ns']) for n in names))
                cells.append(f'{100*(ratio-1):+.1f}%')
            lines.append(f"| {queries[scope]} | {'One column' if workload=='one' else 'All columns'} | " + ' | '.join(cells) + ' |')
    for scope in range(4):
        lines += ['', f'## {scope_names[scope]} size', '',
            '| Dataset | V38 bytes | Indexes only bytes | Page offsets only bytes | Both bytes |', '|---|---:|---:|---:|---:|']
        for name in names:
            lines.append(f'| {names_display.get(name,name)} | ' + ' | '.join(f'{byte_sizes[name,scope,p]:,}' for p in POLICIES) + ' |')
        lines.append('| **Total** | ' + ' | '.join(f'{sum(byte_sizes[n,scope,p] for n in names):,}' for p in POLICIES) + ' |')
        lines += ['', f'## {queries[scope]} decoding', '',
            'Each cell is **one column / all columns**. Baseline values are microseconds; control values are percentage changes versus the matched baseline.', '',
            '| Dataset | V38 µs | Indexes only Δ | Page offsets only Δ | Both Δ |', '|---|---:|---:|---:|---:|']
        for name in names:
            cells = [' / '.join(f"{timings[name,scope,'baseline',w]['median_ns']/1000:.2f}" for w in ('one','all'))]
            for policy in list(POLICIES)[1:]:
                cells.append(' / '.join(f"{100*(timings[name,scope,policy,w]['median_ns']/timings[name,scope,'baseline',w]['median_ns']-1):+.1f}%" for w in ('one','all')))
            lines.append(f'| {names_display.get(name,name)} | ' + ' | '.join(cells) + ' |')
    lines += ['', '## Validation and reproduction', '',
        'All 48 source artifacts match their recorded V38 SHA-256 hashes. Preparation round-trips every converted value, checks frame-mode flags, preserves logical page boundaries and verifies all unchanged streams byte-for-byte. All 192 resulting directories parse through the measured V38 Thrift definition. Release and ASan/UBSan each validate 4,624 queries against source oracles, with matching outputs across the four policies. Sanitizers halt on error. Each timing process also validates its selected projections before measuring.', '',
        'This uses the measured V38 wire layout, before the subsequent review-draft struct reorganization. Runtime PFOR is an experimental encoding. The runner requires the existing local harness, frozen artifacts, generated reader support and external Runtime codecs described in [V38-MEASUREMENTS.md](V38-MEASUREMENTS.md); it is not a self-contained public benchmark.', '',
        '```sh', '/home/jiayi.wang/raincloud/.venv/bin/python footer-pages/v38_pfor.py --stage all',
        '/home/jiayi.wang/raincloud/.venv/bin/python footer-pages/v38_pfor.py --stage report', '```', '',
        '[Absolute sizes/timings and deltas](v38-pfor-comparison.csv) · [Samples, stream decisions and provenance](v38-pfor-results.json.gz) · [Runner](v38_pfor.py) · [Native driver](v38_pfor_decode.cc)', '']
    (HERE/'V38-PFOR.md').write_text('\n'.join(lines))
    print('Reported', len(rows), 'size/timing combinations.', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--harness', type=Path, default=Path('/home/jiayi.wang/parquet-footer-bench/footer-pages'))
    parser.add_argument('--baseline', type=Path, default=HERE/'work/v38-optimized-20261027')
    parser.add_argument('--work', type=Path, default=HERE/'work/v38-pfor-frame-warm-20261027')
    parser.add_argument('--stage', choices=('prepare', 'build', 'validate', 'time', 'report', 'all'), default='all')
    a = parser.parse_args()
    for k in ('harness', 'baseline', 'work'): setattr(a, k, getattr(a, k).resolve())
    a.work.mkdir(parents=True, exist_ok=True)
    if a.stage in ('prepare', 'all'): prepare(a)
    if a.stage in ('build', 'all'): build(a)
    if a.stage in ('validate', 'all'):
        for tag in ('release', 'sanitize'):
            for initial in (False, True): run(a, tag, 'validate', 0, initial)
    if a.stage in ('time', 'all'):
        for seed in SEEDS:
            for initial in (False, True): run(a, 'release', 'time', seed, initial)
    if a.stage in ('report', 'all'): report(a)


if __name__ == '__main__':
    main()
