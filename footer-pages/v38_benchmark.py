"""Prepare and benchmark V38 using the existing frozen footer/codec harness.

Requires the saved V35 artifacts, refined-hierarchy support, and external codec
harness documented in V38-MEASUREMENTS.md. Historical artifacts are read-only.
"""
import argparse
import csv
import gzip
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import statistics
import subprocess
import sys

HERE = Path(__file__).resolve().parent
SEEDS = (9101, 9102, 9103)


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def load_direct(a):
    sys.path[:0] = [str(HERE), str(a.harness)]
    spec = importlib.util.spec_from_file_location('direct_v35_validation', a.direct / 'validate.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def prepare(a):
    direct = load_direct(a); codec, m = direct.a, direct.m
    codec.setup(a.work)
    idl = m.thriftpy2.load(str(HERE / 'ExplicitIndexFooter.thrift'), module_name='v38_benchmark_thrift')
    expected = {(r['dataset'], r['scope']): r for r in json.loads((a.direct / 'results.json').read_text())['artifacts']}
    rows, inputs = [], {str(HERE / 'ExplicitIndexFooter.thrift'): digest(HERE / 'ExplicitIndexFooter.thrift')}
    for suite in ('additional-raincloud', 'wide-raincloud'):
        oracle = a.harness / f'work/{suite}-20261022/request.bin'
        inputs[str(oracle)] = digest(oracle)
        for source in codec.sources(oracle):
            name, base = source['name'], source['base']
            for scope in range(4):
                path = a.direct / f'{name}.{scope}.raw.par3'
                inputs[str(path)] = digest(path)
                assert inputs[str(path)] == expected[name, scope]['sha256']
                root, streams = codec.unpack(m, path.read_bytes(), base)
                assert m.get(root, 1) == 35
                prior = dict(streams)
                for key in ('3.1', '3.2'):
                    if key in streams:
                        values = codec.decode(streams[key][0], 1)
                        assert values[0] == 0
                        streams[key] = codec.encoded(values[1:], 1)
                m.field(root, 1, m.t.I32, 38)
                blob = codec.pack(m, root, streams, base)
                checked_root, checked = codec.unpack(m, blob, base)
                parsed = m.read(idl.ExplicitIndexFooter, m.t.encode(checked_root))
                assert parsed.version == 38
                decoded = direct.values(checked)
                assert len(decoded['3.1']) == source['C']
                assert (decoded['3.1'][-1] if decoded['3.1'] else 0) == len(source['chunks'])
                for key in ('3.1', '3.2'):
                    if key in decoded: decoded[key] = [0] + decoded[key]
                direct.validate(decoded)
                for key in prior:
                    if key not in ('3.1', '3.2'): assert checked[key] == prior[key]
                assert codec.pack(m, checked_root, checked, base) == blob
                out = a.work / f'{name}.{scope}.raw.par3'; out.write_bytes(blob)
                rows.append(dict(dataset=name, scope=scope, bytes=len(blob), v35_bytes=len(path.read_bytes()), sha256=digest(out), base=base))
            codec.encoder.cache.clear()
            print('Prepared V38', name, flush=True)
    fixtures(a, direct)
    codec.encoder.native.replay.close()
    (a.work / 'sizes.json').write_text(json.dumps(rows, indent=2)+'\n')
    (a.work / 'inputs.json').write_text(json.dumps(inputs, indent=2)+'\n')


def fixtures(a, direct):
    m, codec = direct.m, direct.a
    # G deliberately differs from all chunk counts. Dictionaries are reused within columns.
    values = {'3.1': [0, 3, 4, 4, 6], '3.2': [2, 2, 3, 4, 6, 7],
              '4.1': [5000, 50, 2900], '4.2': [30, 10, 100],
              '4.3': [0, 20, 0, 0, 0, 40, 0], '7.1': [0, 10, 20, 0, 0, 10, 20],
              '6.1': [0]*6, '6.2': [0, None, 0, 1, 2, 2], '6.5': [20, 0, 10, 30, 20, 10],
              '6.7': [900, 200, 1700, 400, 3000, 100], '6.8': [50, 0, 70, 30, 100, 20]}
    folder = a.work / 'fixtures'; folder.mkdir(exist_ok=True)
    def write(name, v, columns=5, version=38):
        root, streams = direct.encode(v)
        schema = [[[4, m.t.BINARY, b'root'], [5, m.t.I32, columns]]]
        schema += [[[1, m.t.I32, 2], [3, m.t.I32, 1], [4, m.t.BINARY, f'c{c}'.encode()]] for c in range(columns)]
        info = [[1, m.t.I32, columns], [2, m.t.I64, 30], [3, m.t.LIST, (m.t.STRUCT, schema)]]
        m.field(root, 1, m.t.I32, version); m.field(root, 2, m.t.STRUCT, info)
        m.field(m.get(root, 6), 4, m.t.STRUCT, [])
        streams['6.4'] = codec.encoded([100+i for i in range(len(v['6.2']))])
        (folder / (name + '.par3')).write_bytes(codec.pack(m, root, streams, 8192))
    write('ragged', values)
    absent = dict(values, **{'4.1': [], '4.2': [], '6.2': [None]*6}); write('no_dictionary', absent)
    empty = {k: [] for k in values}; empty['3.1'] = [0]*5; write('empty', empty)
    write('zero_columns', dict(empty, **{'3.1': []}), 0)
    for name, changes in {
        'leading_zero': {'3.1': [0]+values['3.1']},
        'bad_order': {'3.1': [0, 4, 3, 4, 6]},
        'bad_terminal': {'3.1': [0, 3, 4, 4, 7]},
        'bad_page_count': {'3.2': [0]+values['3.2']},
        'bad_page_order': {'3.2': [3, 2, 3, 4, 6, 7]},
        'bad_page_offset': {'4.3': [1]+values['4.3'][1:]},
        'bad_dictionary': {'6.2': [0, None, 0, 3, 2, 2]},
        'negative_index': {'3.1': [-1, 3, 4, 4, 6]},
        'bad_length': {'6.8': [2**63-1]+values['6.8'][1:]},
    }.items(): write(name, dict(values, **changes))
    write('old_version', values, version=35)
    # Retain the independent statistics oracle from the prior ragged fixtures;
    # migrate only locations and hierarchy, leaving every statistics leaf intact.
    for name in ('stats_rows', 'stats_pages', 'stats_pages_rows', 'stats_missing',
                 'stats_arbitrary', 'empty_stats', 'missing_starts', 'bad_start_count',
                 'bad_start_null', 'bad_stats_index'):
        root, streams = codec.unpack(m, (a.support/'explicit'/f'{name}.par3').read_bytes(), 0)
        regions = codec.decode(streams['4.1'][0], 2)
        chunks = codec.decode(streams['4.2'][0], 1)
        refs = codec.decode(streams['6.2'][0], 1)
        ids = list(dict.fromkeys(d for d in refs if d is not None))
        replacements = {
            '4.1': ([regions[d] for d in ids], 2, False),
            '4.2': ([regions[d+1]-regions[d] for d in ids], 2, False),
            '6.2': ([None if d is None else ids.index(d) for d in refs], 1, True),
            '6.7': ([regions[k] for k in chunks], 2, False),
            '6.8': ([regions[k+1]-regions[k] for k in chunks], 2, False),
        }
        for key in ('3.1', '3.2'):
            if key in streams: replacements[key] = (codec.decode(streams[key][0], 1)[1:], 1, False)
        for key, (vals, physical, nullable) in replacements.items():
            group, fid = map(int, key.split('.'))
            m.field(m.get(root, group), fid, m.t.STRUCT, [])
            streams[key] = codec.encoded(vals, physical, nullable)
        m.field(root, 1, m.t.I32, 38)
        (folder / f'{name}.par3').write_bytes(codec.pack(m, root, streams, 8192))


def build(a):
    for name in ('list_page_readers.h', 'matched_comparison.h', 'statistics_comparison.h', 'dbp.cc'):
        shutil.copyfile(a.support/name, a.work/name)
    shutil.copyfile(HERE/'fast_statistics_reader.h', a.work/'fast_statistics_reader.h')
    core = (a.support/'core_base.h').read_text()
    old='(version==23 || (version>=26 && version<=32))'
    assert core.count(old)==1
    core=core.replace(old, 'version==38')
    (a.work/'core_base.h').write_text(core)
    stats=(a.support/'refined_statistics.h').read_text()
    stats=stats.replace('refined::Hierarchy', 'v38::Hierarchy')
    stats=stats.replace('(version==31 || version==32)', 'version==38')
    stats=stats.replace('hierarchy.emplace(pages,version==32,d.C)', 'hierarchy.emplace(pages,d.C)')
    row_methods=(HERE/'refined_statistics_rows.h').read_text()
    row_methods=row_methods.replace('    if(version==32)hierarchy->Values(id,c,sink);\n    else ', '    ')
    stats=stats.replace('#include "refined_statistics_rows.h"', row_methods)
    (a.work/'v38_statistics.h').write_text(stats)
    for tag in ('release', 'sanitize'):
        command=json.loads((a.support/f'build-{tag}.json').read_text())['command']
        command=[s.replace(str(a.support),str(a.work)) for s in command]
        command=[str(HERE/'v38_decode.cc') if s==str(a.work/'decode.cc') else s for s in command]
        (a.work/f'build-{tag}.json').write_text(json.dumps(dict(command=command),indent=2)+'\n')
        with (a.work/f'build-{tag}.log').open('w') as out:
            subprocess.run(command, stdout=out, stderr=subprocess.STDOUT, check=True)
        print('Built V38', tag, flush=True)


def run(a, tag, mode, seed):
    cpu=16 if 16 in os.sched_getaffinity(0) else min(os.sched_getaffinity(0))
    env=os.environ.copy();env['UBSAN_OPTIONS']='halt_on_error=1:print_stacktrace=1'
    path=a.work/f'{mode}-{tag}-{seed}.jsonl'
    with path.open('w') as out, path.with_suffix('.log').open('w') as err:
        subprocess.run(['taskset','-c',str(cpu),str(a.work/f'decode-{tag}'),str(a.harness),
                        str(a.stats/'request.bin'),str(a.work),mode,str(seed),
                        '33' if mode=='validate' else '9'],stdout=out,stderr=err,env=env,check=True)
    print('Completed V38',tag,mode,seed,flush=True)


def report(a):
    sizes=json.loads((a.work/'sizes.json').read_text())
    inputs=json.loads((a.work/'inputs.json').read_text())
    for path, expected in inputs.items(): assert digest(path)==expected, path
    for row in sizes:
        path=a.work/f"{row['dataset']}.{row['scope']}.raw.par3"
        assert path.stat().st_size==row['bytes'] and digest(path)==row['sha256']
    baseline=[]
    for suite in ('additional-raincloud','wide-raincloud'):
        path=a.harness/f'{suite}-results.json';inputs[str(path)]=digest(path)
        for row in json.loads(path.read_text())['sizes']:
            entry=dict(dataset=row['dataset'],scope=row['scenario_id'])
            for variant in ('current','modular'):
                artifact=row['artifacts'][variant];path=a.harness/artifact['path']
                assert path.stat().st_size==artifact['bytes'] and digest(path)==artifact['sha256']
                inputs[str(path)]=digest(path);entry[variant]=artifact['bytes']
            baseline.append(entry)
    validation={}
    for tag in ('release','sanitize'):
        validation[tag]={}
        for mode in ('fixtures','validate'):
            path=a.work/f'{mode}-{tag}-0.jsonl'
            rows=[json.loads(line) for line in path.read_text().splitlines()]
            assert rows==([dict(v38_fixture_checks=88,ragged_empty_shared_dictionary=True,malformed_rejected=True)]
                          if mode=='fixtures' else [dict(validated_queries=2508,cross_format_hashes=True)])
            log=path.with_suffix('.log').read_text()
            assert not any(s in log for s in ('runtime error:','ERROR: AddressSanitizer','ERROR: LeakSanitizer'))
            validation[tag][mode]=rows
    runs=[]
    for seed in SEEDS:
        rows=[json.loads(line) for line in (a.work/f'time-release-{seed}.jsonl').read_text().splitlines()]
        assert len(rows)==960
        assert all(len(r['samples_ns'])==7 and statistics.median(r['samples_ns'])==r['median_ns'] for r in rows)
        runs.append(rows)
    by_run=[{(r['dataset'],r['scope'],r['variant'],r['column']):r for r in rows} for rows in runs]
    assert all(len(r)==960 and set(r)==set(by_run[0]) for r in by_run)
    positions=[]
    for key in sorted(by_run[0]):
        values=[r[key]['median_ns'] for r in by_run]
        positions.append(dict(zip(('dataset','scope','variant','column'),key),median_ns=statistics.median(values),process_ns=values))
    summary=[]
    for name,scope in sorted((r['dataset'],r['scope']) for r in sizes):
        for variant in ('current','modular','v38'):
            for workload in ('one','all'):
                chosen=[r for r in positions if (r['dataset'],r['scope'],r['variant'])==(name,scope,variant)
                        and (r['column']>=0)==(workload=='one')]
                assert len(chosen)==(9 if workload=='one' else 1)
                process=[statistics.median(r['process_ns'][i] for r in chosen) for i in range(3)]
                summary.append(dict(dataset=name,scope=scope,variant=variant,workload=workload,
                                    median_ns=statistics.median(r['median_ns'] for r in chosen),process_ns=process))
    timing={(r['dataset'],r['scope'],r['variant'],r['workload']):r['median_ns']/1000 for r in summary}
    byte_sizes={(r['dataset'],r['scope']):r for r in sizes}
    bases={(r['dataset'],r['scope']):r for r in baseline}
    csv_rows=[]
    for name,scope in sorted(byte_sizes):
        for variant,label in [('current','current_parquet_thrift'),('modular','modular_footer'),('v38','direct_v38')]:
            n=byte_sizes[name,scope]['bytes'] if variant=='v38' else bases[name,scope][variant]
            csv_rows.append(dict(dataset=name,scope=scope,format=label,bytes=n,
                                 one_column_us=f"{timing[name,scope,variant,'one']:.5f}".rstrip('0').rstrip('.'),
                                 all_columns_us=f"{timing[name,scope,variant,'all']:.5f}".rstrip('0').rstrip('.')))
    with (HERE/'proposal-comparison.csv').open('w') as f:
        w=csv.DictWriter(f,fieldnames=csv_rows[0].keys(),lineterminator='\n');w.writeheader();w.writerows(csv_rows)
    source_paths=[HERE/n for n in ('v38_reader.h','v38_decode.cc','v38_benchmark.py','ExplicitIndexFooter.thrift')]
    source_paths += list(a.work.glob('*.h'))+[a.work/'dbp.cc',a.work/'decode-release',a.work/'decode-sanitize']
    provenance=dict(hashes={str(p):digest(p) for p in source_paths},seeds=SEEDS,trials=7,
                    cpu=16 if 16 in os.sched_getaffinity(0) else min(os.sched_getaffinity(0)),
                    cpu_info=Path('/proc/cpuinfo').read_text(),
                    builds={tag:json.loads((a.work/f'build-{tag}.json').read_text()) for tag in ('release','sanitize')},
                    compiler=subprocess.check_output(['g++','--version'],text=True).splitlines()[0],
                    git_head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=HERE,text=True).strip())
    result=dict(sizes=sizes,baselines=baseline,summary=summary,per_position=positions,runs=runs,
                validation=validation,inputs=inputs,provenance=provenance)
    (HERE/'v38-results.json.gz').write_bytes(gzip.compress(json.dumps(result).encode(),mtime=0))
    labels={0:'Core',1:'Core + chunk statistics',2:'Core + chunk statistics + page locations',
            3:'Core + chunk statistics + page locations + page statistics'}
    queries={0:'Chunk metadata',1:'Chunk statistics',2:'Chunk metadata + page locations',3:'Chunk + page statistics'}
    section=['## Size and decoding comparison','',
        'Fresh measurements of V38 against the existing Parquet Compact-Thrift footer (current) and the modular footer. All three formats use the same eight input files and run interleaved in one benchmark executable. Metadata is encoded without Zstd. V38 uses direct offsets, a dictionary table, and cumulative-end indexes with an implicit initial zero.','',
        '### Size','',
        'ClickBench, bytes of complete metadata at each cumulative scope, including schema, directories, page headers, and required external indexes:','',
        '| Metadata retained | Current Parquet Thrift | Modular footer | V38 |','|---|---:|---:|---:|']
    for scope,label in labels.items():
        b=bases['clickbench-hits',scope];v=byte_sizes['clickbench-hits',scope]['bytes']
        section.append(f"| {label} | {b['current']:,} | {b['modular']:,} | {v:,} |")
    core={variant:sum(r[variant] for r in baseline if r['scope']==0) for variant in ('current','modular')}
    total=sum(r['bytes'] for r in sizes if r['scope']==0)
    previous=sum(r['v35_bytes'] for r in sizes if r['scope']==0)
    section+=['',f"Across all eight datasets, core metadata totals **{core['current']:,} bytes** for current Thrift, **{core['modular']:,}** for modular, and **{total:,}** for V38: reductions of {100*(1-total/core['current']):.1f}% and {100*(1-total/core['modular']):.1f}%, respectively. Small core footers can still be larger than modular. Omitting the initial zeros changes the total by only {total-previous:+,} byte versus V35; encoding headers and first-value encoding can offset the removed values.",'',
        '### Decoding time','',
        'ClickBench, microseconds per query; lower is better:','',
        '| Query | Columns | Current Parquet Thrift | Modular footer | V38 |','|---|---|---:|---:|---:|']
    for scope,label in queries.items():
        for workload in ('one','all'):
            values=[timing['clickbench-hits',scope,v,workload] for v in ('current','modular','v38')]
            section.append('| '+label+' | '+workload+' | '+' | '.join(f'{v:,.2f}' for v in values)+' |')
    core_slower=100*(timing['clickbench-hits',0,'v38','all']/timing['clickbench-hits',0,'modular','all']-1)
    stats_slower=100*(timing['clickbench-hits',3,'v38','all']/timing['clickbench-hits',3,'modular','all']-1)
    speed_summary=(f'V38 is faster than modular for all four ClickBench one-column projections. '
                   f'All-column results are mixed: chunk metadata takes {core_slower:.0f}% more time and combined chunk/page statistics '
                   f'{stats_slower:.0f}% more time, while chunk statistics and chunk metadata with page locations are faster. '
                   'These results do not establish a universal decoding advantage.')
    section+=['',
        speed_summary,'',
        'Queries use fresh prototype readers over memory-resident bytes, including parsing, decoding, allocation, constructing projected values, and checksumming; file I/O is excluded. One-column results are medians across nine column positions and read every chunk/page needed for that column. Each format ran seven trials in each of three processes, pinned to CPU 16 on an Intel Xeon Platinum 8375C. Queried fields can be a subset of the retained metadata. Comparisons cover placement and supported pruning information, rather than exact null-count queries.','',
        'See the [full measurements and validation](V38-MEASUREMENTS.md), [comparison CSV](proposal-comparison.csv), and [saved samples and provenance](v38-results.json.gz). Sizes and timings here both measure V38; they do not reuse V31 timings.','']
    proposal=HERE/'PROPOSAL.md';text=proposal.read_text()
    start=text.index('## Size and decoding comparison\n');end=text.index('## Supporting material\n',start)
    text=text[:start]+'\n'.join(section)+'\n'+text[end:]
    # Replace stale benchmark status while retaining the historical schema references.
    start=text.index('## Supporting material\n')
    text=text[:start]+'''## Supporting material

The [full IDL](ExplicitIndexFooter.thrift) defines each field's type, value count, nullability, and dependencies. [V38 measurements](V38-MEASUREMENTS.md) compare the current draft with current Parquet Thrift and the modular footer. Historical [V35 direct-offset results](DIRECT-OFFSETS.md) and [V31 explicit-index](legacy/ExplicitIndexFooterV31.thrift)/[V32 nested-layout](ImplicitLevelsFooter.thrift) experiments describe earlier revisions. Production encryption and a public interoperability suite remain outside the validated scope.
'''
    proposal.write_text(text)
    names={'uci-adult':'UCI Adult','uci-individual-household-electric-power-consumption':'Household Power',
           'kepler-exoplanet-search-results':'Kepler Search','ampds-whe':'AMPds','clickbench-hits':'ClickBench',
           'bi-uscensus':'BI-USCensus','bi-wins':'BI-Wins','kepler-labelled-time-series-data':'Kepler Time Series'}
    lines=['# V38 size and decoding measurements','',
        'Fresh comparison of current Parquet Compact Thrift, the modular footer, and V38. V38 stores direct chunk offsets/lengths, a dictionary offset/length table, nullable chunk dictionary IDs, and shared cumulative-end indexes. Both hierarchy indexes omit the fixed leading zero; relative page offsets retain one value per page, including each chunk’s first zero.','',
        'All metadata is encoded without Zstd. Size is the complete retained metadata, including schema, directories, page headers, required external indexes, and trailer. Integer and dictionary encodings remain enabled. The source corpus is the same eight parquet-java files described in [EXPERIMENT-RESULTS.md](EXPERIMENT-RESULTS.md#2-java-written-inputs).','',
        '## Core size','',
        '| Dataset | Current bytes | Modular bytes | V38 bytes | V38 − V35 bytes |','|---|---:|---:|---:|---:|']
    for name in names:
        b=bases[name,0];v=byte_sizes[name,0]
        lines.append(f"| {names[name]} | {b['current']:,} | {b['modular']:,} | {v['bytes']:,} | {v['bytes']-v['v35_bytes']:+,} |")
    lines.append(f"| **Total** | **{core['current']:,}** | **{core['modular']:,}** | **{total:,}** | **{total-previous:+,}** |")
    lines+=['','Removing two logical values does not guarantee fewer encoded bytes. V38 re-encodes only the affected hierarchy streams with the existing encoding/capacity sweep; all other streams match V35 byte-for-byte. Changed first values and encoding parameters can offset the removed zeros.','',
        '## Decoding by query','',
        'Times are microseconds per query. Each cell is **one column / all columns**. One-column values are the median over nine selected positions, after taking each position’s median across three fresh processes. Every process runs seven shuffled trials per case. All three formats are interleaved on CPU 16 (Intel Xeon Platinum 8375C); input bytes are resident, and each query constructs a new reader.','']
    for scope,title in queries.items():
        lines += ['### '+title,'','| Dataset | Current µs | Modular µs | V38 µs |','|---|---:|---:|---:|']
        for name in names:
            cells=[' / '.join(f"{timing[name,scope,v,w]:,.2f}" for w in ('one','all')) for v in ('current','modular','v38')]
            lines.append('| '+names[name]+' | '+' | '.join(cells)+' |')
        lines.append('')
    lines += [speed_summary,'']
    wins={(scope,w):sum(timing[name,scope,'v38',w]<timing[name,scope,'modular',w] for name in names)
          for scope in queries for w in ('one','all')}
    lines+=['## Scope and validation','',
        'Each projection constructs and checksums the same logical output. Chunk metadata includes data start/end, dictionary start, codec, value count, uncompressed size, and dictionary-use flag; the page projection adds byte extents and first-row positions. Statistics projections return row starts, min/max bounds, and pruning flags. Dictionary lengths and row-group byte hints are preserved from the validated V35 artifacts but are not part of the timed output. Exact null counts, encryption, and cold-storage latency are not benchmarked.','',
        'Release and ASan/UBSan each pass **2,508 cross-format queries** against the independent source oracle and **88 fixture checks**. Fixtures cover unequal/empty columns, zero-page chunks, same-column dictionary reuse, absent dictionaries, zero-column files, chunk/page/arbitrary statistics, missing bounds, statistics without page offsets, and malformed indexes/locations/statistics. Output hashes agree across formats. The real corpus uses regular row-group-aligned chunks; ragged layouts are correctness fixtures, not timing workloads.','',
        'These are prototype readers using the existing external codec harness, including repository-local experimental encodings. They are not a production implementation or an interoperable public encoding profile. Selective validation checks the accessed ranges; preparation and full validation check the complete source outputs.','',
        '## Reproduction','',
        'The checked-in writer/orchestrator is [v38_benchmark.py](v38_benchmark.py); native V38 addressing is in [v38_reader.h](v38_reader.h), and the matched driver/fixtures are in [v38_decode.cc](v38_decode.cc). This run reuses the saved V35 artifacts, frozen `work/refined-hierarchy` reader support/build commands, `work/list-statistics/request.bin`, and the existing external footer/codec harness. It also uses local helpers from earlier experiments, including `tagged_row_ranges.py`, `fast_statistics_reader.h`, and `refined_statistics_rows.h`. Those historical work directories, helper dependencies, and external Runtime codecs are not packaged here; this is a local-harness runner, not a self-contained public benchmark.','',
        '```sh','/home/jiayi.wang/raincloud/.venv/bin/python footer-pages/v38_benchmark.py --stage all','```','',
        'The [CSV](proposal-comparison.csv) contains all 32 dataset/scope combinations for all three formats. Scope 0 is core; 1 adds chunk statistics; 2 adds page locations; 3 adds page statistics. The [result archive](v38-results.json.gz) records all trial samples, per-position/process medians, validation results, artifact/source hashes, CPU information, and exact build commands.','']
    (HERE/'V38-MEASUREMENTS.md').write_text('\n'.join(lines))
    print('V38 core total:',total,'; V35 delta:',total-previous,flush=True)
    print('V38 faster than modular counts (out of 8 per scope/projection):',wins,flush=True)
    print('Reported 32 sizes and 192 timing summaries; updated proposal and CSV.',flush=True)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--harness',type=Path,default=Path('/home/jiayi.wang/parquet-footer-bench/footer-pages'))
    p.add_argument('--direct',type=Path,default=HERE/'work/direct-explicit-20261026')
    p.add_argument('--support',type=Path,default=HERE/'work/refined-hierarchy')
    p.add_argument('--stats',type=Path,default=HERE/'work/list-statistics')
    p.add_argument('--work',type=Path,default=HERE/'work/v38-20261027')
    p.add_argument('--stage',choices=('prepare','build','validate','time','report','all'),default='all')
    a=p.parse_args()
    for key in ('harness','direct','support','stats','work'):setattr(a,key,getattr(a,key).resolve())
    a.work.mkdir(parents=True,exist_ok=True)
    if a.stage in ('prepare','all'):prepare(a)
    if a.stage in ('build','all'):build(a)
    if a.stage in ('validate','all'):
        for tag in ('release','sanitize'):
            run(a,tag,'fixtures',0);run(a,tag,'validate',0)
    if a.stage in ('time','all'):
        for seed in SEEDS:run(a,'release','time',seed)
    if a.stage in ('report','all'):
        report(a)

if __name__=='__main__':main()
