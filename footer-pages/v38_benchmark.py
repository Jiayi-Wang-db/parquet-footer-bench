"""Prepare and benchmark V38 using the existing frozen footer/codec harness.

Requires the saved V35 artifacts, refined-hierarchy support, and external codec
harness documented in V38-MEASUREMENTS.md. Historical artifacts are read-only.
"""
import argparse
import csv
import gzip
import hashlib
import importlib.util
import itertools
import json
import os
from pathlib import Path
import shutil
import statistics
import struct
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


def prepare_initial(a):
    """Apply the same V38 layout/encoding policy to the four original shards."""
    direct=load_direct(a);codec,m=direct.a,direct.m
    from list_core_fields import CHUNKS, PAGES, encode_pair, fields
    from list_statistics import group_columns, read_oracles, write_request, VARIANTS
    from tagged_row_ranges import Footer, rewrite
    spec=importlib.util.spec_from_file_location('dictionary_table',a.direct.parent/'direct-offset-dictionary-table-20261026/measure.py')
    table=importlib.util.module_from_spec(spec);spec.loader.exec_module(table)
    table.audit=codec;table.m=m
    folder=a.work/'initial';folder.mkdir(exist_ok=True);codec.setup(folder)
    idl=m.thriftpy2.load(str(HERE/'ExplicitIndexFooter.thrift'),module_name='v38_initial_thrift')
    manifest=a.harness/'matched-v21-results.json'
    request=a.harness/'work/v21-matched-20261021/request.bin'
    inputs={str(p):digest(p) for p in (manifest,request)}
    previous={(r['dataset'],r['scenario_id']):r for r in json.loads(manifest.read_text())['sizes']}
    sizes=[]
    for source in codec.sources(request):
        name,base,C,G=(source[k] for k in ('name','base','C','G'))
        def original(scope):
            artifact=previous[name,scope]['v2_uncompressed_control'];p=a.harness/artifact['path']
            inputs[str(p)]=digest(p);assert inputs[str(p)]==artifact['sha256']
            assert p.stat().st_size==artifact['bytes']
            return Footer(m,codec.decoder,p.read_bytes(),base)
        core=original(0);regions=core.values('4.1')
        columns=fields(source,regions)
        columns['6.6']=[[source['chunks'][c*G+k]['pages'][0][2] for k in range(G)] for c in range(C)]
        for key in PAGES:
            columns[key]=[list(itertools.chain.from_iterable(columns[key][c*G:(c+1)*G])) for c in range(C)]
        encoded={}
        for key,physical in {**CHUNKS,'6.6':2,**PAGES}.items():
            cols=columns[key];index=[0]+list(itertools.accumulate(map(len,cols)))
            selected=encode_pair(m,codec.encoder,physical,key=='6.2',cols,group_columns(index,16384))['raw'][0]
            encoded[key]=selected['data'],selected['codec']
        ci=[c*G for c in range(C+1)]
        pi=[0]+list(itertools.accumulate(len(ch['pages']) for ch in source['chunks']))
        dictionaries=codec.decode(core.payload('6.2'),1)
        originals=[[ch['data'] for ch in source['chunks']],
                   [ch['end']-ch['data'] for ch in source['chunks']],
                   [None if d is None else regions[d] for d in dictionaries],
                   [None if d is None else regions[d+1]-regions[d] for d in dictionaries]]
        assert originals[2]==[None if ch['dict']<0 else ch['dict'] for ch in source['chunks']]
        _,locations,_,_,_=table.encode_fields(originals,ci,order='first_reference')
        replacements={**encoded,**locations,'3.1':codec.encoded(ci[1:],1),'3.2':codec.encoded(pi[1:],1),
                      '5.2':codec.encoded([regions[i] for i in core.values('5.2',1)])}
        for scope in range(4):
            old=original(scope)
            tagged,_=rewrite(old,[0,1][:len(m.seq(old.root,8) or [])])
            root,streams=codec.unpack(m,tagged,base)
            m.field(root,1,m.t.I32,38)
            for key,value in replacements.items():
                if key=='3.2' and scope<2:continue
                if key in PAGES and scope<2:continue
                if key=='6.6' and scope==0:continue
                if key=='4.2':continue # This slot now holds dictionary lengths.
                group,fid=map(int,key.split('.'))
                if m.get(root,group) is None:m.field(root,group,m.t.STRUCT,[])
                m.field(m.get(root,group),fid,m.t.STRUCT,[]);streams[key]=value
            # The direct dictionary table replaces the old chunk-region stream.
            streams['4.2']=locations['4.2']
            if scope<2:m.get(root,3)[:]=[f for f in m.get(root,3) if f[0]!=2]
            for gran,item in enumerate(m.seq(root,8) or []):
                expected=ci if gran==0 else [pi[k] for k in ci]
                if old.values(f'8.{gran}.1')==expected:
                    assert m.get(m.get(item,2),13)==gran
                    item[:]=[f for f in item if f[0]!=1]
                else:
                    # Preserve sparse statistics exactly using the arbitrary-range form.
                    stats_fields=m.get(item,2);m.field(stats_fields,13,m.t.I32,2)
                    m.field(stats_fields,1,m.t.STRUCT,[])
                    key=f'8.{gran}.2.1'
                    streams[key]=old.payload(key),m.get(old.locations[key],3)
            blob=codec.pack(m,root,streams,base)
            checked_root,checked=codec.unpack(m,blob,base)
            assert m.read(idl.ExplicitIndexFooter,m.t.encode(checked_root)).version==38
            values=direct.values(checked)
            for key in ('3.1','3.2'):
                if key in values:values[key]=[0]+values[key]
            direct.validate(values);table.validate(checked,originals)
            for key,value in checked.items():
                assert value[1]==0
                if key.startswith('8.'):assert value==(old.payload(key),m.get(old.locations[key],3))
            assert codec.pack(m,checked_root,checked,base)==blob
            path=folder/f'{name}.{scope}.raw.par3';path.write_bytes(blob)
            kinds=[m.get(m.get(item,2),13) for item in m.seq(root,8) or []]
            sizes.append(dict(dataset=name,scope=scope,bytes=len(blob),v35_bytes=None,sha256=digest(path),base=base,statistics_kinds=kinds))
            if scope in (1,3):
                for variant in VARIANTS:
                    alias=folder/f'{name}.{scope}.{variant}.raw.par3'
                    if not alias.exists():alias.symlink_to(path.name)
        codec.encoder.cache.clear();print('Prepared initial V38',name,flush=True)
    codec.encoder.native.replay.close()
    # Feed the unchanged native validator all four shards in its first suite.
    for suite,data in [('additional-raincloud',request.read_bytes()),('wide-raincloud',struct.pack('>I',0))]:
        p=folder/f'harness/work/{suite}-20261022/request.bin';p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(data)
    write_request(folder/'request.bin',read_oracles(request),folder)
    (folder/'sizes.json').write_text(json.dumps(sizes,indent=2)+'\n')
    (folder/'inputs.json').write_text(json.dumps(inputs,indent=2)+'\n')


def validate_initial(a):
    folder=a.work/'initial'
    for tag in ('release','sanitize'):
        path=folder/f'validate-{tag}.jsonl'
        env=os.environ.copy();env['UBSAN_OPTIONS']='halt_on_error=1:print_stacktrace=1'
        with path.open('w') as out,path.with_suffix('.log').open('w') as err:
            subprocess.run([str(a.work/f'decode-{tag}'),str(folder/'harness'),str(folder/'request.bin'),
                            str(folder),'validate','0','33'],stdout=out,stderr=err,env=env,check=True)
        assert json.loads(path.read_text())==dict(validated_queries=960,cross_format_hashes=True)
        print('Validated initial V38',tag,flush=True)


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
    # Range reads visit only overlapping pages. Bulk loads already prepare values,
    # so they do not need the scalar reader's rank prepass.
    page_path=a.work/'list_page_readers.h';pages=page_path.read_text()
    old='''    for(int i=0;i<int(s.directory.size());++i) {
      auto& p=s.directory[i];int begin=std::max(lo,p.first),end=std::min(hi,p.end());'''
    new='''    auto first_page=std::lower_bound(s.directory.begin(),s.directory.end(),lo,
        [](const HeaderInfo& p,int n){return p.end()<=n;});
    for(int i=first_page-s.directory.begin();i<int(s.directory.size()) && s.directory[i].first<hi;++i) {
      auto& p=s.directory[i];int begin=std::max(lo,p.first),end=std::min(hi,p.end());'''
    assert pages.count(old)==1;pages=pages.replace(old,new)
    old='      for(int j=begin;j<end;j+=stride)if(int rank='
    assert pages.count(old)==1;pages=pages.replace(old,old.replace('for(', 'if(!bulk)for(', 1))
    page_path.write_text(pages)
    shutil.copyfile(HERE/'fast_statistics_reader.h', a.work/'fast_statistics_reader.h')
    core = (a.support/'core_base.h').read_text()
    old='(version==23 || (version>=26 && version<=32))'
    assert core.count(old)==1
    core=core.replace(old, 'version==38')
    # Avoid copying the string-capable Bound wrapper for each core scalar.
    old='[&](Bound b){sink(i++,b);}'
    assert core.count(old)==1;core=core.replace(old,'[&](const Bound& b){sink(i++,b);}')
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


def run(a, tag, mode, seed, initial=False):
    cpu=16 if 16 in os.sched_getaffinity(0) else min(os.sched_getaffinity(0))
    env=os.environ.copy();env['UBSAN_OPTIONS']='halt_on_error=1:print_stacktrace=1'
    folder=a.work/'initial' if initial else a.work
    harness=folder/'harness' if initial else a.harness
    stats=folder if initial else a.stats
    path=folder/f'{mode}-{tag}-{seed}.jsonl'
    with path.open('w') as out, path.with_suffix('.log').open('w') as err:
        subprocess.run(['taskset','-c',str(cpu),str(a.work/f'decode-{tag}'),str(harness),
                        str(stats/'request.bin'),str(folder),mode,str(seed),
                        '33' if mode=='validate' else '9'],stdout=out,stderr=err,env=env,check=True)
    print('Completed V38', 'initial' if initial else 'additional/wide',tag,mode,seed,flush=True)


def report(a):
    sizes=json.loads((a.work/'sizes.json').read_text())
    inputs=json.loads((a.work/'inputs.json').read_text())
    sizes+=json.loads((a.work/'initial/sizes.json').read_text())
    inputs.update(json.loads((a.work/'initial/inputs.json').read_text()))
    for path, expected in inputs.items(): assert digest(path)==expected, path
    for row in sizes:
        folder=a.work if row['v35_bytes'] is not None else a.work/'initial'
        path=folder/f"{row['dataset']}.{row['scope']}.raw.par3"
        assert path.stat().st_size==row['bytes'] and digest(path)==row['sha256']
    baseline=[]
    for suite in ('additional-raincloud','wide-raincloud','matched-v21'):
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
        path=a.work/'initial'/f'validate-{tag}.jsonl'
        rows=[json.loads(line) for line in path.read_text().splitlines()]
        assert rows==[dict(validated_queries=960,cross_format_hashes=True)]
        log=path.with_suffix('.log').read_text()
        assert not any(s in log for s in ('runtime error:','ERROR: AddressSanitizer','ERROR: LeakSanitizer'))
        validation[tag]['initial']=rows
    runs=[]
    for seed in SEEDS:
        rows=[json.loads(line) for line in (a.work/f'time-release-{seed}.jsonl').read_text().splitlines()]
        assert len(rows)==960
        initial=[json.loads(line) for line in (a.work/f'initial/time-release-{seed}.jsonl').read_text().splitlines()]
        assert len(initial)==480
        rows+=initial
        assert all(len(r['samples_ns'])==7 and statistics.median(r['samples_ns'])==r['median_ns'] for r in rows)
        runs.append(rows)
    by_run=[{(r['dataset'],r['scope'],r['variant'],r['column']):r for r in rows} for rows in runs]
    assert all(len(r)==1440 and set(r)==set(by_run[0]) for r in by_run)
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
            times={}
            for workload,column in [('one','one_column_us'),('all','all_columns_us')]:
                value=timing[name,scope,variant,workload]
                times[column]=f'{value:.5f}'.rstrip('0').rstrip('.')
            csv_rows.append(dict(dataset=name,scope=scope,format=label,bytes=n,**times))
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
    provenance['timing_campaigns']=[
        dict(datasets=count,processes=[dict(seed=seed,path=str(folder/f'time-release-{seed}.jsonl'),
             sha256=digest(folder/f'time-release-{seed}.jsonl')) for seed in SEEDS])
        for folder,count in ((a.work,8),(a.work/'initial',4))]
    result=dict(sizes=sizes,baselines=baseline,summary=summary,per_position=positions,runs=runs,
                validation=validation,inputs=inputs,provenance=provenance)
    (HERE/'v38-results.json.gz').write_bytes(gzip.compress(json.dumps(result).encode(),mtime=0))
    labels={0:'Core',1:'Core + chunk statistics',2:'Core + chunk statistics + page locations',
            3:'Core + chunk statistics + page locations + page statistics'}
    queries={0:'Chunk metadata',1:'Chunk statistics',2:'Chunk metadata + page locations',3:'Chunk + page statistics'}
    names={'us-accidents-00004-of-00007':'US Accidents','fineweb-10bt-000':'FineWeb',
           'hacker-news-00000-of-00039':'Hacker News','yellow-tripdata-2025-01':'NYC Taxi',
           'uci-adult':'UCI Adult','uci-individual-household-electric-power-consumption':'Household Power',
           'kepler-exoplanet-search-results':'Kepler Search','ampds-whe':'AMPds','clickbench-hits':'ClickBench',
           'bi-uscensus':'BI-USCensus','bi-wins':'BI-Wins','kepler-labelled-time-series-data':'Kepler Time Series'}
    assert set(names)=={r['dataset'] for r in summary}
    size_tables=[]
    for scope,label in labels.items():
        size_tables+=['#### '+label,'',
                      '| Dataset | Current Parquet Thrift | Modular footer | Benchmark implementation |','|---|---:|---:|---:|']
        for name in names:
            b=bases[name,scope];v=byte_sizes[name,scope]['bytes']
            size_tables.append(f"| {names[name]} | {b['current']:,} | {b['modular']:,} | {v:,} |")
        totals=[sum(bases[name,scope][variant] for name in names) for variant in ('current','modular')]
        totals.append(sum(byte_sizes[name,scope]['bytes'] for name in names))
        size_tables.append('| **Total** | '+' | '.join(f'**{v:,}**' for v in totals)+' |')
        size_tables.append('')
    timing_tables=[]
    for scope,title in queries.items():
        timing_tables+=['#### '+title,'',
                       '| Dataset | Current Parquet Thrift | Modular footer | Benchmark implementation |','|---|---:|---:|---:|']
        for name in names:
            cells=[' / '.join(f"{timing[name,scope,v,w]:,.2f}" for w in ('one','all')) for v in ('current','modular','v38')]
            timing_tables.append('| '+names[name]+' | '+' | '.join(cells)+' |')
        timing_tables.append('')
    section=['## Size and decoding comparison','',
        'The benchmark compares the proposed stream representation with the existing Parquet Compact-Thrift footer and the modular footer on the same 12 datasets. All three implementations are interleaved in each timing run. Metadata is encoded without Zstd. The benchmark implementation uses direct offsets, a dictionary table, and cumulative-end indexes with an implicit initial zero.','',
        'Benchmark scope: the tables use the [benchmark schema](ExplicitIndexFooter.thrift) and measure placement and pruning predicates. Exact-count statistics, the proposed directory serialization, encryption, and column sets are outside the measured scope.','',
        '### Size','',
        'All values are bytes of complete metadata at each cumulative scope, including schema, directories, page headers, required external indexes, and trailer. Each table includes every dataset in the corpus.','']+size_tables
    core={variant:sum(r[variant] for r in baseline if r['scope']==0) for variant in ('current','modular')}
    total=sum(r['bytes'] for r in sizes if r['scope']==0)
    delta=sum(r['bytes']-r['v35_bytes'] for r in sizes if r['scope']==0 and r['v35_bytes'] is not None)
    sparse_note='FineWeb has no page statistics for one column. Its page-statistics set uses the supported ARBITRARY form to preserve that sparse coverage; complete chunk/page sets reuse the shared indexes.'
    section+=[f"Across all 12 datasets, core metadata totals **{core['current']:,} bytes** for current Thrift, **{core['modular']:,}** for modular, and **{total:,}** for the benchmark implementation: reductions of {100*(1-total/core['current']):.1f}% and {100*(1-total/core['modular']):.1f}%, respectively. Small core footers can be larger than modular.",'',
        sparse_note,'',
        '### Decoding time','',
        'Microseconds per query; lower is better. Each cell is **one column / all columns**. All 12 datasets are shown for each query.','']+timing_tables
    cases=list(itertools.product(names,queries,('one','all')))
    faster={v:sum(timing[n,s,'v38',w]<timing[n,s,v,w] for n,s,w in cases) for v in ('current','modular')}
    speed_summary=(f"The benchmark implementation takes less decoding time than current Parquet Thrift in **{faster['current']}/{len(cases)}** "
                   f"measured cases and less than modular in **{faster['modular']}/{len(cases)}**. "
                   'Each case is one dataset, query, and projection; performance depends on the workload and reader implementation.')
    regressions=['### Cases more than 10% slower than modular','',
                 'Percentages compare decoding time using unrounded medians.','',
                 '| Query | One column | All columns |','|---|---|---|']
    for scope,title in queries.items():
        cells=[]
        for workload in ('one','all'):
            slower=[(timing[n,scope,'v38',workload]/timing[n,scope,'modular',workload]-1,n) for n in names]
            cells.append('; '.join(f'{names[n]} +{100*delta:.1f}%' for delta,n in sorted(slower,reverse=True) if delta>0.1) or 'None')
        regressions.append('| '+title+' | '+' | '.join(cells)+' |')
    regressions.append('')
    reader_details='The benchmark reader caches dictionary offsets for full core reads, using 8 bytes per dictionary entry. Range reads visit only overlapping metadata pages.'
    section+=[
        speed_summary,'',reader_details,'']+regressions+[
        'Queries construct fresh readers over memory-resident bytes and include parsing, decoding, allocation, projected values, and checksumming; file I/O is excluded. One-column results are medians across nine column positions and read every chunk/page needed for that column. Each format runs seven trials in each of three processes per dataset, pinned to CPU 16 on an Intel Xeon Platinum 8375C, using the same executable and protocol across all datasets. Queried fields can be a subset of the retained metadata. Comparisons cover placement and supported pruning information; exact null-count queries are excluded.','',
        'See the [measurement details and validation](V38-MEASUREMENTS.md), [comparison CSV](proposal-comparison.csv), and [samples and provenance](v38-results.json.gz).','',
        'The tables use size-minimizing encoding. The recommended [encoding policy](V38-PFOR.md) uses ordinary-frame PFOR for relative page offsets and adaptive encoding for hierarchy indexes. Matched measurements show 4.0% lower selective decoding time for chunk metadata plus page locations than size-minimizing encoding. Metadata at that scope totals 3,826,102 bytes versus modular\'s 8,830,452 bytes (56.7% smaller); full metadata totals 12,321,007 versus 25,272,629 bytes (51.2% smaller). Both scopes are smaller than modular on all 12 datasets. Encoding policy is a writer choice; the schema does not mandate PFOR.','']
    proposal=HERE/'PROPOSAL.md';text=proposal.read_text()
    start=text.index('## Size and decoding comparison\n');end=text.index('## Supporting material\n',start)
    text=text[:start]+'\n'.join(section)+'\n'+text[end:]
    # Keep supporting links aligned with the presentation.
    start=text.index('## Supporting material\n')
    text=text[:start]+'''## Supporting material

The [full IDL](proposal/ModularFooterV2.thrift) defines each field's type, value count, nullability, and dependencies. The optional [column-set extension](proposal/ColumnSetFooter.thrift) reuses those types. [Benchmark details](V38-MEASUREMENTS.md) document the inputs, methodology, validation, and reproducibility requirements. Encryption and public interoperability are outside the benchmark validation scope.
'''
    proposal.write_text(text)
    lines=['# V38 size and decoding measurements','',
        'Fresh comparison of current Parquet Compact Thrift, the modular footer, and V38. V38 stores direct chunk offsets/lengths, a dictionary offset/length table, nullable chunk dictionary IDs, and shared cumulative-end indexes. Both hierarchy indexes omit the fixed leading zero; relative page offsets retain one value per page, including each chunk’s first zero.','',
        'All metadata is encoded without Zstd. Size is the complete retained metadata, including schema, directories, page headers, required external indexes, and trailer. Integer and dictionary encodings remain enabled. Sizes and decoding timings cover all 12 parquet-java files described in [EXPERIMENT-RESULTS.md](EXPERIMENT-RESULTS.md#2-java-written-inputs).','',
        '## Core size','',
        '| Dataset | Current bytes | Modular bytes | V38 bytes | V38 − V35 bytes |','|---|---:|---:|---:|---:|']
    for name in names:
        b=bases[name,0];v=byte_sizes[name,0]
        diff='—' if v['v35_bytes'] is None else f"{v['bytes']-v['v35_bytes']:+,}"
        lines.append(f"| {names[name]} | {b['current']:,} | {b['modular']:,} | {v['bytes']:,} | {diff} |")
    lines.append(f"| **Total** | **{core['current']:,}** | **{core['modular']:,}** | **{total:,}** | **{delta:+,} (8 matched)** |")
    lines+=['','Removing two logical values does not guarantee fewer encoded bytes. For the eight matched V35 datasets, V38 re-encodes only the affected hierarchy streams; all other streams match V35 byte-for-byte. Changed first values and encoding parameters can offset the removed zeros. A dash means no V35 measurement.','',
        'The four original shards are converted from their saved V21 artifacts using the same V38 layout, direct-location encoding sweep, and column-aligned metadata page policy. Core/chunk-statistics scopes omit the optional page index. Statistics value streams are preserved byte-for-byte; their row-range tags and shared indexes are updated.', '',
        sparse_note,'','## Size at every scope','']
    lines += [line.replace('#### ','### ',1) for line in size_tables]
    lines += [
        '## Decoding by query','',
        'Times are microseconds per query. Each cell is **one column / all columns**. One-column values are the median over nine selected positions, after taking each position’s median across three fresh processes. Every process runs seven shuffled trials per case. All three formats are interleaved on CPU 16 (Intel Xeon Platinum 8375C); input bytes are resident, and each query constructs a new reader. All timings were rerun with the optimized reader, using the same executable for the eight-dataset and four-dataset suites.','']
    lines += [line.replace('#### ','### ',1) for line in timing_tables]
    lines += [speed_summary,'',reader_details,'']+regressions
    wins={(scope,w):sum(timing[name,scope,'v38',w]<timing[name,scope,'modular',w] for name in names)
          for scope in queries for w in ('one','all')}
    lines+=['## Scope and validation','',
        'Each projection constructs and checksums the same logical output. Chunk metadata includes data start/end, dictionary start, codec, value count, uncompressed size, and dictionary-use flag; the page projection adds byte extents and first-row positions. Statistics projections return row starts, min/max bounds, and pruning flags. Dictionary lengths and row-group byte hints are stored in the artifacts but are not part of the timed output. Exact null counts, encryption, and cold-storage latency are not benchmarked.','',
        'Release and ASan/UBSan each pass **2,508 cross-format queries** against the independent source oracle and **88 fixture checks**. Fixtures cover unequal/empty columns, zero-page chunks, same-column dictionary reuse, absent dictionaries, zero-column files, chunk/page/arbitrary statistics, missing bounds, statistics without page offsets, and malformed indexes/locations/statistics. Output hashes agree across formats. The real corpus uses regular row-group-aligned chunks; ragged layouts are correctness fixtures, not timing workloads.','',
        'The four original shards pass another **960 cross-format queries** in both release and ASan/UBSan, using the same optimized executable. All 16 additional V38 artifacts were parsed through the current Thrift definition and checked against source chunk/dictionary extents during preparation; retained statistics streams were verified byte-for-byte. This reader-only rerun verifies the saved hashes of all 48 unchanged artifacts. Each timing process also validates its selected projections against the source oracle before measuring.','',
        'These are prototype readers using the existing external codec harness, including repository-local experimental encodings. They are not a production implementation or an interoperable public encoding profile. Selective validation checks the accessed ranges; preparation and full validation check the complete source outputs.','',
        '## Reproduction','',
        'The checked-in writer/orchestrator is [v38_benchmark.py](v38_benchmark.py); native V38 addressing is in [v38_reader.h](v38_reader.h), and the matched driver/fixtures are in [v38_decode.cc](v38_decode.cc). This run reuses the saved V35 artifacts, frozen `work/refined-hierarchy` reader support/build commands, `work/list-statistics/request.bin`, and the existing external footer/codec harness. It also uses local helpers from earlier experiments, including `tagged_row_ranges.py`, `fast_statistics_reader.h`, and `refined_statistics_rows.h`. Those historical work directories, helper dependencies, and external Runtime codecs are not packaged here; this is a local-harness runner, not a self-contained public benchmark.','',
        '```sh','/home/jiayi.wang/raincloud/.venv/bin/python footer-pages/v38_benchmark.py --stage all','```','',
        'The [CSV](proposal-comparison.csv) contains size and timing values for all 48 dataset/scope combinations and all three formats. Scope 0 is core; 1 adds chunk statistics; 2 adds page locations; 3 adds page statistics. The [result archive](v38-results.json.gz) records all trial samples, per-position/process medians, validation results, artifact/source hashes, CPU information, and exact build commands. Its `runs` arrays combine the two timing campaigns by seed; `provenance.timing_campaigns` identifies the six separate process outputs.','']
    (HERE/'V38-MEASUREMENTS.md').write_text('\n'.join(lines))
    print('V38 core total:',total,'; V35 delta for 8 matched datasets:',delta,flush=True)
    print('V38 faster than modular counts (out of 12 per scope/projection):',wins,flush=True)
    print('Reported 48 sizes and 288 timing summaries; updated proposal and CSV.',flush=True)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--harness',type=Path,default=Path('/home/jiayi.wang/parquet-footer-bench/footer-pages'))
    p.add_argument('--direct',type=Path,default=HERE/'work/direct-explicit-20261026')
    p.add_argument('--support',type=Path,default=HERE/'work/refined-hierarchy')
    p.add_argument('--stats',type=Path,default=HERE/'work/list-statistics')
    p.add_argument('--work',type=Path,default=HERE/'work/v38-optimized-20261027')
    p.add_argument('--stage',choices=('prepare','prepare-initial','build','validate','validate-initial','time','time-initial','report','all'),default='all')
    a=p.parse_args()
    for key in ('harness','direct','support','stats','work'):setattr(a,key,getattr(a,key).resolve())
    a.work.mkdir(parents=True,exist_ok=True)
    if a.stage in ('prepare','all'):prepare(a)
    if a.stage in ('prepare-initial','all'):prepare_initial(a)
    if a.stage in ('build','all'):build(a)
    if a.stage in ('validate','all'):
        for tag in ('release','sanitize'):
            run(a,tag,'fixtures',0);run(a,tag,'validate',0)
    if a.stage in ('validate-initial','all'):validate_initial(a)
    if a.stage in ('time','all'):
        for seed in SEEDS:run(a,'release','time',seed)
    if a.stage in ('time-initial','all'):
        for seed in SEEDS:run(a,'release','time',seed,initial=True)
    if a.stage in ('report','all'):
        report(a)

if __name__=='__main__':main()
