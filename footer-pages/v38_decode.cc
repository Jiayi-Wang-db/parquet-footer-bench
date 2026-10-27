// Matched V38/current/modular projections, with fresh readers and resident bytes.
#include "list_page_readers.h"
#include "matched_comparison.h"
#include "statistics_comparison.h"
namespace corebench {
using namespace matched;
#include "core_base.h"
}
#include "v38_reader.h"
namespace v38stats {
using namespace paired_bounds;using Data=listed::Data;using Range=listed::Range;
#include "v38_statistics.h"
uint64_t Run(Data& d,int scope,int variant,int column,bool validate=false,Counts* cost=nullptr) {
  pfor_reader_policy=column<0?1:2;
  v38stats::Reader reader(d,scope,variant,column<0);uint64_t hash=1;
  auto value=[&](const Bound& b){hash=hash*31+b.present;hash=hash*31+b.bits;for(uint8_t c:b.bytes)hash=hash*31+c;};
  for(int c=column<0?0:column;c<(column<0?d.C:column+1);++c)for(int g=0;g<(scope?2:1);++g) {
    auto ranges=reader.Stats(c,g);hash=hash*31+ranges.size();
    if(validate) {
      int first=d.starts[g][c],end=d.starts[g][c+1];Require(int(ranges.size())==end-first,"Oracle range count");
      for(int i=0;i<int(ranges.size());++i)Require(ranges[i]==d.ranges[g][first+i],"Source statistics mismatch");
    }
    for(auto& r:ranges){hash=hash*31+r.row;value(r.lo);value(r.hi);for(int f:r.flags)hash=hash*31+f;}
  }
  if(cost)*cost=reader.pages.counts;blackhole=hash;return hash;
}
}
void Fixtures(const std::string& folder) {
  using namespace paired_bounds;
  int checks=0;
  const std::vector<int> ci={0,0,3,4,4,6};
  const std::vector<int64_t> offsets={900,200,1700,400,3000,100},lengths={50,0,70,30,100,20};
  const std::vector<int64_t> dictionaries={5000,-1,5000,50,2900,2900},values={20,0,10,30,20,10};
  const std::vector<std::vector<int64_t>> pages={{0,20},{},{0},{0},{0,40},{0}},rows={{0,10},{},{20},{0},{0,10},{20}};
  for(auto name:{"ragged","no_dictionary","empty","zero_columns"}) {
    bool empty=std::string(name)=="empty" || std::string(name)=="zero_columns";
    matched::Data d;d.C=std::string(name)=="zero_columns"?0:5;d.G=99;d.rows=30;d.base=8192;
    for(int scope:{0,2})for(int col=-1;col<d.C;++col) {
      d.artifacts[scope][2].bytes=ReadFile(folder+"/fixtures/"+name+".par3");
      corebench::DirectCore reader(d,scope,col<0);matched::Output got(true),expected(true);
      for(int c=col<0?0:col;c<(col<0?d.C:col+1);++c) {
        reader.Column(c,got);
        if(!empty)for(int k=ci[c];k<ci[c+1];++k) {
          matched::ChunkInfo ch;ch.data=offsets[k];ch.end=offsets[k]+lengths[k];
          ch.dict=std::string(name)=="no_dictionary"?-1:dictionaries[k];ch.codec=0;
          ch.values=values[k];ch.uncompressed=100+k;ch.fully=false;matched::AddChunk(expected,ch);
          if(scope==2) {
            expected.push_back(matched::Num(pages[k].size()));
            for(size_t p=0;p<pages[k].size();++p) {
              for(auto v:{offsets[k]+pages[k][p],p+1<pages[k].size()?offsets[k]+pages[k][p+1]:ch.end,rows[k][p]})
                expected.push_back(matched::Num(v));
            }
          }
        }
      }
      Require(got.values==expected.values,"V38 fixture output");++checks;
    }
  }
  for(auto name:{"leading_zero","bad_order","bad_terminal","bad_page_count","bad_page_order",
                 "bad_page_offset","bad_dictionary","negative_index","bad_length","old_version"}) {
    matched::Data d;d.C=5;d.G=99;d.rows=30;d.base=8192;
    d.artifacts[2][2].bytes=ReadFile(folder+"/fixtures/"+name+".par3");bool rejected=false;
    try {corebench::DirectCore reader(d,2,true);matched::Output out;for(int c=0;c<d.C;++c)reader.Column(c,out);}
    catch(const std::exception&){rejected=true;}
    Require(rejected,"Invalid V38 fixture accepted");++checks;
  }
  const std::vector<std::vector<int64_t>> statrows={{0,5},{11},{22},{0,8,16},{0},{17,24}};
  for(auto fixture:{"stats_rows","stats_pages","stats_pages_rows","stats_missing","stats_arbitrary","empty_stats"}) {
    std::string name=fixture;bool arbitrary=name=="stats_arbitrary",empty=name=="empty_stats",missing=name=="stats_missing";
    int scope=name=="stats_rows" || arbitrary?0:1;listed::Data stats;stats.C=5;stats.G=99;stats.rows=100;stats.base=8192;
    stats.starts[0]=empty?std::vector<int>(6,0):arbitrary?std::vector<int>{0,1,1,3,3,4}:ci;
    stats.starts[1]=empty?std::vector<int>(6,0):std::vector<int>{0,0,4,7,7,10};
    int chunk=0,page=0;
    if(arbitrary)for(auto row:{0,0,50,0}) {
      listed::Range r;r.row=row;r.lo=matched::Num(100+chunk);r.hi=matched::Num(200+chunk++);stats.ranges[0].push_back(r);
    } else if(!empty)for(int k=0;k<6;++k) {
      listed::Range r;r.row=statrows[k][0];
      if(!missing || chunk!=1){r.lo=matched::Num(100+chunk);r.hi=matched::Num(200+chunk);}++chunk;
      stats.ranges[0].push_back(r);
      for(auto row:statrows[k]) {
        listed::Range q;q.row=row;if(!missing || page!=1){q.lo=matched::Num(100+page);q.hi=matched::Num(200+page);}++page;stats.ranges[1].push_back(q);
      }
    }
    stats.artifacts[scope][0].bytes=ReadFile(folder+"/fixtures/"+name+".par3");
    for(int col=-1;col<5;++col){v38stats::Run(stats,scope,0,col,true);++checks;}
  }
  for(auto name:{"missing_starts","bad_start_count","bad_start_null","bad_stats_index"}) {
    listed::Data stats;stats.C=5;stats.G=99;stats.rows=100;stats.base=8192;
    stats.artifacts[1][0].bytes=ReadFile(folder+"/fixtures/"+name+".par3");bool rejected=false;
    try {v38stats::Run(stats,1,0,-1,false);}catch(const std::exception&){rejected=true;}
    Require(rejected,"Invalid V38 statistics accepted");++checks;
  }
  std::cout<<"{\"v38_fixture_checks\":"<<checks<<",\"ragged_empty_shared_dictionary\":true,\"malformed_rejected\":true}\n";
}
namespace comparison {
using namespace matched;
struct Data {matched::Data source;listed::Data stats;matched::Data core;};
const char* Name(int scope,int variant) {
  return variant==0?"current":variant==1?"modular":"v38";
}
uint64_t Core(Data& d,int scope,int variant,int column,bool validate) {
  bool full=column<0;pfor_reader_policy=full?1:2;Output out(validate);
  int lo=full?0:column,hi=full?d.source.C:column+1;
  if(variant>=2) {
    corebench::DirectCore reader(d.core,scope,full);
    for(int c=lo;c<hi;++c)reader.Column(c,out);
    Require(reader.pages.counts.expanded==0,"Compressed core timing artifact");
  } else {
    auto reader=matched::Open(d.source,scope,variant);
    for(int c=lo;c<hi;++c)for(int g=0;g<d.source.G;++g) {
      AddChunk(out,reader->Chunk(c,g));if(scope==2)AddPages(out,reader->Pages(c,g));
    }
  }
  if(validate) {
    Output oracle(true);
    for(int c=lo;c<hi;++c)for(int g=0;g<d.source.G;++g) {
      auto& ch=d.source.chunks[c*d.source.G+g];AddChunk(oracle,ch);if(scope==2)AddPages(oracle,ch.pages);
    }
    Require(out.values==oracle.values,"Core oracle mismatch");
  }
  blackhole=out.hash;return out.hash;
}
uint64_t Stats(Data& d,int scope,int variant,int column,bool validate) {
  if(variant>=2) {
    Counts counts;auto hash=v38stats::Run(d.stats,scope==3,0,column,validate,&counts);
    Require(counts.expanded==0,"Compressed statistics timing artifact");return hash;
  }
  auto reader=matched::Open(d.source,scope,variant);uint64_t hash=1;
  auto value=[&](const Bound& b){hash=hash*31+b.present;hash=hash*31+b.bits;for(uint8_t c:b.bytes)hash=hash*31+c;};
  for(int c=column<0?0:column;c<(column<0?d.source.C:column+1);++c)for(int g=0;g<(scope==3?2:1);++g) {
    auto ranges=reader->Stats(c,g);hash=hash*31+ranges.size();
    if(validate) {
      int a=d.source.range_starts[g][c],b=d.source.range_starts[g][c+1];Require(int(ranges.size())==b-a,"Statistics range count");
      for(int i=0;i<int(ranges.size());++i) {
        const auto& x=ranges[i];const auto& y=d.source.ranges[g][a+i];
        Require(x.row==y.row && x.lo==y.lo && x.hi==y.hi && x.flags==y.flags,"Statistics oracle mismatch");
      }
    }
    for(auto& r:ranges){hash=hash*31+r.row;value(r.lo);value(r.hi);for(int f:r.flags)hash=hash*31+f;}
  }
  blackhole=hash;return hash;
}
struct Case {Data* data;int scope,variant,column,batches=1;std::vector<double> samples;};
uint64_t Run(const Case& c,bool validate=false) {
  return c.scope%2?Stats(*c.data,c.scope,c.variant,c.column,validate):Core(*c.data,c.scope,c.variant,c.column,validate);
}
}
int main(int argc,char** argv) try {
  using namespace comparison;Require(argc==7,"compare harness stats-request core-work validate|time seed positions");
  std::string harness=argv[1],core=argv[3],mode=argv[4];std::mt19937 rng(std::stoi(argv[5]));int positions=std::stoi(argv[6]);
  if(mode=="fixtures"){Fixtures(core);return 0;}
  std::vector<comparison::Data> datasets;
  for(auto suite:{"additional-raincloud","wide-raincloud"}) {
    Input in(harness+"/work/"+suite+"-20261022/request.bin");int n=in.Integer(4);
    for(int i=0;i<n;++i){comparison::Data d;d.source=LoadData(in);datasets.push_back(std::move(d));}
    Require(in.in.get()==-1,"Trailing source request");
  }
  Input in(argv[2]);int n=in.Integer(4);Require(n==int(datasets.size()),"Dataset count");
  for(auto& d:datasets) {
    d.stats=listed::Load(in);auto& s=d.source;auto& t=d.stats;
    Require(s.name==t.name && s.C==t.C && s.G==t.G && s.rows==t.rows && s.base==t.base,"Source dimensions");
    for(int g=0;g<2;++g) {
      Require(s.range_starts[g]==t.starts[g] && s.ranges[g].size()==t.ranges[g].size(),"Source range dimensions");
      for(size_t i=0;i<s.ranges[g].size();++i) {
        auto& x=s.ranges[g][i];auto& y=t.ranges[g][i];
        Require(x.row==y.row && x.lo==y.lo && x.hi==y.hi && x.flags==y.flags,"Source range agreement");
      }
    }
    auto& target=d.core;target.name=s.name;target.C=s.C;target.G=s.G;target.rows=s.rows;target.base=s.base;
    for(int scope:{0,2})target.artifacts[scope][2].bytes=ReadFile(core+"/"+s.name+"."+std::to_string(scope)+".raw.par3");
    for(int scope:{1,3})d.stats.artifacts[scope==3][0].bytes=ReadFile(core+"/"+s.name+"."+std::to_string(scope)+".raw.par3");
  }
  Require(in.in.get()==-1,"Trailing statistics request");std::vector<comparison::Case> cases;
  for(auto& d:datasets)for(int scope=0;scope<4;++scope) {
    auto columns=listed::Positions(d.source.C,positions);columns.push_back(-1);
    for(int column:columns) {
      uint64_t expected=0;
      for(int variant=0;variant<3;++variant) {
        comparison::Case c{&d,scope,variant,column};auto hash=Run(c,true);
        if(variant==0)expected=hash;else Require(hash==expected,"Cross-format output hash mismatch");
        cases.push_back(c);
      }
    }
  }
  if(mode=="validate"){std::cout<<"{\"validated_queries\":"<<cases.size()<<",\"cross_format_hashes\":true}\n";return 0;}
  Require(mode=="time","Mode");
  auto measure=[](comparison::Case& c) {
    auto start=std::chrono::steady_clock::now();for(int i=0;i<c.batches;++i)Run(c);
    return std::chrono::duration<double,std::nano>(std::chrono::steady_clock::now()-start).count()/c.batches;
  };
  std::shuffle(cases.begin(),cases.end(),rng);
  for(auto& c:cases)while(measure(c)*c.batches<2e6 && c.batches<1024)c.batches*=2;
  for(int trial=0;trial<7;++trial) {
    std::shuffle(cases.begin(),cases.end(),rng);for(auto& c:cases)c.samples.push_back(measure(c));
    std::cerr<<"Completed trial "<<trial+1<<"\n";
  }
  for(auto& c:cases) {
    auto samples=c.samples;std::sort(samples.begin(),samples.end());
    std::cout<<"{\"dataset\":\""<<c.data->source.name<<"\",\"scope\":"<<c.scope<<",\"variant\":\""<<Name(c.scope,c.variant)
      <<"\",\"column\":"<<c.column<<",\"median_ns\":"<<samples[3]<<",\"samples_ns\":[";
    for(int i=0;i<7;++i)std::cout<<(i?",":"")<<c.samples[i];std::cout<<"]}\n";
  }
  return 0;
} catch(const std::exception& e){std::cerr<<"ERROR: "<<e.what()<<"\n";return 1;}
