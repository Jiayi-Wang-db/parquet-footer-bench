// Encoding-only controls using the unchanged V38 reader and source oracles.
#include "v38_comparison_support.h"

int main(int argc,char** argv) try {
  using namespace comparison;
  Require(argc==7,"harness stats-request experiment-work validate|time seed positions");
  std::string harness=argv[1],work=argv[3],mode=argv[4];
  std::mt19937 rng(std::stoi(argv[5]));int positions=std::stoi(argv[6]);
  const std::array<std::string,4> policies={"baseline","indexes_frame","offsets_frame","both_frame"};
  std::vector<comparison::Data> sources;
  for(auto suite:{"additional-raincloud","wide-raincloud"}) {
    Input in(harness+"/work/"+suite+"-20261022/request.bin");int n=in.Integer(4);
    for(int i=0;i<n;++i) {comparison::Data d;d.source=LoadData(in);sources.push_back(std::move(d));}
    Require(in.in.get()==-1,"Trailing source request");
  }
  Input in(argv[2]);Require(in.Integer(4)==sources.size(),"Dataset count");
  std::vector<comparison::Data> datasets;datasets.reserve(sources.size()*policies.size());
  for(auto& source:sources) {
    source.stats=listed::Load(in);auto& s=source.source;auto& t=source.stats;
    Require(s.name==t.name && s.C==t.C && s.G==t.G && s.rows==t.rows && s.base==t.base,"Source dimensions");
    for(int g=0;g<2;++g) {
      Require(s.range_starts[g]==t.starts[g] && s.ranges[g].size()==t.ranges[g].size(),"Source ranges");
      for(size_t i=0;i<s.ranges[g].size();++i) {
        auto& x=s.ranges[g][i];auto& y=t.ranges[g][i];
        Require(x.row==y.row && x.lo==y.lo && x.hi==y.hi && x.flags==y.flags,"Source range agreement");
      }
    }
    for(auto& policy:policies) {
      comparison::Data d=source;auto& target=d.core;
      target.name=s.name;target.C=s.C;target.G=s.G;target.rows=s.rows;target.base=s.base;
      for(int scope=0;scope<4;++scope) {
        auto bytes=ReadFile(work+"/"+policy+"/"+s.name+"."+std::to_string(scope)+".raw.par3");
        if(scope%2)d.stats.artifacts[scope==3][0].bytes=std::move(bytes);
        else target.artifacts[scope][2].bytes=std::move(bytes);
      }
      datasets.push_back(std::move(d));
    }
  }
  Require(in.in.get()==-1,"Trailing statistics request");
  struct Trial {comparison::Case query;int policy;};
  std::vector<Trial> cases;
  for(size_t i=0;i<datasets.size();i+=policies.size())for(int scope=0;scope<4;++scope) {
    auto columns=listed::Positions(datasets[i].source.C,positions);columns.push_back(-1);
    for(int column:columns) {
      uint64_t expected=0;
      for(int policy=0;policy<int(policies.size());++policy) {
        comparison::Case c{&datasets[i+policy],scope,2,column};auto hash=Run(c,true);
        if(policy==0)expected=hash;else Require(hash==expected,"Encoding control output mismatch");
        cases.push_back({c,policy});
      }
    }
  }
  if(mode=="validate") {
    std::cout<<"{\"validated_queries\":"<<cases.size()<<",\"cross_encoding_hashes\":true}\n";return 0;
  }
  Require(mode=="time","Mode");
  auto measure=[](Trial& t) {
    auto& c=t.query;Run(c);  // Warm allocation/cache state; each Run still opens a fresh reader.
    auto start=std::chrono::steady_clock::now();
    for(int i=0;i<c.batches;++i)Run(c);
    return std::chrono::duration<double,std::nano>(std::chrono::steady_clock::now()-start).count()/c.batches;
  };
  std::shuffle(cases.begin(),cases.end(),rng);
  for(auto& t:cases)while(t.query.batches<4 || (measure(t)*t.query.batches<2e6 && t.query.batches<1024))t.query.batches*=2;
  for(int trial=0;trial<7;++trial) {
    std::shuffle(cases.begin(),cases.end(),rng);
    for(auto& t:cases)t.query.samples.push_back(measure(t));
    std::cerr<<"Completed trial "<<trial+1<<"\n";
  }
  for(auto& t:cases) {
    auto& c=t.query;auto samples=c.samples;std::sort(samples.begin(),samples.end());
    std::cout<<"{\"dataset\":\""<<c.data->source.name<<"\",\"scope\":"<<c.scope
      <<",\"variant\":\""<<policies[t.policy]<<"\",\"column\":"<<c.column
      <<",\"median_ns\":"<<samples[3]<<",\"samples_ns\":[";
    for(int i=0;i<7;++i)std::cout<<(i?",":"")<<c.samples[i];std::cout<<"]}\n";
  }
  return 0;
} catch(const std::exception& e){std::cerr<<"ERROR: "<<e.what()<<"\n";return 1;}
