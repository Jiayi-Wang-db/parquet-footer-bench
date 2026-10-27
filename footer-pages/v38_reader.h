// V38 benchmark reader: direct extents, dictionary IDs, and cumulative-end indexes.
// Scalar decoders and Compact-Thrift parsing come from the frozen comparison harness.
#include <optional>
namespace v38 {
using namespace paired_bounds;
struct Hierarchy {
  PageReader& pages; int C,K,P=-1,D;
  int Count(int id,bool nullable=false) {
    auto& s=pages.streams.at(id);pages.Open(s);int64_t count=0;
    for(auto& p:s.directory) {
      Require(p.rl==0 && (nullable || (p.dl==0 && p.nulls==0)),"V38 flat stream shape");
      count+=p.rows;Require(count<=INT32_MAX,"V38 stream too large");
    }
    return count;
  }
  int At(int id,int index) {
    Require(index>=0,"Negative index position");auto& s=pages.streams.at(id);pages.Open(s);
    auto it=std::lower_bound(s.directory.begin(),s.directory.end(),index,
        [](const HeaderInfo& p,int i){return p.end()<=i;});
    Require(it!=s.directory.end() && index>=it->first,"Index extent");
    auto& q=pages.Load(s,it-s.directory.begin(),1);int rank=q.shape->ranks[index-it->first];
    Require(rank>=0,"Null index");if(!pages.bulk)q.values->Prepare(rank,rank+1);
    auto b=q.values->Get(rank,1);Require(b.present && b.bits<=INT32_MAX,"Index value");return b.bits;
  }
  int Boundary(int id,int n) {
    Require(n>=0 && n<=(id==301?C:K),"Boundary position");return n==0?0:At(id,n-1);
  }
  Hierarchy(PageReader& p,int columns):pages(p),C(columns) {
    Require(C>=0,"Negative column count");
    for(auto& [id,s]:pages.streams)s.max_definition=1;
    Require(Count(301)==C,"V38 column-index count");K=Boundary(301,C);
    for(int id:{601,602,603,605,607,608})Require(Count(id,id==602)==K,"Chunk field count");
    for(int id:{604,606})if(pages.streams.count(id))Require(Count(id)==K,"Optional chunk field count");
    D=Count(401);Require(Count(402)==D,"Dictionary table count");
    if(pages.streams.count(302)){Require(Count(302)==K,"V38 page-index count");P=Boundary(302,K);}
    for(int id:{403,701})if(pages.streams.count(id))Require(P>=0 && Count(id)==P,"Page field count");
  }
  std::pair<int,int> Chunks(int c) {
    Require(c>=0 && c<C,"Column ordinal");int a=Boundary(301,c),b=At(301,c);
    Require(a<=b && b<=K,"Chunk index order");return {a,b};
  }
  std::pair<int,int> Pages(int c,bool tagged=false) {
    auto [a,b]=Chunks(c);Require(P>=0,"Missing page index");
    int lo=Boundary(302,a),hi=Boundary(302,b);
    Require(lo<=hi && hi<=P,"Page index order");return {lo,hi};
  }
};
}
namespace corebench {
struct DirectCore : FastCore {
  v38::Hierarchy hierarchy;
  DirectCore(matched::Data& d,int scope,bool full):FastCore(d,scope,full),hierarchy(pages,d.C) {
    Require(version==38,"V38 core version");
  }
  static int64_t Number(Bound b) {
    Require(b.present && b.bits<=INT64_MAX,"Missing or negative location");return b.bits;
  }
  void Column(int c,matched::Output& out) {
    auto [lo,hi]=hierarchy.Chunks(c);std::vector<matched::ChunkInfo> chunks(hi-lo);
    Read(607,2,lo,hi,[&](int i,Bound b){chunks[i].data=Number(b);});
    Read(608,2,lo,hi,[&](int i,Bound b){
      int64_t n=Number(b);Require(chunks[i].data<=d.base && n<=d.base-chunks[i].data,"Data extent");
      chunks[i].end=chunks[i].data+n;
    });
    Read(602,1,lo,hi,[&](int i,Bound b){
      Require(!b.present || b.bits<uint64_t(hierarchy.D),"Dictionary ID");
      chunks[i].dict=b.present?At(401,2,b.bits):-1;
      Require(!b.present || (chunks[i].dict>=0 && chunks[i].dict<d.base),"Dictionary offset");
    });
    Read(601,1,lo,hi,[&](int i,Bound b){Require(b.present,"Codec");chunks[i].codec=b.bits;});
    Read(605,2,lo,hi,[&](int i,Bound b){chunks[i].values=Number(b);});
    // The matched projection requests uncompressed sizes, present in this corpus.
    Read(604,2,lo,hi,[&](int i,Bound b){chunks[i].uncompressed=Number(b);});
    Read(603,0,lo,hi,[&](int i,Bound b){Require(b.present && b.bits<=1,"Dictionary flag");chunks[i].fully=b.bits;});
    std::vector<int> starts;std::vector<layouts::DataPage> ps;
    if(scenario==2) {
      Require(hierarchy.P>=0,"Missing page index");starts.resize(hi-lo+1);
      starts[0]=hierarchy.Boundary(302,lo);
      Read(302,1,lo,hi,[&](int i,Bound b){Require(b.present && b.bits<=INT32_MAX,"Page end");starts[i+1]=b.bits;});
      Require(std::is_sorted(starts.begin(),starts.end()) && starts.back()<=hierarchy.P,"Page index order");
      ps.resize(starts.back()-starts.front());
      Read(403,2,starts.front(),starts.back(),[&](int i,Bound b){ps[i].offset=Number(b);});
      Read(701,2,starts.front(),starts.back(),[&](int i,Bound b){ps[i].first=Number(b);});
      int first=starts.front();for(int& start:starts)start-=first;
      for(int k=0;k<hi-lo;++k) {
        int a=starts[k],b=starts[k+1];int64_t length=chunks[k].end-chunks[k].data;
        Require((a==b)==(length==0),"Empty chunk extent");
        for(int i=a;i<b;++i) {
          Require(ps[i].offset<length && (i==a?ps[i].offset==0:ps[i-1].offset<ps[i].offset),"Relative page order");
        }
        for(int i=a;i<b;++i)ps[i].offset+=chunks[k].data;
        for(int i=a;i<b;++i)ps[i].end=i+1<b?ps[i+1].offset:chunks[k].end;
      }
    }
    for(int k=0;k<hi-lo;++k) {
      matched::AddChunk(out,chunks[k]);
      if(scenario==2) {
        out.push_back(matched::Num(starts[k+1]-starts[k]));
        for(int i=starts[k];i<starts[k+1];++i)for(auto n:{ps[i].offset,ps[i].end,ps[i].first})out.push_back(matched::Num(n));
      }
    }
  }
};
}
