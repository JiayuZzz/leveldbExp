// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include "db/filename.h"
#include "db/dbformat.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "leveldb/vtable_builder.h"
#include "funcs.h"

namespace leveldb {

Status BuildTable(const std::string& dbname,
                  Env* env,
                  const Options& options,
                  TableCache* table_cache,
                  Iterator* iter,
                  FileMetaData* meta,
                  std::atomic<size_t>& lastVtable, std::unordered_map<std::string, VfileMeta>& metaTable) {
  Status s;
  meta->file_size = 0;
  iter->SeekToFirst();

  std::string prefix(1,'a'+options.exp_ops.mergeLevel);

  std::string fname = TableFileName(dbname, meta->number);
  std::string vtablename;
  std::string vtablepathname;

  if (iter->Valid()) {
    WritableFile* file;
    s = env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      return s;
    }

    TableBuilder* builder = new TableBuilder(options, file);
    VtableBuilder* vtableBuilder = new VtableBuilder();
    FILE* f = nullptr;
    meta->smallest.DecodeFrom(iter->key());
    std::string start_key, end_key;
    for (; iter->Valid(); iter->Next()) {
      Slice key = iter->key();
      meta->largest.DecodeFrom(key);
      std::string value = iter->value().ToString();
      if(value[0]!='m'||value.back()!='~'){
//        std::cerr<<"small or large, size:"<<value.size()<<std::endl;
        builder->Add(key,iter->value());
      } else {                             // write value to vtable, write location to lsm-tree
      Slice userKey = ExtractUserKey(key);
      end_key = userKey.ToString();
	    if(vtableBuilder->Finished()){
	      vtablename = conbineStr({prefix,std::to_string(++lastVtable)});
	      vtablepathname = conbineStr({dbname,"/values/",vtablename});
	      vtableBuilder->NextFile(vtablepathname);
        start_key = userKey.ToString();
	    }
	    std::string filename;
	    size_t offset, size;
	    parseValueInfo(value,filename,offset,size);
	    if(!f) {
	        f = fopen(conbineStr({dbname,"/values/",filename}).c_str(),"r");
	        if(!f) continue;
	    }
	    char realvalue[size+1];
	    // extract value from midlog
        uint64_t startRead = NowMicros();
	    pread(fileno(f),realvalue,size,offset);
	    realvalue[size]='\0';
          STATS::Time(STATS::GetInstance()->readValueFile,startRead,NowMicros());
        offset = vtableBuilder->Add(userKey, Slice(realvalue));
        // filename $ offset $ value size
        std::string valueInfo = conbineValueInfo(vtablename,offset,size);
        builder->Add(key, valueInfo);
        // finish this vtable
        if(offset>options.exp_ops.tableSize){
          int cnt = vtableBuilder->Finish();
          metaTable[vtablename] = VfileMeta(cnt, start_key, end_key);
        }
      }
    }

    // Finish and check for builder errors
    if(f) fclose(f);
    if(!vtableBuilder->Finished()){
      int cnt = vtableBuilder->Finish();
      metaTable[vtablename] = VfileMeta(cnt, start_key, end_key);
    }
    vtableBuilder->Sync();
    s = builder->Finish();
    if (s.ok()) {
      meta->file_size = builder->FileSize();
      assert(meta->file_size > 0);
    }
    delete builder;
    delete vtableBuilder;

    // Finish and check for file errors
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
    delete file;
    file = nullptr;

    if (s.ok()) {
      // Verify that the table is usable
      Iterator* it = table_cache->NewIterator(ReadOptions(),
                                              meta->number,
                                              meta->file_size);
      s = it->status();
      delete it;
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (s.ok() && meta->file_size > 0) {
    // Keep it
  } else {
    env->DeleteFile(fname);
  }
    return s;
}

}  // namespace leveldb
