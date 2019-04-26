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

namespace leveldb {

Status BuildTable(const std::string& dbname,
                  Env* env,
                  const Options& options,
                  TableCache* table_cache,
                  Iterator* iter,
                  FileMetaData* meta,
                  size_t& lastVtable) {
  Status s;
  meta->file_size = 0;
  iter->SeekToFirst();

  std::string fname = TableFileName(dbname, meta->number);
  std::string vtablename = std::to_string(lastVtable+1)+"t";
  std::string vtablepathname = dbname+"/values/"+vtablename;

  if (iter->Valid()) {
    WritableFile* file;
    FILE* vtable = fopen(vtablepathname.c_str(),"w");
    s = env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      return s;
    }

    TableBuilder* builder = new TableBuilder(options, file);
    VtableBuilder* vtableBuilder = new VtableBuilder(vtable);
    meta->smallest.DecodeFrom(iter->key());
    for (; iter->Valid(); iter->Next()) {
      Slice key = iter->key();
      meta->largest.DecodeFrom(key);
      size_t valueSize = iter->value().size();
      // small value?
      if(valueSize<=options.exp_ops.smallThreshold){
        builder->Add(key,iter->value());
      } else {
        Slice userKey = ExtractUserKey(key);
        Slice value = iter->value();
        size_t offset = vtableBuilder->Add(userKey, value);
        // filename $ offset $ value size
        std::string valueInfo = vtablename +"$"+std::to_string(offset)+"$"+std::to_string(value.size());
        builder->Add(key, valueInfo);
        // finish this vtable
        if(offset>options.exp_ops.tableSize){
          vtableBuilder->Finish();
          lastVtable+=1;
          vtablename = std::to_string(lastVtable+1)+"t";
          vtablepathname = dbname+"/values/"+vtablename;
          vtableBuilder->NextFile(fopen(vtablepathname.c_str(),"w"));
        }
      }
    }

    // Finish and check for builder errors
    if(!vtableBuilder->Done()){
      vtableBuilder->Finish();
      lastVtable+=1;
    }
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
