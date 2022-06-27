#pragma once

#include <base/logger_useful.h>


#include <Core/Block.h>
#include <Core/ExternalResultDescription.h>
#include <Interpreters/Context.h>

#include <Columns/ColumnNullable.h>
#include <Dictionaries/IDictionarySource.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Formats/FormatFactory.h>
#include "Processors/Sources/SourceWithProgress.h"
#include "base/types.h"

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#if USE_LMDB
#include <lmdb.h>
#endif

namespace DB
{

    

template<typename KVStore>
class KVStoreSource final : public SourceWithProgress{
public:

    KVStoreSource(
        ContextPtr context_,
        const Block & sample_block_,
        const std::vector<UInt64> & ids, // const std::vector<std::string> & keys_,
        KVStore store_);

    ~KVStoreSource() override = default;

protected:

    Chunk generate() override;

    String getName() const override { return "KVStore"; }

private:
    size_t cursor = 0;
    bool all_read = false;

    ExternalResultDescription description;

    ContextPtr context;
    Block sample_block;
    std::vector<UInt64> keys;
    KVStore store;
};

class KVStore {
public:
  explicit KVStore(std::string path_, size_t max_block_size_)
    : path(path_)
    , max_block_size(max_block_size_) {}

  KVStore(const KVStore & other) = default;

  std::string getPath() {
    return path;
  }

  size_t maxBlockSize() const {
      return max_block_size;
  }

  virtual std::unique_ptr<DB::ReadBuffer> lookup(std::string & key) = 0;

  virtual ~KVStore() = default;
private:
  std::string path;
  size_t max_block_size;
};

#if USE_LMDB
class KVStoreLMDB : public KVStore {
public:
  KVStoreLMDB(std::string path, UInt64 mapsize, std::string dbname);
  ~KVStoreLMDB() override = default;

  KVStoreLMDB(const KVStoreLMDB & other); // TODO should we have copy constructor?

  std::unique_ptr<DB::ReadBuffer> lookup(std::string & key) override;
private:
  MDB_env *env;
  MDB_dbi dbi;
  MDB_txn * txn;
};
#endif


/** KVStoreDictionarySource allows loading data from an KVStore dictionary on disk.
  * TODO document thoroughly
  */
template <typename KVStore>
class KVStoreDictionarySource final : public IDictionarySource
{
public:
    
    KVStoreDictionarySource(
        const DictionaryStructure & dict_struct_,
        KVStore store_,
        Block & sample_block_,
        ContextPtr context_,
        bool created_from_ddl);

    KVStoreDictionarySource(const KVStoreDictionarySource & other);
    KVStoreDictionarySource & operator=(const KVStoreDictionarySource &) = delete;

    Pipe loadAll() override;

    /** The logic of this method is flawed, absolutely incorrect and ignorant.
      * It may lead to skipping some values due to clock sync or timezone changes.
      * The intended usage of "update_field" is totally different.
      */
    Pipe loadUpdatedAll() override;

    Pipe loadIds(const std::vector<UInt64> & ids) override;

    Pipe loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows) override;

    bool isModified() const override;

    bool supportsSelectiveLoad() const override {
      return true;
    }

    bool hasUpdateField() const override;

    DictionarySourcePtr clone() const override;

    std::string toString() const override;

    Pipe getStreamForBlock(const Block & block);

private:
    const DictionaryStructure dict_struct;
    KVStore store;

    Block sample_block;
    ContextPtr context;
    Poco::Logger * log;
};

}
