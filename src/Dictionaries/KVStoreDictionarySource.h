#pragma once

#include <base/logger_useful.h>
#include <Core/Block.h>
#include <Interpreters/Context.h>
#include <Dictionaries/IDictionarySource.h>
#include <Dictionaries/DictionaryStructure.h>
#include "Processors/Sources/SourceWithProgress.h"

#if USE_LMDB
#include <lmdb.h>
#endif

namespace DB
{

/** KVStoreSource performs lookups in the store for a number of keys
  * It is generic over `KVStore`, which allows various key-value storage backends to be plugged in.
  */
template<typename KVStore>
class KVStoreSource final : public SourceWithProgress{
public:

    KVStoreSource(
        const Block & sample_block_,
        const std::vector<std::string> & keys,
        KVStore store_);

    ~KVStoreSource() override = default;

protected:

    Chunk generate() override;

    String getName() const override { return "KVStore"; }

private:
    size_t cursor = 0;
    bool all_read = false;

    Block sample_block;
    std::vector<std::string> keys;
    KVStore store;
};

/** KVStore is superclass for key-value store providers
  */
class KVStore {
public:
  explicit KVStore(size_t max_block_size_)
    : max_block_size(max_block_size_) {}

  KVStore(const KVStore & other) = default;

  size_t maxBlockSize() const {
      return max_block_size;
  }

  // lookup should make a lookup to the backend. Return null if the entry isn't found
  virtual std::unique_ptr<DB::ReadBuffer> lookup(std::string & key) = 0;

  virtual ~KVStore() = default;
private:
  size_t max_block_size;
};

#if USE_LMDB
/** KVStoreLMDB is a provider for KVStoreSource which pulls from LMDB.
  * Records must be stored in a CSV format. Fields encoded including the key, in the order defined in the structure.
  * They should be indexed using keys in their string format.
  * We search in the database `dbname`.
  */
class KVStoreLMDB : public KVStore {
public:
  KVStoreLMDB(std::string path, UInt64 mapsize, std::string dbname);
  ~KVStoreLMDB() override = default;

  KVStoreLMDB(const KVStoreLMDB & other); // TODO should we have copy constructor?

  std::unique_ptr<DB::ReadBuffer> lookup(std::string & key) override;
private:
  std::string path;
  MDB_env *env;
  MDB_dbi dbi;
  MDB_txn * txn;
};
#endif


/** KVStoreDictionarySource allows loading data from a key-value store
  * It is generic over various different storage providers, such as LMDB
  */
template <typename KVStore>
class KVStoreDictionarySource final : public IDictionarySource
{
public:
    
    KVStoreDictionarySource(
        const DictionaryStructure & dict_struct_,
        KVStore store_,
        Block & sample_block_);

    KVStoreDictionarySource(const KVStoreDictionarySource & other);
    KVStoreDictionarySource & operator=(const KVStoreDictionarySource &) = delete;

    // loadAll isn't supported for this dictionary source
    Pipe loadAll() override;

    // loadUpdatedAll isn't supported for this dictionary source
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

private:
    KVStore store;
    const DictionaryStructure dict_struct;
    Block sample_block;
    Poco::Logger * log;
};

}
