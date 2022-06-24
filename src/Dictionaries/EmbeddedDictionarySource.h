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


namespace DB
{

    

template<typename Lookup>
class EmbeddedSource final : public SourceWithProgress{
public:

    EmbeddedSource(
        ContextPtr context_,
        const Block & sample_block_,
        const std::vector<UInt64> & ids, // const std::vector<std::string> & keys_,
        Lookup lookup_);

    ~EmbeddedSource() override = default;

protected:

    Chunk generate() override;

    String getName() const override { return "Embedded"; }

private:
    size_t cursor = 0;
    bool all_read = false;

    ExternalResultDescription description;

    ContextPtr context;
    Block sample_block;
    std::vector<UInt64> keys;
    Lookup lookup;
};

class Lookup {
public:
  explicit Lookup(std::string path_, size_t max_block_size_)
    : path(path_)
    , max_block_size(max_block_size_) {}

  Lookup(const Lookup & other) = default;

  std::string getPath() {
    return path;
  }

  size_t maxBlockSize() const {
      return max_block_size;
  }

  virtual std::unique_ptr<DB::ReadBuffer> lookup(std::string & key) = 0;

  virtual ~Lookup() = default;
private:
  std::string path;
  size_t max_block_size;
};

#if USE_LMDB
#include <lmdb.h>
class LookupLMDB : public Lookup {
public:
  LookupLMDB(std::string path, UInt64 mapsize, std::string dbname);
  ~LookupLMDB() override = default;

  LookupLMDB(const LookupLMDB & other);

  std::unique_ptr<DB::ReadBuffer> lookup(std::string & key) override;

private:
  MDB_env *env;
  MDB_dbi dbi;
  MDB_txn * txn;
};
#endif


/** EmbeddedDictionarySource allows loading data from an embedded dictionary on disk.
  * TODO document thoroughly
  */
template <typename Lookup>
class EmbeddedDictionarySource final : public IDictionarySource
{
public:
    
    EmbeddedDictionarySource(
        const DictionaryStructure & dict_struct_,
        Lookup lookup_,
        Block & sample_block_,
        ContextPtr context_,
        bool created_from_ddl);

    EmbeddedDictionarySource(const EmbeddedDictionarySource & other);
    EmbeddedDictionarySource & operator=(const EmbeddedDictionarySource &) = delete;

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
    Lookup lookup;

    Block sample_block;
    ContextPtr context;
    Poco::Logger * log;
};

}
