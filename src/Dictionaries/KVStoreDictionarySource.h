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
    /** KVStoreSource performs lookups in the store for a number of keys
      * It is generic over `KVStore`, which allows various key-value storage backends to be plugged in.
      * `sample_block` should have a structure similar to the structure of the dictionary.
      * `keys` in this case is the set of keys to be looked up from our KVStore. TODO consider making this iterator instead of vector
      * `store_` is the already initialised storage backend for this source. All lookups will be performed against this.
      */
    KVStoreSource(
        const Block & sample_block_,
        const std::vector<std::string> & keys,
        KVStore store_);

    /** No special destructor is required for the source itself.
      */
    ~KVStoreSource() override = default;

protected:
    /** generate will do a block of lookups and return the result in a chunk.
      * The size of the chunk is restricted by `store.maxBlockSize()`.
      * Since the lookups return a row encoded as CSV, we will parse this CSV and append the values to each column respectively.
      */
    Chunk generate() override;

    /** getName returns a string indicating the name of the source.
      * This doesn't include information about the specific backend being used though
      * TODO maybe we should include backend info?? e.g. ""KVStore<LMDB>" or something?
      */
    String getName() const override { return "KVStore"; }

private:
    size_t cursor = 0; // our index into `keys`
    bool all_read = false; // true when cursor == keys.size()-1

    Block sample_block; // Gives us the shape of the chunks we should generate
    std::vector<std::string> keys; // The keys to be looked up. TODO consider using an iterator instead
    KVStore store; // The actual backend which performs lookups
};

/** KVStore is superclass for key-value store providers
  */
class KVStore {
public:
  /** KVStore is intended to contain any variables which will be common to *all* potential storage backends.
    * Currently, this is only one variable, hence marking this constructor explicit.
    * `max_block_size_` is the max size of block allowed for this backend. This variable is used in the KVStoreSource to set max size of chunks.
    */
  explicit KVStore(size_t max_block_size_)
    : max_block_size(max_block_size_) {}

  // TODO should copying be legal????
  KVStore(const KVStore & other) = default;

  /** maxBlockSize will return the maximum size of block to be generated when making lookups against this source.
    */
  size_t maxBlockSize() const {
      return max_block_size;
  }

  /** lookup performs a lookup against the KV store.
    * `key` is the key to be looked up. Remember that for KVStore source, the keys are always in their string format.
    * Values should be encoded as CSV with a structure matching the dictionary structure. Including the key/id
    * You should push this CSV string into a readbuffer and return it
    * If the value doesn't exist, return a nullptr instead of a readbuffer
    * If the lookup fails for some unexpected reason, throw an exception
    */
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
  * The LMDB  instance should have a single database.

  TODO we want to be able to make queries simultaneously in the background for live updates?? Needs some thought
  */
class KVStoreLMDB : public KVStore {
public:
  /** KVStoreLMDB
    * `path` is the location at which the database exists. This must have *already* been verified as a safe location. We don't check within
    * `mapsize` indicates how large the database could possibly be. This is an upper bound on the potential size of the memory mapping. Must be > database size
    * `dbname` is the name within LMDB of the database containing our key value pairs. This database shouldn't contain any other data, as we may in future support loading of all KV pairs.
    */
  KVStoreLMDB(std::string path, UInt64 mapsize, std::string dbname);

  // TODO need to cleanup LMDB stuff properly??
  ~KVStoreLMDB() override = default;

  KVStoreLMDB(const KVStoreLMDB & other); // = delete; // TODO should we have copy constructor?
  KVStoreLMDB & operator=(const KVStoreLMDB &) = delete;

  /** lookup will perform the lookup against the database
    * All lookups occur within the same LMDB transaction
    TODO that may need to change if we're allowing live updates to the database
    */
  std::unique_ptr<DB::ReadBuffer> lookup(std::string & key) override;
private:
  std::string path;
  MDB_env *env;
  MDB_dbi dbi;
  MDB_txn * txn;
};
#endif


/** KVStoreDictionarySource is a dictionary source that pulls data from a key value storage.
  * It is generic over various different storage providers, such as LMDB
  * Within the KV store, the pairs must be encoded as such:
  * - key/id: a single scalar value (eg int or string, but not (int,string)) encoded as a string
  * - value: a CSV encoding of the row. Must have same structure as described in the structure for this dictionary.
  */
template <typename KVStore>
class KVStoreDictionarySource final : public IDictionarySource
{
public:
    /** KVStoreDictionarySource is a dictionary source that pulls data from a key value storage.
      * `dict_struct_` is the structure -- we verify it has a single scalar as the key
      * `store_` is the storage backend to use
      * `sample_block_` gives us the structure of rows to be returned when lookup occurs.
      */
    KVStoreDictionarySource(
        const DictionaryStructure & dict_struct_,
        KVStore store_,
        Block & sample_block_);

    // TODO think carefully about all these copy constructors
    KVStoreDictionarySource(const KVStoreDictionarySource & other);

    // TODO why do we delete this again? IIRC I saw somewhere else there's things not safe for copying, but we still have copy constructor?
    KVStoreDictionarySource & operator=(const KVStoreDictionarySource &) = delete;

    // loadAll isn't supported for this dictionary source. It could be in future
    Pipe loadAll() override;

    // loadUpdatedAll isn't supported for this dictionary source
    Pipe loadUpdatedAll() override;

    /** loadIds will transform the ids into their string representation, then perform lookups of these keys in chunks
      * `ids` the ids we're going to lookup
      */
    Pipe loadIds(const std::vector<UInt64> & ids) override;

    /** loadKeys will transform the keys into their string representation, then perform lookups of these keys in chunks
      * `key_columns` the key columns. Remember, we currently only support scalar keys, so this should have a single column
      * `requested_rows` the rows being looked for. Each entry is an index indicating which row in `key_columns` is the key that we wish to lookup.
      */
    Pipe loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows) override;

    // TODO we should think about this in the context of background updates
    bool isModified() const override;

    /** supportsSelectiveLoad must always be true for any key value storage... it wouldn't be a very good KVStore if it didn't
      */
    bool supportsSelectiveLoad() const override {
      return true;
    }

    // TODO think about in the context of background updates
    bool hasUpdateField() const override;

    // TODO think about this.. is it safe?
    DictionarySourcePtr clone() const override;

    /** toString should be implemented for each possible backend, reporting a value as such: "KVStore,backend=$BACKEND"
      */
    std::string toString() const override;

private:
    KVStore store;
    const DictionaryStructure dict_struct;
    Block sample_block;
    Poco::Logger * log;
};

}
