#include "KVStoreDictionarySource.h"

#include <base/logger_useful.h>
#include <Common/filesystemHelpers.h>
#include <Common/FieldVisitorToString.h>
#include <IO/EmptyReadBuffer.h>
#include "IO/ReadBuffer.h"
#include "Processors/Executors/StreamingFormatExecutor.h"
#include "Processors/Formats/IRowInputFormat.h"
#include "Processors/Formats/Impl/CSVRowInputFormat.h"
#include <Interpreters/Context.h>
#include <Dictionaries/DictionarySourceFactory.h>
#include <Dictionaries/DictionarySourceHelpers.h>
#include <Dictionaries/DictionaryStructure.h>

#if USE_LMDB
#include <lmdb.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNSUPPORTED_METHOD;
    extern const int PATH_ACCESS_DENIED;
    extern const int LMDB_ERROR;
}

#if USE_LMDB
const int LMDB_MAX_BLOCK_SIZE = DEFAULT_BLOCK_SIZE;
#endif

template<typename KVStore> KVStoreSource<KVStore>::KVStoreSource(
    const Block & sample_block_,
    const std::vector<String> & keys_,
    KVStore store_)
    : SourceWithProgress(sample_block_)
    , sample_block(sample_block_)
    , keys(keys_)
    , store(store_)
{}

template<typename KVStore> Chunk KVStoreSource<KVStore>::generate()
{
    if (keys.empty() || sample_block.rows() == 0 || cursor >= keys.size())
        all_read = true;

    if (all_read)
        return {};

    EmptyReadBuffer empty;
    const FormatSettings format_settings; // TODO idk if we should let people supply this in the config?? Probably not necessary
    const RowInputFormatParams params{store.maxBlockSize()};
    StreamingFormatExecutor executor(sample_block, std::make_shared<CSVRowInputFormat>(sample_block, empty, params, false, false, format_settings));
    
    size_t final_idx = cursor + std::min(store.maxBlockSize(), keys.size() - cursor);
    for (; cursor < final_idx; ++cursor)
    {
        std::unique_ptr<ReadBuffer> s = store.lookup(keys[cursor]);
        if (!s)
            continue; // The key wasn't found
        executor.execute(*s);
    }

    auto cols = executor.getResultColumns();
    size_t num_rows = cols.at(0)->size();

    return Chunk(std::move(cols), num_rows);
}

#if USE_LMDB
KVStoreLMDB::KVStoreLMDB(std::string path_, size_t mapsize, std::string dbname)
    : KVStore(LMDB_MAX_BLOCK_SIZE)
    , path(path_)
{
    // TODO include some context with these exceptions: dbname, mapsize, path etc.

    if (int res = mdb_env_create(&env)) {
        throw Exception(ErrorCodes::LMDB_ERROR, "LMDB Error creating env: {}", res);
    }

    // We only open a single database for our dictionary.
    if (int res = mdb_env_set_maxdbs(env, 1)) {
        throw Exception(ErrorCodes::LMDB_ERROR, "LMDB error setting maxdbs for env: {}", res);
    }

    if (int res = mdb_env_set_mapsize(env, mapsize)) {
        throw Exception(ErrorCodes::LMDB_ERROR, "LMDB error setting mapsize for env: {}", res);
    }

    if (int res = mdb_env_open(env, path.c_str(), MDB_NOTLS | MDB_RDONLY, 0664)) {
        throw Exception(ErrorCodes::LMDB_ERROR, "LMDB Failed to open env: {}", res);
    }

    if (auto res = mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn)) {
        throw Exception(ErrorCodes::LMDB_ERROR, "LMDB Failed to begin transaction: {}", res);
    }

    if (auto res = mdb_dbi_open(txn, dbname.c_str(), 0, &dbi)) {
        throw Exception(ErrorCodes::LMDB_ERROR, "LMDB Failed to open dbi: {}", res);
    }
}

// TODO verify it's ok to copy lmdb objects
KVStoreLMDB::KVStoreLMDB(const KVStoreLMDB & other) = default;
// KVStoreLMDB::KVStoreLMDB(KVStoreLMDB && other) noexcept = default;

std::unique_ptr<DB::ReadBuffer> KVStoreLMDB::lookup(std::string & key) {
    MDB_val key_val = {
        static_cast<size_t>(key.size()),
        static_cast<void *>(key.data()),
    };

    MDB_val found = {};

    int ret = mdb_get(txn, dbi, &key_val, &found);
    if (ret == MDB_NOTFOUND) {
        return nullptr;
    } else if (ret != 0) {
        throw Exception(ErrorCodes::LMDB_ERROR, "LMDB Lookup failure: {}", ret);
    }

    return std::make_unique<ReadBufferFromMemory>(static_cast<const char*>(found.mv_data), found.mv_size);
}
#endif

template<typename KVStore> KVStoreDictionarySource<KVStore>::KVStoreDictionarySource(
    const DictionaryStructure & dict_struct_,
    KVStore store_,
    Block & sample_block_)
    : store(store_)
    , dict_struct(dict_struct_)
    , sample_block(sample_block_)
    , log(&Poco::Logger::get("KVStoreDictionarySource"))
{}

// TODO default copy constructor possible?
template<typename KVStore> KVStoreDictionarySource<KVStore>::KVStoreDictionarySource(const KVStoreDictionarySource & other)
    : store(other.store)
    , dict_struct(other.dict_struct)
    , sample_block(other.sample_block)
    , log(&Poco::Logger::get("KVStoreDictionarySource"))
{
}

template<typename KVStore> Pipe KVStoreDictionarySource<KVStore>::loadAll()
{
    // This could probably be supported -- most KV stores support iteration. But it doesn't seem like it would be that useful
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "KVStoreDictionarySource does not support loadAll method");
}

template<typename KVStore> Pipe KVStoreDictionarySource<KVStore>::loadUpdatedAll()
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "KVStoreDictionarySource does not support loadUpdatedAll method");
}

template<typename KVStore> Pipe KVStoreDictionarySource<KVStore>::loadIds(const std::vector<UInt64> & ids)
{
    LOG_TRACE(log, "loadIds {} size = {}", toString(), ids.size());

    // TODO maybe figure out if we can preallocate this or just generally be smarter
    // Maybe instead of having a fully-realised vector of keys, you provide an iterator, allowing conversion on the fly?
    std::vector<std::string> keys;
    std::transform(std::begin(ids),
                   std::end(ids),
                   std::back_inserter(keys),
                   [](UInt64 id){ return std::to_string(id); });

    return Pipe(std::make_shared<KVStoreSource<KVStore>>(
            sample_block,
            keys,
            store));
}


template<typename KVStore> Pipe KVStoreDictionarySource<KVStore>::loadKeys(const Columns &  key_columns, const std::vector<size_t> &  requested_rows)
{
    if (key_columns.size() != dict_struct.key->size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to lookup with the wrong number of key columns. Got: ", key_columns.size(), ", expected: ", dict_struct.key->size());

    std::vector<std::string> keys;
    for (auto row : requested_rows)
    {
        auto rowvalue = (*key_columns[0])[row];
        std::string k = Field::dispatch(FieldVisitorToString(), rowvalue);
        keys.emplace_back(k);
    }

    return Pipe(std::make_shared<KVStoreSource<KVStore>>(
            sample_block,
            keys,
            store));
}

template<typename KVStore> bool KVStoreDictionarySource<KVStore>::isModified() const
{
    return true;
}

template<typename KVStore> bool KVStoreDictionarySource<KVStore>::hasUpdateField() const
{
    return false;
}

template<typename KVStore> DictionarySourcePtr KVStoreDictionarySource<KVStore>::clone() const
{
    return std::make_shared<KVStoreDictionarySource>(*this);
}

#if USE_LMDB
template<> std::string KVStoreDictionarySource<KVStoreLMDB>::toString() const
{
    // TODO figure out what to put in this
    return "KVStore dictionary, provider=LMDB";
}
#endif

void registerDictionarySourceKVStore(DictionarySourceFactory & factory)
{
    auto create_table_source = [=](const DictionaryStructure & dict_struct,
                                 const Poco::Util::AbstractConfiguration & config,
                                 const std::string & config_prefix,
                                 Block & sample_block,
                                 ContextPtr global_context,
                                 const std::string & /* default_database */,
                                 bool created_from_ddl) -> DictionarySourcePtr
    {
        if (dict_struct.has_expressions)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Dictionary source of type `KVStore` does not support attribute expressions");

        if (dict_struct.key && dict_struct.key->size() > 1)
            // TODO you could enable this by encoding keys in CSV format or something. We don't have this requirement though, so not implementing currently
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Dictionary source of type `KVStore` only supports complex keys with a single attribute");

        ContextMutablePtr context = copyContextAndApplySettingsFromDictionaryConfig(global_context, config, config_prefix);

        String settings_config_prefix = config_prefix + ".KVStore";

        const std::string store = config.getString(settings_config_prefix + ".store");
        
        if (store == "LMDB") {
            #if USE_LMDB
            const std::string path = config.getString(settings_config_prefix + ".path");
            size_t mapsize = 32 * 1024 * 1024 * 1024l; // Default 32 GiB
            if (config.has(settings_config_prefix + ".mapsize"))
                mapsize = config.getInt64(settings_config_prefix + ".mapsize");
            
            // Verify that if this was created with DDL, the path is safe
            auto user_files_path = context->getUserFilesPath();
            if (created_from_ddl && !fileOrSymlinkPathStartsWith(path, user_files_path))
                throw Exception(ErrorCodes::PATH_ACCESS_DENIED, "File path {} is not inside {}", path, user_files_path);
            
            KVStoreLMDB kvstore (
                path,
                mapsize,
                config.getString(settings_config_prefix + ".dbname")
            );
            return std::make_unique<KVStoreDictionarySource<KVStoreLMDB>>(dict_struct, kvstore, sample_block);
            #else
            throw Exception(ErrorCodes::FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME, "LMDB not compiled into this instance of ClickHouse, can't use LMDB KVStore dictionary");
            #endif
        } else {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "{}: Unknown provider.", store);
        }
    };

    factory.registerSource("KVStore", create_table_source);
}

}
