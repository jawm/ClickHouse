#include "EmbeddedDictionarySource.h"

#include <filesystem>
#include <optional>
#include <lmdb.h>

#include <boost/algorithm/string/split.hpp>

#include <base/logger_useful.h>
#include <Common/LocalDateTime.h>
#include <Common/filesystemHelpers.h>
#include <Common/ShellCommand.h>
#include <IO/EmptyReadBuffer.h>
#include "IO/ReadBuffer.h"
#include "Processors/Executors/PipelineExecutor.h"
#include "Processors/Executors/StreamingFormatExecutor.h"
#include "Processors/Formats/IRowInputFormat.h"
#include "Processors/Formats/Impl/CSVRowInputFormat.h"
#include "Processors/Sources/RemoteSource.h"

#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Sources/ShellCommandSource.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Formats/formatBlock.h>

#include <Interpreters/Context.h>

#include <Dictionaries/DictionarySourceFactory.h>
#include <Dictionaries/DictionarySourceHelpers.h>
#include <Dictionaries/DictionaryStructure.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
    extern const int LOGICAL_ERROR;
    extern const int DICTIONARY_ACCESS_DENIED;
    extern const int UNSUPPORTED_METHOD;
    extern const int PATH_ACCESS_DENIED;
    extern const int UNKNOWN_TYPE;
    #ifdef USE_LMDB
    extern const int LMDB_ERROR;
    #endif
}

const int LMDB_MAX_BLOCK_SIZE = DEFAULT_BLOCK_SIZE;

template<typename Lookup> EmbeddedSource<Lookup>::EmbeddedSource(
    ContextPtr context_,
    const Block & sample_block_,
    const std::vector<UInt64> & ids_, // const std::vector<std::string> & keys_,
    Lookup lookup_)
    : SourceWithProgress(sample_block_)
    , context(context_)
    , sample_block(sample_block_)
    , keys(ids_)
    , lookup(lookup_)
{
    description.init(sample_block);
}

template<typename Lookup> Chunk EmbeddedSource<Lookup>::generate()
{
    if (keys.empty() || sample_block.rows() == 0 || cursor >= keys.size())
    all_read = true;

    if (all_read)
        return {};

    EmptyReadBuffer empty;
    const FormatSettings format_settings; // TODO idk if we should let people supply this in the config??
    const RowInputFormatParams params{LMDB_MAX_BLOCK_SIZE};
    // CSVRowInputFormat csv(sample_block, empty, params, false, false, format_settings);
    StreamingFormatExecutor executor(sample_block, std::make_shared<CSVRowInputFormat>(sample_block, empty, params, false, false, format_settings));
    
    size_t final_idx = cursor + std::min(lookup.maxBlockSize(), keys.size() - cursor);
    for (; cursor < final_idx; ++cursor)
    {
        std::string key = std::to_string(keys[cursor]);
        std::unique_ptr<ReadBuffer> s = lookup.lookup(key);
        if (!s)
            continue; // The key wasn't found
        executor.execute(*s);
    }

    auto cols = executor.getResultColumns();

    size_t num_rows = cols.at(0)->size();

    std::cerr << "ROW COUNT " << num_rows << "::::::" << std::endl;

    return Chunk(std::move(cols), num_rows);

}

#ifdef USE_LMDB
LookupLMDB::LookupLMDB(std::string path_, size_t mapsize, std::string dbname)
    : Lookup(path_, LMDB_MAX_BLOCK_SIZE)
{    
    // TODO open db, make txn etc
    if (int res = mdb_env_create(&env)) {
        throw Exception(ErrorCodes::LMDB_ERROR, "LMDB Error creating env: {}", res);
    }

    mdb_env_set_maxdbs(env, 100);
    mdb_env_set_mapsize(env, mapsize);

    if (int res = mdb_env_open(env, getPath().c_str(), MDB_NOTLS | MDB_RDONLY, 0664)) {
        throw Exception(ErrorCodes::LMDB_ERROR, "LMDB Failed to open env: {}", res);
    }

    if (auto res = mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn)) {
        throw Exception(ErrorCodes::LMDB_ERROR, "LMDB Failed to begin transaction: {}", res);
    }

    if (auto res = mdb_dbi_open(txn, dbname.c_str(), 0, &dbi)) {
        throw Exception(ErrorCodes::LMDB_ERROR, "LMDB Failed to open dbi: {}", res);
    }
}

LookupLMDB::LookupLMDB(const LookupLMDB & other) = default;

std::unique_ptr<DB::ReadBuffer> LookupLMDB::lookup(std::string & key) {
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

template<typename Lookup> EmbeddedDictionarySource<Lookup>::EmbeddedDictionarySource(
    const DictionaryStructure & dict_struct_,
    Lookup lookup_,
    Block & sample_block_,
    ContextPtr context_,
    bool created_from_ddl)
    : dict_struct(dict_struct_)
    , lookup(lookup_)
    , sample_block(sample_block_)
    , context(context_)
    , log(&Poco::Logger::get("EmbeddedDictionarySource"))
{
    auto user_files_path = context->getUserFilesPath();
    if (created_from_ddl && !fileOrSymlinkPathStartsWith(lookup.getPath(), user_files_path))
        throw Exception(ErrorCodes::PATH_ACCESS_DENIED, "File path {} is not inside {}", lookup.getPath(), user_files_path);
}

template<typename Lookup> EmbeddedDictionarySource<Lookup>::EmbeddedDictionarySource(const EmbeddedDictionarySource & other)
    : dict_struct(other.dict_struct)
    , lookup(other.lookup)
    , sample_block(other.sample_block)
    , context(Context::createCopy(other.context))
    , log(&Poco::Logger::get("EmbeddedDictionarySource"))
{
}

template<typename Lookup> Pipe EmbeddedDictionarySource<Lookup>::loadAll()
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "ExecutablePoolDictionarySource does not support loadAll method");
}

template<typename Lookup> Pipe EmbeddedDictionarySource<Lookup>::loadUpdatedAll()
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "ExecutablePoolDictionarySource does not support loadUpdatedAll method");
}

template<typename Lookup> Pipe EmbeddedDictionarySource<Lookup>::loadIds(const std::vector<UInt64> & ids)
{
    LOG_TRACE(log, "loadIds {} size = {}", toString(), ids.size());

    // std::vector<std::string> keys;
    // keys.reserve(ids.size());
    // for (UInt64 id : ids)
    //         keys.emplace_back("zone::" + DB::toString(id) + "::zone_plan"); // TODO hmm

    std::cerr << "loadIDS" << ids[0] << "::::::" << std::endl;

    return Pipe(std::make_shared<EmbeddedSource<Lookup>>(
            context,
            sample_block,
            ids, // std::move(keys),
            lookup));
}


template<typename Lookup> Pipe EmbeddedDictionarySource<Lookup>::loadKeys(const Columns &  /*key_columns*/, const std::vector<size_t> &  /*requested_rows*/)
{
    // LOG_TRACE(log, "loadKeys {} size = {}", toString(), requested_rows.size());

    // // TODO
    // return nullptr;

    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "ExecutablePoolDictionarySource does not support loadKeys method");
}

template<typename Lookup> bool EmbeddedDictionarySource<Lookup>::isModified() const
{
    return true;
}

template<typename Lookup> bool EmbeddedDictionarySource<Lookup>::hasUpdateField() const
{
    return false;
}

template<typename Lookup> DictionarySourcePtr EmbeddedDictionarySource<Lookup>::clone() const
{
    return std::make_shared<EmbeddedDictionarySource>(*this);
}

#ifdef USE_LMDB
template<> std::string EmbeddedDictionarySource<LookupLMDB>::toString() const
{
    // TODO figure out what to put in this
    return "Embedded dictionary, provider=LMDB";
}
#endif

void registerDictionarySourceEmbedded(DictionarySourceFactory & factory)
{
    auto create_table_source = [=](const DictionaryStructure & dict_struct,
                                 const Poco::Util::AbstractConfiguration & config,
                                 const std::string & config_prefix,
                                 Block & sample_block,
                                 ContextPtr global_context,
                                 const std::string & /* default_database */,
                                 bool created_from_ddl) -> DictionarySourcePtr
    {
        // TODO need this?
        if (dict_struct.has_expressions)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Dictionary source of type `embedded` does not support attribute expressions");

        ContextMutablePtr context = copyContextAndApplySettingsFromDictionaryConfig(global_context, config, config_prefix);

        String settings_config_prefix = config_prefix + ".embedded";

        const std::string provider = config.getString(settings_config_prefix + ".provider");
        const std::string path = config.getString(settings_config_prefix + ".path");

        if (provider == "LMDB") {
            #ifdef USE_LMDB
            size_t mapsize = 32 * 1024 * 1024 * 1024l;
            if (config.has(settings_config_prefix + ".mapsize"))
                mapsize = config.getInt64(settings_config_prefix + ".mapsize");
            LookupLMDB lookup (
                path,
                mapsize,
                config.getString(settings_config_prefix + ".dbname")
            );
            return std::make_unique<EmbeddedDictionarySource<LookupLMDB>>(dict_struct, lookup, sample_block, context, created_from_ddl);
            #else
            throw Exception(ErrorCodes::FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME, "LMDB not compiled into this instance of ClickHouse, can't use LMDB embedded dictionary");
            #endif
        } else {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "{}: Unknown provider.", provider);
        }
    };

    factory.registerSource("embedded", create_table_source);
}

}
