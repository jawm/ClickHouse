#include "EmbeddedDictionarySource.h"

#include <filesystem>

#include <boost/algorithm/string/split.hpp>

#include <base/logger_useful.h>
#include <Common/LocalDateTime.h>
#include <Common/filesystemHelpers.h>
#include <Common/ShellCommand.h>

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
}


template<typename Provider> EmbeddedDictionarySource<Provider>::EmbeddedDictionarySource(
    const DictionaryStructure & dict_struct_,
    Provider & provider_,
    Block & sample_block_,
    ContextPtr context_,
    bool created_from_ddl)
    : dict_struct(dict_struct_)
    , provider(provider_)
    , sample_block(sample_block_)
    , context(context_)
    , log(&Poco::Logger::get("EmbeddedDictionarySource"))
{
    auto user_files_path = context->getUserFilesPath();
    if (created_from_ddl && !fileOrSymlinkPathStartsWith(provider.getPath(), user_files_path))
        throw Exception(ErrorCodes::PATH_ACCESS_DENIED, "File path {} is not inside {}", provider.getPath(), user_files_path);
}

template<typename Provider> EmbeddedDictionarySource<Provider>::EmbeddedDictionarySource(const EmbeddedDictionarySource & other)
    : dict_struct(other.dict_struct)
    , provider(other.provider)
    , sample_block(other.sample_block)
    , context(Context::createCopy(other.context))
    , log(&Poco::Logger::get("EmbeddedDictionarySource"))
{
}

template<typename Provider> Pipe EmbeddedDictionarySource<Provider>::loadAll()
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "ExecutablePoolDictionarySource does not support loadAll method");
}

template<typename Provider> Pipe EmbeddedDictionarySource<Provider>::loadUpdatedAll()
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "ExecutablePoolDictionarySource does not support loadUpdatedAll method");
}

// template<typename Provider> Pipe EmbeddedDictionarySource<Provider>::loadIds(const std::vector<UInt64> & ids)
// {
//     LOG_TRACE(log, "loadIds {} size = {}", toString(), ids.size());

//     auto block = blockForIds(dict_struct, ids);
//     return getStreamForBlock(block);
// }

#ifdef USE_LMDB
template<> Pipe EmbeddedDictionarySource<ProviderLMDB>::loadIds(const std::vector<UInt64> & ids)
{
    std::vector<std::string> keys;
    for (UInt64 id : ids)
            keys.emplace_back(DB::toString(id));

    return Pipe(std::make_shared<RedisSource>(
            std::move(connection), std::move(keys),
            configuration.storage_type, sample_block, REDIS_MAX_BLOCK_SIZE));
}
#endif

template<typename Provider> Pipe EmbeddedDictionarySource<Provider>::loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows)
{
    LOG_TRACE(log, "loadKeys {} size = {}", toString(), requested_rows.size());

    auto block = blockForKeys(dict_struct, key_columns, requested_rows);
    return getStreamForBlock(block);
}

#ifdef USE_LMDB
template<> Pipe EmbeddedDictionarySource<ProviderLMDB>::getStreamForBlock(const Block & block)
{
    auto source = std::make_shared<SourceFromSingleChunk>(block);
    auto provider_input_pipe = Pipe(std::move(source));

    auto pipe = provider.createPipe(
        provider_input_pipe,
        sample_block,
        context);

    return pipe;
}
#endif

template<typename Provider> bool EmbeddedDictionarySource<Provider>::isModified() const
{
    return true;
}

template<typename Provider> bool EmbeddedDictionarySource<Provider>::hasUpdateField() const
{
    return false;
}

template<typename Provider> DictionarySourcePtr EmbeddedDictionarySource<Provider>::clone() const
{
    return std::make_shared<EmbeddedDictionarySource>(*this);
}

#ifdef USE_LMDB
template<> std::string EmbeddedDictionarySource<ProviderLMDB>::toString() const
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

        const std::string provider = config.getString(config_prefix + ".provider");

        if (provider == "LMDB") {
            #ifdef USE_LMDB
            ProviderLMDB provider (
                config.getString(settings_config_prefix = ".path"),
                config.getInt64(settings_config_prefix + ".provider.mapsize")
            );
            return std::make_unique<EmbeddedDictionarySource<ProviderLMDB>>(dict_struct, provider, sample_block, context, created_from_ddl);
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
