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

    namespace {
        using ValueType = ExternalResultDescription::ValueType;

        template <typename T>
        inline void insert(IColumn & column, const String & string_value)
        {
            assert_cast<ColumnVector<T> &>(column).insertValue(parse<T>(string_value));
        }

        void insertValue(IColumn & column, const ValueType type, const std::string & string_value)
        {
            if (string_value.empty())
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected not Null String");

            switch (type)
            {
                case ValueType::vtUInt8:
                    insert<UInt8>(column, string_value);
                    break;
                case ValueType::vtUInt16:
                    insert<UInt16>(column, string_value);
                    break;
                case ValueType::vtUInt32:
                    insert<UInt32>(column, string_value);
                    break;
                case ValueType::vtUInt64:
                    insert<UInt64>(column, string_value);
                    break;
                case ValueType::vtInt8:
                    insert<Int8>(column, string_value);
                    break;
                case ValueType::vtInt16:
                    insert<Int16>(column, string_value);
                    break;
                case ValueType::vtInt32:
                    insert<Int32>(column, string_value);
                    break;
                case ValueType::vtInt64:
                    insert<Int64>(column, string_value);
                    break;
                case ValueType::vtFloat32:
                    insert<Float32>(column, string_value);
                    break;
                case ValueType::vtFloat64:
                    insert<Float64>(column, string_value);
                    break;
                case ValueType::vtEnum8:
                case ValueType::vtEnum16:
                case ValueType::vtString:
                    assert_cast<ColumnString &>(column).insert(parse<String>(string_value));
                    break;
                case ValueType::vtDate:
                    assert_cast<ColumnUInt16 &>(column).insertValue(parse<LocalDate>(string_value).getDayNum());
                    break;
                case ValueType::vtDateTime:
                {
                    ReadBufferFromString in(string_value);
                    time_t time = 0;
                    readDateTimeText(time, in);
                    if (time < 0)
                        time = 0;
                    assert_cast<ColumnUInt32 &>(column).insertValue(time);
                    break;
                }
                case ValueType::vtUUID:
                    assert_cast<ColumnUUID &>(column).insertValue(parse<UUID>(string_value));
                    break;
                default:
                    throw Exception(ErrorCodes::UNKNOWN_TYPE,
                        "Value of unsupported type: {}",
                        column.getName());
            }
        }
    }

class ProviderSource final : public SourceWithProgress
    {
    public:

        using SendDataTask = std::function<std::string(std::string)>;
        ProviderSource(
            ContextPtr context_,
            const Block & sample_block_,
            std::vector<std::string> & keys_,
            SendDataTask lookup_)
            : SourceWithProgress(sample_block_)
            , context(context_)
            , sample_block(sample_block_)
            , keys(keys_)
            , lookup(lookup_)

        {
            description.init(sample_block);
        }

        ~ProviderSource() override = default;

    protected:

        Chunk generate() override
        {
            if (keys.empty() || sample_block.rows() == 0 || cursor >= keys.size())
            all_read = true;

            if (all_read)
                return {};

            const size_t size = sample_block.columns();
            MutableColumns columns(size);

            for (size_t i = 0; i < size; ++i)
                columns[i] = sample_block.getByPosition(i).column->cloneEmpty();

            const auto insert_value_by_idx = [this, &columns](size_t idx, const auto & value)
            {
                if (description.types[idx].second) // TODO what is types? second?
                {
                    ColumnNullable & column_nullable = static_cast<ColumnNullable &>(*columns[idx]);
                    insertValue(column_nullable.getNestedColumn(), description.types[idx].first, value);
                    column_nullable.getNullMapData().emplace_back(0);
                }
                else
                    insertValue(*columns[idx], description.types[idx].first, value);
            };

            size_t final_idx = cursor + std::min(max_block_size, keys.size() - cursor);
            for (; cursor < final_idx; ++cursor)
            {
                std::string s = lookup(keys[cursor]);
                insert_value_by_idx(0, keys[cursor]);
                insert_value_by_idx(1, s);
            }

            size_t num_rows = columns.at(0)->size();
            return Chunk(std::move(columns), num_rows);

        }

        Status prepare() override
        {
            auto status = SourceWithProgress::prepare();

            if (status == Status::Finished)
            {
                for (auto & thread : send_data_threads)
                    if (thread.joinable())
                        thread.join();

                rethrowExceptionDuringSendDataIfNeeded();
            }

            return status;
        }

        String getName() const override { return "EmbeddedProviderSource"; }

    private:

        void rethrowExceptionDuringSendDataIfNeeded()
        {
            std::lock_guard<std::mutex> lock(send_data_lock);
            if (exception_during_send_data)
            {
                std::rethrow_exception(exception_during_send_data);
            }
        }

        size_t max_block_size; // TODO populate
        size_t cursor = 0;
        bool all_read = false;

        ExternalResultDescription description;

        ContextPtr context;
        Block sample_block;
        SendDataTask lookup;

        std::vector<std::string> keys;

        std::mutex send_data_lock;
        std::exception_ptr exception_during_send_data;
    };

class Provider {
public:
  explicit Provider(std::string path_)
    : path(path_) {}

  std::string getPath() {
    return path;
  }

  Pipe createPipe(Pipe & input_pipe, Block sample_block, ContextPtr context) {
      auto f = [this](std::string key) -> std::string {
          return lookup(key);
      };
      ProviderSource s(context, sample_block, nullptr, f);
      return Pipe(std::move(s));
  }

  virtual std::string lookup(const std::string & key) = 0;

  virtual ~Provider() = default;
private:
  std::string path;

};

#if USE_LMDB
#include <lmdb.h>
class ProviderLMDB : public Provider {
public:
  ProviderLMDB(std::string path_, UInt64 mapsize_)
    : Provider(path_)
    , mapsize(mapsize_) {}

  ~ProviderLMDB() override = default;

  Pipe createPipe(Pipe &, Block, ContextPtr ) override {
    std::cout << mapsize << std::endl;

    std::string tmp;
    MDB_val key = {
        static_cast<size_t>(tmp.size()),
        static_cast<void *>(tmp.data()),
    };

    MDB_val found = {};

    mdb_get(txn, dbi, &key, &found);
    auto p;
    auto res = Pipe(std::move(p));
    return res;
  }
private:
  MDB_dbi dbi;
  MDB_txn * txn;
  UInt64 mapsize;
};
#endif


/** EmbeddedDictionarySource allows loading data from an embedded dictionary on disk.
  * TODO document thoroughly
  */
template <typename Provider>
class EmbeddedDictionarySource final : public IDictionarySource
{
public:
    
    EmbeddedDictionarySource(
        const DictionaryStructure & dict_struct_,
        Provider & provider_,
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
    Provider & provider;

    Block sample_block;
    ContextPtr context;
    Poco::Logger * log;
};

}
