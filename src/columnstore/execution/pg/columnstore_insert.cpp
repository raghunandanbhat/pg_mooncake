#include "columnstore/columnstore.hpp"
#include "duckdb/main/client_context.hpp"
#include "pgduckdb/pgduckdb_types.hpp"
#include "pgduckdb/pg/relations.hpp"

extern "C" {
#include "postgres.h"
#include "executor/tuptable.h"
}

#include "pgduckdb/pgduckdb_detoast.hpp"
#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/utility/cpp_wrapper.hpp"


void duckdb::Columnstore::PgInsert(::Relation rel, TupleTableSlot **slots, int nslots) {
    elog(INFO, "In PgInsert()-");

    auto &context = pgduckdb::DuckDBManager::Get().GetConnection()->context;
    const char *schema_name = pgduckdb::GetNamespaceName(rel);
    const char *table_name = pgduckdb::GetRelationName(rel);
    elog(INFO, "Schema: %s, Table Name: %s", schema_name, table_name);
    
    TupleDesc desc = pgduckdb::RelationGetDescr(rel);
    duckdb::vector<duckdb::LogicalType> types;
    for (int i = 0; i < desc->natts; i++) {
        Form_pg_attribute attr = pgduckdb::GetAttr(desc, i);
        types.push_back(pgduckdb::ConvertPostgresToDuckColumnType(attr));
    }
    duckdb::DataChunk chunk;
    chunk.Initialize(*context, types);
    
    for (int row = 0; row < nslots; row++) {
        TupleTableSlot *slot = slots[row];
        for ( int col = 0; col < desc->natts; col++) {
            auto &vec = chunk.data[col];
            if (slot->tts_isnull[col]) {
                duckdb::FlatVector::Validity(vec).SetInvalid(chunk.size());
            } else {
                if (desc->attrs[col].attlen == -1){
                    bool should_free = false;
                    Datum value = pgduckdb::DetoastPostgresDatum(reinterpret_cast<varlena *>(slot->tts_values[col]), &should_free);
                    pgduckdb::ConvertPostgresToDuckValue(slot->tts_tableOid, value,vec, chunk.size());
                    if (should_free) {
                        duckdb_free(reinterpret_cast<void *>(value));
                    }
                } else {
                    pgduckdb::ConvertPostgresParameterToDuckValue(slot->tts_values[col], chunk.size());
                }
            }
        }
    }
    chunk.SetCardinality(chunk.size() + 1);

    //CoulmnstoreTable::Insert()
}