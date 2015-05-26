#include "logdecoder.h"
#include "format-json.h"
#include "funcapi.h"
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "parser/parse_coerce.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/json.h"
#include "utils/jsonapi.h"
#include "utils/lsyscache.h"

#include "io_util.h"
#include "protocol_server.h"

/* String to output for infinite dates and timestamps */
#define DT_INFINITY "\"infinity\""

static void output_json_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt, bool is_init);
static void output_json_shutdown(LogicalDecodingContext *ctx);
static void output_json_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn);
static void output_json_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, XLogRecPtr commit_lsn);
static void output_json_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, Relation rel, ReorderBufferChange *change);


void output_format_json_init(OutputPluginCallbacks *cb) {
    elog(DEBUG1, "bottledwater: output_format_json_init");
    cb->startup_cb = output_json_startup;
    cb->begin_cb = output_json_begin_txn;
    cb->change_cb = output_json_change;
    cb->commit_cb = output_json_commit_txn;
    cb->shutdown_cb = output_json_shutdown;
}

static void output_json_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt,
        bool is_init) {
    opt->output_type = OUTPUT_PLUGIN_TEXTUAL_OUTPUT;
}

static void output_json_shutdown(LogicalDecodingContext *ctx) {
}

static void output_json_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn) {
    OutputPluginPrepareWrite(ctx, true);
    appendStringInfo(ctx->out, "{ \"command\": \"BEGIN\", \"xid\": %u }", txn->xid);
    OutputPluginWrite(ctx, true);
}

static void output_json_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
        XLogRecPtr commit_lsn) {
    OutputPluginPrepareWrite(ctx, true);
    appendStringInfo(ctx->out, "{ \"command\": \"COMMIT\", \"xid\": %u }", txn->xid);
    OutputPluginWrite(ctx, true);
}

static void output_json_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
        Relation rel, ReorderBufferChange *change) {
    HeapTuple oldtuple = NULL, newtuple = NULL;
    const char *command = NULL;

    switch (change->action) {
        case REORDER_BUFFER_CHANGE_INSERT:
            if (!change->data.tp.newtuple) {
                elog(ERROR, "output_json_change: insert action without a tuple");
            }
            newtuple = &change->data.tp.newtuple->tuple;

            command = "INSERT";
            break;

        case REORDER_BUFFER_CHANGE_UPDATE:
            if (!change->data.tp.newtuple) {
                elog(ERROR, "output_json_change: update action without a tuple");
            }
            newtuple = &change->data.tp.newtuple->tuple;

            if (change->data.tp.oldtuple) {
                oldtuple = &change->data.tp.oldtuple->tuple;
            }
            command = "UPDATE";
            break;

        case REORDER_BUFFER_CHANGE_DELETE:
            if (change->data.tp.oldtuple) {
                oldtuple = &change->data.tp.oldtuple->tuple;
            }
            command = "DELETE";
            break;

        default:
            elog(ERROR, "output_json_change: unknown change action %d", change->action);
    }

    OutputPluginPrepareWrite(ctx, true);

    output_json_relation_header(ctx->out, command, txn->xid, change->lsn, rel);
    if (newtuple) {
        appendStringInfoString(ctx->out, ", \"newtuple\": ");
        output_json_tuple(ctx->out, newtuple, RelationGetDescr(rel));
    }
    if (oldtuple) {
        appendStringInfoString(ctx->out, ", \"oldtuple\": ");
        output_json_tuple(ctx->out, oldtuple, RelationGetDescr(rel));
    }
    appendStringInfoString(ctx->out, " }");

    OutputPluginWrite(ctx, true);
}

void output_json_relation_header(StringInfo out, const char *cmd,
                                 TransactionId xid, XLogRecPtr lsn, Relation rel) {
    appendStringInfo(out,
                     "{ \"command\": \"%s\""
                     ", \"xid\": %u"
                     ", \"wal_pos\": \"%X/%X\""
                     ", \"relname\": \"%s\""
                     ", \"relnamespace\": \"%s\"",
                     cmd,
                     xid,
                     (uint32) (lsn >> 32), (uint32) lsn,
                     RelationGetRelationName(rel),
                     get_namespace_name(RelationGetNamespace(rel)));
}

/* most of the following code is taken from utils/adt/json.c and put into one function */
void output_json_tuple(StringInfo out, HeapTuple tuple, TupleDesc desc) {
    int i;
    bool need_sep = false;

    appendStringInfoChar(out, '{');

    for (i = 0; i < desc->natts; i++) {
        Form_pg_attribute attr = desc->attrs[i];
        Datum val;
        bool isnull;
        Oid typoid;
        Oid outfuncoid;
        bool typisvarlena;
        char *outputstr = NULL;

        if (attr->attisdropped) {
            continue;
        }

        if (need_sep) { /* can't count on i > 0, because of attisdropped just above */
            appendStringInfoChar(out, ',');
        }
        need_sep = true;

        escape_json(out, NameStr(attr->attname));
        appendStringInfoChar(out, ':');

        val = heap_getattr(tuple, i + 1, desc, &isnull);
        if (isnull) {
            appendStringInfoString(out, "null");
            continue;
        }

        typoid = getBaseType(attr->atttypid);
        getTypeOutputInfo(typoid, &outfuncoid, &typisvarlena);

        switch (typoid) {
            case BOOLOID:
                appendStringInfoString(out, DatumGetBool(val) ? "true" : "false");
                break;

            case INT2OID:
            case INT4OID:
            case FLOAT4OID:
            case FLOAT8OID:
                outputstr = OidOutputFunctionCall(outfuncoid, val);
                if (IsValidJsonNumber(outputstr, strlen(outputstr))) {
                    appendStringInfoString(out, outputstr);
                } else {
                    escape_json(out, outputstr);
                }
                break;

            case INT8OID:
            case NUMERICOID:
                outputstr = OidOutputFunctionCall(outfuncoid, val);
                escape_json(out, outputstr);
                break;

            case DATEOID:
                {
                    DateADT date;
                    struct pg_tm tm;
                    char buf[MAXDATELEN + 1];

                    date = DatumGetDateADT(val);

                    if (DATE_NOT_FINITE(date)) {
                        /* we have to format infinity ourselves */
                        appendStringInfoString(out, DT_INFINITY);
                    } else {
                        j2date(date + POSTGRES_EPOCH_JDATE,
                               &(tm.tm_year), &(tm.tm_mon), &(tm.tm_mday));
                        EncodeDateOnly(&tm, USE_XSD_DATES, buf);
                        appendStringInfo(out, "\"%s\"", buf);
                    }
                }
                break;

            case TIMESTAMPOID:
                {
                    Timestamp timestamp;
                    struct pg_tm tm;
                    fsec_t fsec;
                    char buf[MAXDATELEN + 1];

                    timestamp = DatumGetTimestamp(val);

                    if (TIMESTAMP_NOT_FINITE(timestamp)) {
                        /* we have to format infinity ourselves */
                        appendStringInfoString(out, DT_INFINITY);
                    } else if (timestamp2tm(timestamp, NULL, &tm, &fsec, NULL, NULL) == 0) {
                        EncodeDateTime(&tm, fsec, false, 0, NULL, USE_XSD_DATES, buf);
                        appendStringInfo(out, "\"%s\"", buf);
                    } else {
                        ereport(ERROR,
                                (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                                 errmsg("timestamp out of range")));
                    }
                }
                break;

            case TIMESTAMPTZOID:
                {
                    TimestampTz timestamp;
                    struct pg_tm tm;
                    int  tz;
                    fsec_t fsec;
                    const char *tzn = NULL;
                    char buf[MAXDATELEN + 1];

                    timestamp = DatumGetTimestamp(val);

                    if (TIMESTAMP_NOT_FINITE(timestamp)) {
                        /* we have to format infinity ourselves */
                        appendStringInfoString(out, DT_INFINITY);
                    } else if (timestamp2tm(timestamp, &tz, &tm, &fsec, &tzn, NULL) == 0) {
                        EncodeDateTime(&tm, fsec, true, tz, tzn, USE_XSD_DATES, buf);
                        appendStringInfo(out, "\"%s\"", buf);
                    } else {
                        ereport(ERROR,
                                (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                                 errmsg("timestamp out of range")));
                    }
                }
                break;

            default:
                {
                    text *jsontext = NULL;

                    if (OidIsValid(get_element_type(typoid))) {
                        jsontext = DatumGetTextP(DirectFunctionCall1(array_to_json, val));
                    }

                    else if (type_is_rowtype(typoid)) {
                        jsontext = DatumGetTextP(DirectFunctionCall1(row_to_json, val));
                    }

                    else if (typoid >= FirstNormalObjectId) {
                        Oid castfuncoid;
                        CoercionPathType ctype;

                        ctype = find_coercion_pathway(JSONOID, typoid, COERCION_EXPLICIT,
                                                      &castfuncoid);
                        if (ctype == COERCION_PATH_FUNC && OidIsValid(castfuncoid)) {
                            jsontext = DatumGetTextP(OidFunctionCall1(castfuncoid, val));
                        }
                    }

                    if (jsontext) {
                        outputstr = text_to_cstring(jsontext);
                        pfree(jsontext);
                    }

                    if (outputstr) {
                        /*
                         * We assume anything produced by cast or
                         * *_to_json() calls to return valid json:
                         * don't escape it.
                         */
                        appendStringInfoString(out, outputstr);
                    } else {
                        outputstr = OidOutputFunctionCall(outfuncoid, val);
                        escape_json(out, outputstr);
                    }
                }
                break;
        }

        if (outputstr) {
            pfree(outputstr);
        }
    }

    appendStringInfoChar(out, '}');
}
