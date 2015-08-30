#include "logdecoder.h"
#include "format-json.h"
#include "nodes/parsenodes.h"
#include "utils/elog.h"

PG_MODULE_MAGIC;

/* Entry point when Postgres loads the plugin */
extern void _PG_init(void);
extern void _PG_output_plugin_init(OutputPluginCallbacks *cb);

static void output_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt, bool is_init);
static void output_shutdown(LogicalDecodingContext *ctx);
static void output_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn);
static void output_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, XLogRecPtr commit_lsn);
static void output_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, Relation rel, ReorderBufferChange *change);


void _PG_init() {
}

void _PG_output_plugin_init(OutputPluginCallbacks *cb) {
    elog(DEBUG1, "json_decoder: _PG_output_plugin_init");
    AssertVariableIsOfType(&_PG_output_plugin_init, LogicalOutputPluginInit);
    cb->startup_cb = output_startup;
    cb->begin_cb = output_begin_txn;
    cb->change_cb = output_change;
    cb->commit_cb = output_commit_txn;
    cb->shutdown_cb = output_shutdown;
}

static void output_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt,
        bool is_init) {
    plugin_state *state;
    void (*format_init_func)(OutputPluginCallbacks *) = NULL;

    elog(DEBUG1, "json_decoder: output_startup: is_init=%s", is_init ? "true" : "false");
    if (is_init) {
        return;
    }
    state = palloc0(sizeof(plugin_state));
    ctx->output_plugin_private = state;

    state->memctx = AllocSetContextCreate(ctx->context, "json_decoder context",
            ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);

    /* Use JSON */
    format_init_func = output_format_json_init;

    state->format_cb = palloc0(sizeof(OutputPluginCallbacks));
    format_init_func(state->format_cb);

    state->format_cb->startup_cb(ctx, opt, is_init);
}

static void output_shutdown(LogicalDecodingContext *ctx) {
    plugin_state *state = ctx->output_plugin_private;
    MemoryContext oldctx;

    /* state can be NULL if we are in CreateReplicationSlot */
    if (state) {
        oldctx = MemoryContextSwitchTo(state->memctx);

        state->format_cb->shutdown_cb(ctx);

        MemoryContextSwitchTo(oldctx);
        MemoryContextDelete(state->memctx);
    }
}

static void output_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn) {
    plugin_state *state = ctx->output_plugin_private;
    MemoryContext oldctx = MemoryContextSwitchTo(state->memctx);

    state->format_cb->begin_cb(ctx, txn);

    MemoryContextSwitchTo(oldctx);
    MemoryContextReset(state->memctx);
}

static void output_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
        XLogRecPtr commit_lsn) {
    plugin_state *state = ctx->output_plugin_private;
    MemoryContext oldctx = MemoryContextSwitchTo(state->memctx);

    state->format_cb->commit_cb(ctx, txn, commit_lsn);

    MemoryContextSwitchTo(oldctx);
    MemoryContextReset(state->memctx);
}

static void output_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
        Relation rel, ReorderBufferChange *change) {
    plugin_state *state = ctx->output_plugin_private;
    MemoryContext oldctx = MemoryContextSwitchTo(state->memctx);

    state->format_cb->change_cb(ctx, txn, rel, change);

    MemoryContextSwitchTo(oldctx);
    MemoryContextReset(state->memctx);
}
