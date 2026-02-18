/*
 * feature_service_c.h
 *
 * Pure-C public API for FeatureService.
 *
 * Architecture
 * ------------
 *  ┌──────────────────────────────────────────────────────┐
 *  │  C caller  (any language that links C symbols)       │
 *  │   feat_service_submit(svc, "libfoo.compress", ...)   │
 *  └─────────────────┬────────────────────────────────────┘
 *                    │  op name + opaque params bytes
 *                    ▼
 *  ┌──────────────────────────────────────────────────────┐
 *  │  C wrapper  (this file + feature_service_c.cpp)      │
 *  │   dispatch table: op_name → (handler_fn, lib_ctx)    │
 *  └─────────────────┬────────────────────────────────────┘
 *                    │  calls handler on worker thread
 *                    ▼
 *  ┌──────────────────────────────────────────────────────┐
 *  │  feat::FeatureService  (C++ single-worker queue)     │
 *  └──────────────────────────────────────────────────────┘
 *                    │  dispatches to registered handler
 *                    ▼
 *  ┌──────────────────────────────────────────────────────┐
 *  │  C library A   C library B   C library C   ...       │
 *  │  (each with its own API, registered under a name)    │
 *  └──────────────────────────────────────────────────────┘
 *
 * Usage pattern
 * -------------
 *  1. feat_service_create()               – allocate + configure
 *  2. feat_service_register_handler()     – one call per C library / operation
 *  3. feat_service_start()                – spawn worker thread
 *  4. feat_service_submit()               – dispatch a named operation
 *  5. (optional) feat_service_cancel()    – best-effort cancellation
 *  6. feat_service_stop() / _destroy()    – teardown
 *
 * Thread safety
 * -------------
 *  - All API functions are safe to call from any thread.
 *  - Handlers are invoked serially on the single worker thread.
 *  - The user callback is invoked from the worker thread (results) or
 *    the calling thread (lifecycle events from start/stop).
 *
 * Memory ownership
 * ----------------
 *  - params passed to feat_service_submit() are copied immediately;
 *    the caller may free them as soon as submit() returns.
 *  - Payloads delivered to the callback are heap-allocated by the service.
 *    The callback MUST call feat_payload_release() when done with each one.
 *  - Output buffers written by handlers must be malloc()'d by the handler;
 *    the service takes ownership and frees them after copying into the result.
 */

#ifndef FEATURE_SERVICE_C_H
#define FEATURE_SERVICE_C_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* =========================================================================
 * Opaque service handle
 * ========================================================================= */

typedef struct feat_service_s feat_service_t;

/* =========================================================================
 * Ticket
 * ========================================================================= */

typedef uint64_t feat_ticket_t;
#define FEAT_TICKET_INVALID ((feat_ticket_t)0)

/* =========================================================================
 * Status codes  (mirror feat::Status values)
 * ========================================================================= */

typedef enum {
    FEAT_OK        = 0,
    FEAT_INVALID   = 1,
    FEAT_BAD_STATE = 2,
    FEAT_INTERNAL  = 3,
    FEAT_NOT_FOUND = 4
} feat_status_t;

/* =========================================================================
 * Payload  (layout-compatible with feat::FeaturePayload)
 * ========================================================================= */

typedef struct {
    char  tag[8];   /* 8-byte ASCII tag – identifies the payload type     */
    void* data;     /* tag-specific pointer; release via feat_payload_release() */
} feat_payload_t;

/* Tag constants (null-terminated for convenience; only first 8 bytes used) */
#define FEAT_TAG_RUNNING "FEATRUN1"   /* lifecycle event  */
#define FEAT_TAG_RESULT  "FEATRES1"   /* task result      */

/* Lifecycle payload  (payload->data when tag == FEAT_TAG_RUNNING) */
typedef struct {
    uint8_t state;  /* 0 = NotRunning, 1 = Running */
} feat_running_data_t;

/* Result payload  (payload->data when tag == FEAT_TAG_RESULT) */
typedef struct {
    feat_ticket_t ticket;
    void*         data;    /* malloc'd result bytes, may be NULL */
    size_t        size;    /* 0 for empty / cancelled-pending    */
} feat_result_data_t;

/* =========================================================================
 * User callback
 *
 *  Invoked on:
 *    - worker thread  for task results   (FEAT_TAG_RESULT)
 *    - calling thread for lifecycle events (FEAT_TAG_RUNNING) from start/stop
 *
 *  The callback takes ownership of `payload`; call feat_payload_release()
 *  when finished.
 * ========================================================================= */

typedef void (*feat_callback_t)(void* user, feat_payload_t* payload);

/* =========================================================================
 * Init options  (null-terminated key/value strings)
 * ========================================================================= */

typedef struct {
    const char* key;
    const char* value;
} feat_option_t;

/* =========================================================================
 * Operation handler
 *
 *  Registered per operation name via feat_service_register_handler().
 *  Called on the worker thread for each matching submitted task.
 *
 *  Parameters
 *  ----------
 *  op      : the operation name (same string used in feat_service_submit)
 *  params  : opaque input bytes copied from the submit call (may be NULL)
 *  params_size : byte length of params (0 if params is NULL)
 *  out     : handler must malloc() a buffer and assign *out; the service
 *            copies the bytes into the result payload and then free()s *out.
 *            Set *out = NULL / *out_size = 0 to emit an empty result.
 *  out_size: byte length of *out
 *  lib_ctx : the library-specific context registered with the handler
 *
 *  Return value
 *  ------------
 *  0       : success – *out/*out_size are used as the result
 *  non-zero: error   – result is emitted as empty (service ignores *out)
 *
 *  The handler may examine a cancel signal via a library-specific mechanism
 *  set up in the feat_cancel_fn (see below) and return early with empty output.
 * ========================================================================= */

typedef int (*feat_handler_fn)(
    const char* op,
    const void* params,
    size_t      params_size,
    uint8_t**   out,
    size_t*     out_size,
    void*       lib_ctx
);

/* =========================================================================
 * Native cancel hook  (optional, passed per-submit)
 *
 *  Called best-effort when feat_service_cancel() is issued while the task
 *  is already running on the worker thread.  Use it to signal the underlying
 *  C library to abort (e.g. set an atomic flag the handler polls).
 *
 *  cancel_ctx : arbitrary pointer supplied at submit time
 * ========================================================================= */

typedef void (*feat_cancel_fn)(void* cancel_ctx);

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

/*
 * feat_service_create
 *
 *  Allocates and initialises a service instance.
 *  Does NOT start the worker thread; call feat_service_start() next.
 *
 *  cb      : result/event callback (may be NULL if results are not needed)
 *  user    : opaque pointer passed back in every callback invocation
 *  opts    : array of key/value option pairs (strings are copied)
 *  n_opts  : number of elements in opts (0 if opts is NULL)
 *
 *  Returns NULL on allocation failure.
 */
feat_service_t* feat_service_create(
    feat_callback_t      cb,
    void*                user,
    const feat_option_t* opts,
    size_t               n_opts
);

/*
 * feat_service_destroy
 *
 *  Stops the service (if running) and frees all resources.
 *  Blocks until the worker thread exits.
 *  Safe to call with NULL.
 */
void feat_service_destroy(feat_service_t* svc);

/*
 * feat_service_start
 *
 *  Spawns the worker thread.  Emits a FEAT_TAG_RUNNING (state=1) payload
 *  to the callback.  Idempotent: safe to call when already running.
 */
feat_status_t feat_service_start(feat_service_t* svc);

/*
 * feat_service_stop
 *
 *  Stops accepting new tasks, drains the queue, joins the worker thread,
 *  then emits a FEAT_TAG_RUNNING (state=0) payload.  Idempotent.
 */
feat_status_t feat_service_stop(feat_service_t* svc);

/* =========================================================================
 * Dispatch table registration
 * ========================================================================= */

/*
 * feat_service_register_handler
 *
 *  Associates a handler function (and library context) with an operation
 *  name.  Replaces any previously registered handler for the same name.
 *
 *  op      : null-terminated operation name, e.g. "libfoo.compress"
 *  handler : function invoked on the worker thread for every matching submit
 *  lib_ctx : arbitrary pointer forwarded to handler (e.g. lib handle/config)
 *
 *  Returns FEAT_INVALID if svc, op, or handler is NULL.
 *  May be called at any time (before or after start).
 *
 *  Example – registering two C libraries:
 *
 *    feat_service_register_handler(svc, "imglib.resize",  imglib_resize_handler,  &imglib_ctx);
 *    feat_service_register_handler(svc, "cryptolib.hash", crypto_hash_handler,    &crypto_ctx);
 */
feat_status_t feat_service_register_handler(
    feat_service_t* svc,
    const char*     op,
    feat_handler_fn handler,
    void*           lib_ctx
);

/* =========================================================================
 * Submit / Cancel
 * ========================================================================= */

/*
 * feat_service_submit
 *
 *  Enqueues a task that dispatches to the handler registered under `op`.
 *
 *  op          : operation name (must match a registered handler)
 *  params      : input data for the handler (copied immediately; caller can
 *                free after this call returns). NULL is valid (params_size=0).
 *  params_size : byte length of params
 *  cancel_fn   : optional hook called if the task is cancelled while running
 *  cancel_ctx  : opaque pointer passed to cancel_fn
 *
 *  Returns a non-zero ticket on success, FEAT_TICKET_INVALID (0) on failure
 *  (service not running, allocation error, NULL op).
 *
 *  The result is delivered asynchronously via the callback with tag
 *  FEAT_TAG_RESULT.  The ticket in the result payload matches the return value.
 *
 *  Example:
 *
 *    struct ImgResizeParams { int w; int h; const uint8_t* src; size_t src_len; };
 *    struct ImgResizeParams p = { 640, 480, jpeg_buf, jpeg_len };
 *    feat_ticket_t t = feat_service_submit(svc,
 *                                          "imglib.resize",
 *                                          &p, sizeof(p),
 *                                          NULL, NULL);
 */
feat_ticket_t feat_service_submit(
    feat_service_t* svc,
    const char*     op,
    const void*     params,
    size_t          params_size,
    feat_cancel_fn  cancel_fn,
    void*           cancel_ctx
);

/*
 * feat_service_cancel
 *
 *  Requests cancellation of a previously submitted task.
 *
 *  Pending  : removed from queue; callback receives an empty FEAT_TAG_RESULT
 *             immediately.
 *  Running  : cancel_fn (if supplied at submit time) is called; the worker
 *             completes the task and emits whatever result fn(out) produced.
 *  Done     : returns FEAT_NOT_FOUND.
 */
feat_status_t feat_service_cancel(feat_service_t* svc, feat_ticket_t ticket);

/* =========================================================================
 * Payload memory management
 * ========================================================================= */

/*
 * feat_payload_release
 *
 *  Releases a payload received in the callback.
 *  Must be called exactly once per payload; safe to call with NULL.
 */
void feat_payload_release(feat_payload_t* payload);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* FEATURE_SERVICE_C_H */
