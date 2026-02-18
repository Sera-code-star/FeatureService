/*
 * feature_service_c.cpp
 *
 * C++ implementation of the pure-C wrapper around feat::FeatureService.
 *
 * Key design decisions
 * --------------------
 *
 * 1. Layout-compatible cast between C and C++ payload types.
 *    feat_payload_t and feat::FeaturePayload are both standard-layout structs
 *    with identical fields in the same order (char tag[8]; void* data;).
 *    The same holds for feat_result_data_t / feat::FeatureResultData and
 *    feat_running_data_t / feat::FeatureRunningData.
 *    We therefore reinterpret_cast between them in the callback bridge instead
 *    of copying, which avoids an extra allocation on the hot path.
 *
 * 2. Params lifetime safety via shared_ptr.
 *    params are copied on submit() and stored in a shared_ptr<void> captured
 *    by the work lambda.  If the task is cancelled before it runs, the
 *    FeatureService destroys the std::function (and its captures), which
 *    triggers the shared_ptr destructor and frees the copy – no leak.
 *
 * 3. Handler lookup outside the mutex.
 *    The dispatch table lock is acquired only to copy the HandlerEntry, then
 *    released before invoking the handler.  This keeps the critical section
 *    minimal and avoids holding the lock during potentially long C-lib calls.
 *
 * 4. Null callback handling.
 *    callback_bridge is always set as the C++ callback so that payloads are
 *    always released.  If the user supplied a null cb, the bridge frees the
 *    payload internally.
 */

#include "feature_service_c.h"
#include "FeatureService.h"

#include <cstring>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <functional>
#include <new>

/* =========================================================================
 * Internal structures
 * ========================================================================= */

namespace {

struct HandlerEntry {
    feat_handler_fn fn;
    void*           lib_ctx;
};

} /* anonymous namespace */

struct feat_service_s {
    feat::FeatureService                          svc;
    feat_callback_t                               user_cb;
    void*                                         user_ctx;
    std::mutex                                    handler_mtx;
    std::unordered_map<std::string, HandlerEntry> handlers;

    feat_service_s(feat_callback_t cb, void* user, const feat::FeatureOptions& opts)
        : svc(&feat_service_s::callback_bridge, this, opts)
        , user_cb(cb)
        , user_ctx(user)
    {}

    /*
     * callback_bridge
     *
     * Translates feat::FeatureCallback → feat_callback_t.
     *
     * The two payload struct hierarchies are layout-compatible, so we
     * reinterpret_cast the pointer without any data movement:
     *
     *   feat::FeaturePayload  { char tag[8]; void* data; }
     *   feat_payload_t        { char tag[8]; void* data; }  ← same layout
     *
     * If the user provided no callback, we release the payload ourselves
     * (feat::FeatureService always calls the registered C++ callback).
     */
    static void callback_bridge(void* self_ptr, feat::FeaturePayload* payload)
    {
        feat_service_s* self = static_cast<feat_service_s*>(self_ptr);
        feat_payload_t* cpayload = reinterpret_cast<feat_payload_t*>(payload);

        if (self->user_cb) {
            self->user_cb(self->user_ctx, cpayload);
        } else {
            feat::FeatureService::DeletePayload(payload);
        }
    }
};

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

feat_service_t* feat_service_create(
    feat_callback_t      cb,
    void*                user,
    const feat_option_t* opts,
    size_t               n_opts)
{
    feat::FeatureOptions options;
    for (size_t i = 0; i < n_opts; ++i) {
        if (opts && opts[i].key && opts[i].value) {
            options[opts[i].key] = opts[i].value;
        }
    }
    return new (std::nothrow) feat_service_s(cb, user, options);
}

void feat_service_destroy(feat_service_t* svc)
{
    delete svc; /* ~feat_service_s → ~FeatureService → stop() + join */
}

feat_status_t feat_service_start(feat_service_t* svc)
{
    if (!svc) return FEAT_INVALID;
    return static_cast<feat_status_t>(static_cast<int>(svc->svc.start()));
}

feat_status_t feat_service_stop(feat_service_t* svc)
{
    if (!svc) return FEAT_INVALID;
    return static_cast<feat_status_t>(static_cast<int>(svc->svc.stop()));
}

/* =========================================================================
 * Dispatch table
 * ========================================================================= */

feat_status_t feat_service_register_handler(
    feat_service_t* svc,
    const char*     op,
    feat_handler_fn handler,
    void*           lib_ctx)
{
    if (!svc || !op || !handler) return FEAT_INVALID;

    HandlerEntry entry{ handler, lib_ctx };
    std::lock_guard<std::mutex> lk(svc->handler_mtx);
    svc->handlers[op] = entry;
    return FEAT_OK;
}

/* =========================================================================
 * Submit
 * ========================================================================= */

feat_ticket_t feat_service_submit(
    feat_service_t* svc,
    const char*     op,
    const void*     params,
    size_t          params_size,
    feat_cancel_fn  cancel_fn,
    void*           cancel_ctx)
{
    if (!svc || !op) return FEAT_TICKET_INVALID;

    /* --- Copy params immediately so caller can free after this returns --- */
    std::shared_ptr<void> params_holder; /* empty = no params */
    if (params && params_size) {
        void* buf = std::malloc(params_size);
        if (!buf) return FEAT_TICKET_INVALID;
        std::memcpy(buf, params, params_size);
        params_holder = std::shared_ptr<void>(buf, std::free);
    }

    /* --- Capture everything by value for the worker lambda --- */
    std::string   op_str(op);
    size_t        captured_size = params_size;

    auto work = [svc, op_str, params_holder, captured_size]
                (std::vector<uint8_t>& out)
    {
        /* Lookup handler (copy entry, then release lock before calling) */
        HandlerEntry entry{};
        bool found = false;
        {
            std::lock_guard<std::mutex> lk(svc->handler_mtx);
            auto it = svc->handlers.find(op_str);
            if (it != svc->handlers.end()) {
                entry = it->second;
                found = true;
            }
        }

        if (!found || !entry.fn) {
            /* No handler registered for this op – emit empty result */
            return;
        }

        uint8_t* out_ptr  = nullptr;
        size_t   out_size = 0;

        int rc = entry.fn(op_str.c_str(),
                          params_holder.get(),
                          captured_size,
                          &out_ptr,
                          &out_size,
                          entry.lib_ctx);

        if (rc == 0 && out_ptr && out_size) {
            out.assign(out_ptr, out_ptr + out_size);
        }
        /* Handler is responsible for malloc; we free it after copying. */
        std::free(out_ptr);
        /*
         * params_holder shared_ptr is destroyed at end of lambda scope
         * (or when the std::function is destroyed on cancel-before-run),
         * freeing the params copy automatically.
         */
    };

    /* --- Optional native cancel hook --- */
    std::function<void()> native_cancel;
    if (cancel_fn) {
        native_cancel = [cancel_fn, cancel_ctx]() { cancel_fn(cancel_ctx); };
    }

    return svc->svc.submit(work, native_cancel);
}

/* =========================================================================
 * Cancel
 * ========================================================================= */

feat_status_t feat_service_cancel(feat_service_t* svc, feat_ticket_t ticket)
{
    if (!svc) return FEAT_INVALID;
    return static_cast<feat_status_t>(static_cast<int>(svc->svc.cancel(ticket)));
}

/* =========================================================================
 * Payload memory management
 * ========================================================================= */

void feat_payload_release(feat_payload_t* payload)
{
    feat::FeatureService::DeletePayload(
        reinterpret_cast<feat::FeaturePayload*>(payload));
}
