#include "FeatureService.h"
#include <algorithm>
#include <cerrno>
#include <climits>
#include <cfloat>
#include <cstring>

namespace feat {

    // ---------- static members ----------
    std::mutex FeatureService::s_deleter_mtx_;
    std::unordered_map<std::string, FeaturePayloadDeleter> FeatureService::s_deleters_;

    // ---------- ctor / dtor ----------
    FeatureService::FeatureService(FeatureCallback cb, void* user, const FeatureOptions& opts,
        const std::vector<IFeatureLib*>& libs, IFeatureVerifier* verifier)
        : cb_(cb)
        , cb_user_(user)
        , state_(ServiceState::NotRunning)
        , opts_(opts)
        , libs_(libs)
        , verifier_(verifier)
        , next_ticket_(1)
        , stop_flag_(false)
        , current_(nullptr)
    {
        // Ensure built-in deleters are present (idempotent)
        RegisterPayloadDeleter(TAG_FEAT_RUNNING_V1, &FeatureService::DefaultFree);
        RegisterPayloadDeleter(TAG_FEAT_RESULT_V1, &FeatureService::ResultFree);
    }

    FeatureService::~FeatureService() {
        stopSync(); // must join before members are destroyed
    }

    // ---------- lifecycle ----------
    Status FeatureService::start() {
        ServiceState expected = ServiceState::NotRunning;
        if (!state_.compare_exchange_strong(expected, ServiceState::Initializing)) {
            return Status::Ok; // already Initializing or Running
        }

        stop_flag_.store(false);
        authKeywords_.clear();

        // Wrap the full lib init procedure in one internal task:
        //   Phase 1 — verify libs  |  Phase 2 — init all libs  |  Phase 3 — emit cb
        Task* initTask = new (std::nothrow) Task();
        if (!initTask) {
            state_.store(ServiceState::NotRunning);
            return Status::Internal;
        }
        initTask->id       = 0;
        initTask->internal = true;
        initTask->fn = [this](std::vector<uint8_t>&) {
            // ---- Phase 1: verify libs (null guard before touching any lib) ----
            for (size_t i = 0; i < libs_.size(); ++i) {
                if (!libs_[i]) {
                    ServiceState exp = ServiceState::Initializing;
                    if (state_.compare_exchange_strong(exp, ServiceState::NotRunning)) {
                        emitRunningEvent(false); // null lib — cannot proceed
                        stop_flag_.store(true);
                    }
                    return;
                }
            }

            // ---- Phase 2: init all libs, collect keywords ----
            for (size_t i = 0; i < libs_.size(); ++i) {
                if (stop_flag_.load()) { authKeywords_.clear(); return; }
                if (!libs_[i]->init(opts_)) {
                    ServiceState exp = ServiceState::Initializing;
                    if (state_.compare_exchange_strong(exp, ServiceState::NotRunning)) {
                        authKeywords_.clear();
                        emitRunningEvent(false); // lib init failed
                        stop_flag_.store(true);
                    }
                    return;
                }
                std::vector<std::string> kw = libs_[i]->getKeywords();
                for (size_t j = 0; j < kw.size(); ++j)
                    authKeywords_.push_back(kw[j]);
            }

            if (stop_flag_.load()) { authKeywords_.clear(); return; }

            std::sort(authKeywords_.begin(), authKeywords_.end());
            authKeywords_.erase(std::unique(authKeywords_.begin(), authKeywords_.end()),
                                authKeywords_.end());

            // ---- Phase 3: emit callback — Running or NotRunning ----
            ServiceState exp = ServiceState::Initializing;
            if (!state_.compare_exchange_strong(exp, ServiceState::Running)) {
                authKeywords_.clear(); // stop() already set NotRunning
                return;
            }
            emitRunningEvent(true); // all libs ready — RUNNING
        };

        {
            std::lock_guard<std::mutex> lk(mtx_);
            queue_.push_back(initTask);
        }

        try {
            worker_ = std::thread(&FeatureService::workerLoop, this);
        }
        catch (...) {
            std::lock_guard<std::mutex> lk(mtx_);
            for (Task* qt : queue_) delete qt;
            queue_.clear();
            state_.store(ServiceState::NotRunning);
            return Status::Internal;
        }

        return Status::Ok; // worker will emit Running once the init task completes
    }

    // ---------- shared stop implementation ----------
    // Signals the worker to stop and performs queue/callback cleanup.
    // sync=true: also blocks until the worker thread exits (stopSync).
    // sync=false: returns immediately after signalling (stop); worker
    //             finishes its current task in the background and then exits.
    Status FeatureService::doStop(bool sync) {
        // CAS loop: transition Running or Initializing → NotRunning.
        ServiceState old = state_.load();
        bool wasActive = false;
        for (;;) {
            if (old == ServiceState::NotRunning) break;
            if (state_.compare_exchange_weak(old, ServiceState::NotRunning)) {
                wasActive = true; break;
            }
        }

        // Signal the worker.
        stop_flag_.store(true);
        cv_.notify_all();

        // Sync variant: wait for the worker to finish before cleanup.
        // Also handles the case where the worker self-exited after an init failure.
        if (sync && worker_.joinable()) worker_.join();

        if (!wasActive) return Status::Ok;

        // --- shared: drain queue, reset tracking, emit NotRunning callback ---
        {
            std::lock_guard<std::mutex> lk(mtx_);
            for (Task* t : queue_) delete t;
            queue_.clear();
            tasks_.clear();
            current_ = nullptr;
        }
        authKeywords_.clear();
        emitRunningEvent(false); // STOPPED
        return Status::Ok;
    }

    // Async stop: signal + cleanup; worker may still be finishing its current task.
    Status FeatureService::stop()     { return doStop(false); }

    // Sync stop: same as stop() but blocks until the worker thread exits.
    Status FeatureService::stopSync() { return doStop(true);  }

    // ---------- submit ----------
    FeatureTicket FeatureService::submit(const std::function<void(std::vector<uint8_t>& out)>& fn)
    {
        if (!fn) return 0;
        if (state_.load() != ServiceState::Running) return 0;
        if (verifier_ && !verifier_->isAuthorized(authKeywords_)) return 0;

        Task* t = new (std::nothrow) Task();
        if (!t) return 0;

        t->id = next_ticket_.fetch_add(1);
        t->fn = fn;

        {
            std::lock_guard<std::mutex> lk(mtx_);
            if (stop_flag_.load()) { delete t; return 0; }
            queue_.push_back(t);
            tasks_[t->id] = t;
        }
        cv_.notify_one();
        return t->id;
    }

    // ---------- cancel(ticket) ----------
    // Pending -> remove {queue,tasks}, delete t, EMIT empty result now.
    // Running -> DO NOT EMIT. Native cancel is handled by lower libs. Worker will emit later from fn(out).
    // Done/unknown -> NotFound.
    Status FeatureService::cancel(FeatureTicket ticket) {
        Task* t = nullptr;
        bool emitEmpty = false;

        {
            std::lock_guard<std::mutex> lk(mtx_);
            auto it = tasks_.find(ticket);
            if (it == tasks_.end()) return Status::NotFound;
            t = it->second;

            // Pending? (present in queue_)
            for (std::deque<Task*>::iterator qit = queue_.begin(); qit != queue_.end(); ++qit) {
                if ((*qit)->id == ticket) {
                    queue_.erase(qit);
                    tasks_.erase(ticket);
                    emitEmpty = true;
                    break;
                }
            }

            if (!emitEmpty) {
                // Running? (current_ == t)
                if (current_ != it->second) {
                    // Not in queue and not current_: likely already finished
                    return Status::NotFound;
                }
            }
        }

        // Emit terminal "done" (empty) only for pending cancels.
        if (emitEmpty) {
            emitResult(ticket, NULL, 0);
        }

        return Status::Ok;
    }

    // ---------- worker (single thread) ----------
    // Processes all tasks (internal and normal) from the queue.
    // The first task is always the internal init task enqueued by start().
    void FeatureService::workerLoop() {
        for (;;) {
            Task* t = nullptr;
            {
                std::unique_lock<std::mutex> lk(mtx_);
                cv_.wait(lk, [this] { return stop_flag_.load() || !queue_.empty(); });
                if (stop_flag_.load() && queue_.empty()) return;
                t = queue_.front(); queue_.pop_front();
                if (!t->internal) current_ = t;
            }

            std::vector<uint8_t> out;
            try {
                t->fn(out);
            }
            catch (...) {
                out.clear();
            }

            if (t->internal) {
                // Init task: no result emitted; not tracked in tasks_.
                // If it set stop_flag_, the next loop iteration will exit.
                delete t;
            } else {
                // Normal task: emit the single terminal result.
                emitResult(t->id, out.empty() ? NULL : out.data(), out.size());
                {
                    std::lock_guard<std::mutex> lk(mtx_);
                    if (current_ == t) current_ = nullptr;
                    tasks_.erase(t->id);
                }
                delete t;
            }
        }
    }

    // ---------- events ----------
    FeaturePayload* FeatureService::makeRunningPayload(bool running) {
        FeaturePayload* p = static_cast<FeaturePayload*>(std::malloc(sizeof(FeaturePayload)));
        if (!p) return 0;

        FeatureRunningData* d = static_cast<FeatureRunningData*>(std::malloc(sizeof(FeatureRunningData)));
        if (!d) { std::free(p); return 0; }

        std::memcpy(p->tag, TAG_FEAT_RUNNING_V1, 8);
        d->state = running ? FeatureRunningState::Running : FeatureRunningState::NotRunning;

        p->data = d;
        return p;
    }

    void FeatureService::emitRunningEvent(bool running) {
        if (!cb_) return;
        FeaturePayload* pl = makeRunningPayload(running);
        if (!pl) return; // OOM: drop silently
        cb_(cb_user_, pl); // user must release via DeletePayload()
    }

    void FeatureService::emitResult(FeatureTicket ticket, const void* data_ptr, size_t size) {
        if (!cb_) return;

        FeaturePayload* p = static_cast<FeaturePayload*>(std::malloc(sizeof(FeaturePayload)));
        if (!p) return;

        FeatureResultData* d = static_cast<FeatureResultData*>(std::malloc(sizeof(FeatureResultData)));
        if (!d) { std::free(p); return; }

        std::memcpy(p->tag, TAG_FEAT_RESULT_V1, 8);
        d->ticket = ticket;

        if (data_ptr && size) {
            void* buf = std::malloc(size);
            if (!buf) {
                d->data = NULL; d->size = 0;
            }
            else {
                std::memcpy(buf, data_ptr, size);
                d->data = buf;
                d->size = size;
            }
        }
        else {
            d->data = NULL;
            d->size = 0;
        }

        p->data = d;
        cb_(cb_user_, p); // user must release via DeletePayload()
    }

    // ---------- class-scoped static deleters ----------
    void FeatureService::DefaultFree(FeaturePayload* payload) {
        if (!payload) return;
        if (payload->data) {
            std::free(payload->data);
            payload->data = 0;
        }
        std::free(payload);
    }

    void FeatureService::ResultFree(FeaturePayload* payload) {
        if (!payload) return;
        if (payload->data) {
            FeatureResultData* d = static_cast<FeatureResultData*>(payload->data);
            if (d->data) std::free(d->data);
            std::free(d);
        }
        std::free(payload);
    }

    Status FeatureService::RegisterPayloadDeleter(const char tag8[8], FeaturePayloadDeleter d) {
        if (!tag8 || !d) return Status::Invalid;
        std::lock_guard<std::mutex> lk(s_deleter_mtx_);
        s_deleters_[std::string(tag8, tag8 + 8)] = d;
        return Status::Ok;
    }

    void FeatureService::DeletePayload(FeaturePayload* payload) {
        if (!payload) return;

        FeaturePayloadDeleter fn = nullptr;
        {
            std::lock_guard<std::mutex> lk(s_deleter_mtx_);
            std::string key(payload->tag, payload->tag + 8);
            std::unordered_map<std::string, FeaturePayloadDeleter>::iterator it = s_deleters_.find(key);
            if (it != s_deleters_.end()) fn = it->second;
        }
        if (fn) { fn(payload); return; }
        DefaultFree(payload);
    }

    // ---------- options ----------
    bool FeatureService::hasOption(const std::string& key) const {
        return opts_.find(key) != opts_.end();
    }
    std::string FeatureService::getOption(const std::string& key) const {
        auto it = opts_.find(key);
        if (it == opts_.end()) return std::string();
        return it->second;
    }
    std::string FeatureService::getOptionOr(const std::string& key, const std::string& fallback) const {
        auto it = opts_.find(key);
        if (it == opts_.end()) return fallback;
        return it->second;
    }

    // Parsers (C++11-friendly)
    bool FeatureService::parseBool(const std::string& s, bool* ok) {
        std::string t; t.reserve(s.size());
        for (size_t i = 0; i < s.size(); ++i) { char c = s[i]; if (c >= 'A' && c <= 'Z') c = char(c - 'A' + 'a'); t.push_back(c); }
        if (t == "1" || t == "true" || t == "yes" || t == "y") { if (ok)*ok = true; return true; }
        if (t == "0" || t == "false" || t == "no" || t == "n") { if (ok)*ok = true; return false; }
        if (ok)*ok = false; return false;
    }
    int        FeatureService::parseInt(const std::string& s, bool* ok) { char* e = 0; errno = 0; long v = strtol(s.c_str(), &e, 10); if (e == s.c_str() || *e != '\0' || errno == ERANGE || v<INT_MIN || v>INT_MAX) { if (ok)*ok = false; return 0; } if (ok)*ok = true; return (int)v; }
    long long  FeatureService::parseLongLong(const std::string& s, bool* ok) { char* e = 0; errno = 0; long long v = strtoll(s.c_str(), &e, 10); if (e == s.c_str() || *e != '\0' || errno == ERANGE) { if (ok)*ok = false; return 0; } if (ok)*ok = true; return v; }
    double     FeatureService::parseDouble(const std::string& s, bool* ok) { char* e = 0; errno = 0; double v = strtod(s.c_str(), &e); if (e == s.c_str() || *e != '\0' || errno == ERANGE) { if (ok)*ok = false; return 0.0; } if (ok)*ok = true; return v; }

    double FeatureService::getDoubleOr(const std::string& key, double fallback) const {
        auto it = opts_.find(key);
        if (it == opts_.end()) return fallback;
        bool ok = false; double v = parseDouble(it->second, &ok);
        return ok ? v : fallback;
    }
    long long FeatureService::getLongLongOr(const std::string& key, long long fallback) const {
        auto it = opts_.find(key);
        if (it == opts_.end()) return fallback;
        bool ok = false; long long v = parseLongLong(it->second, &ok);
        return ok ? v : fallback;
    }
    int FeatureService::getIntOr(const std::string& key, int fallback) const {
        auto it = opts_.find(key);
        if (it == opts_.end()) return fallback;
        bool ok = false; int v = parseInt(it->second, &ok);
        return ok ? v : fallback;
    }
    bool FeatureService::getBoolOr(const std::string& key, bool fallback) const {
        auto it = opts_.find(key);
        if (it == opts_.end()) return fallback;
        bool ok = false; bool v = parseBool(it->second, &ok);
        return ok ? v : fallback;
    }

} // namespace feat