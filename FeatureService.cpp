#include "FeatureService.h"
#include <algorithm>
#include <cerrno>
#include <climits>
#include <cfloat>
#include <cstring>

namespace feat {

    // ---------- packFeatureParams / freeFeatureParams ----------
    FeatureParams* packFeatureParams(const void* data, size_t size) {
        FeatureParams* p = static_cast<FeatureParams*>(std::malloc(sizeof(FeatureParams)));
        if (!p) return nullptr;
        if (size > 0 && data) {
            p->data = std::malloc(size);
            if (!p->data) { std::free(p); return nullptr; }
            std::memcpy(p->data, data, size);
            p->size = size;
        } else {
            p->data = nullptr;
            p->size = 0;
        }
        return p;
    }

    void freeFeatureParams(void* p) {
        FeatureParams* fp = static_cast<FeatureParams*>(p);
        if (fp) {
            std::free(fp->data);
            std::free(fp);
        }
    }

    // ---------- ctor / dtor ----------
    FeatureService::FeatureService(FeatureCallback cb, void* cb_user,
                                   const FeatureOptions& opts,
                                   const std::vector<IFeatureLib*>& libs,
                                   IFeatureVerifier* verifier)
        : cb_(cb)
        , cb_user_(cb_user)
        , state_(ServiceState::NotRunning)
        , opts_(opts)
        , libs_(libs)
        , verifier_(verifier)
        , next_ticket_(1)
        , stop_flag_(false)
        , current_(nullptr)
    {}

    FeatureService::~FeatureService() {
        stop(true);
    }

    // ---------- on ----------
    void FeatureService::on(const char* tag, FeatureHandlerFn biz_func, FeatureHandlerDel delete_void) {
        if (!tag || !biz_func) return;
        std::lock_guard<std::mutex> lk(handlers_mtx_);
        handlers_[tag] = { biz_func, delete_void };
    }

    // ---------- deleteResult ----------
    void deleteResult(void* p) {
        FeatureResult* r = static_cast<FeatureResult*>(p);
        if (r->del && r->data) r->del(r->data);
        delete r;
    }

    // ---------- deleteFeatureEvent ----------
    void deleteFeatureEvent(void* p) {
        FeatureEvent* ev = static_cast<FeatureEvent*>(p);
        if (ev->free_result && ev->result) ev->free_result(ev->result);
        if (ev->free_user_input && ev->user_input) ev->free_user_input(ev->user_input);
        delete ev;
    }

    // ---------- dispatch ----------
    // If a handler is registered for tag: calls biz_func(user_input) and places the
    // heap result in ev->result as a FeatureResult*.
    // user_input is echoed back in ev->user_input regardless of whether a handler ran.
    // The event takes ownership of both result and user_input; deleteFeatureEvent frees them.
    void FeatureService::dispatch(const char* tag, FeatureTicket ticket,
                                  void* user_input, void(*free_user_input)(void*)) {
        void*  result          = nullptr;
        void (*free_result)(void*) = nullptr;

        auto it = dispatch_table_.find(tag);
        if (it != dispatch_table_.end()) {
            const HandlerEntry& h = it->second;
            FeatureResult* r = new FeatureResult();
            r->data = h.fn(user_input);   // biz_func: transform user_input -> result
            r->del  = h.del;
            result      = r;
            free_result = deleteResult;
        }

        FeatureEvent* ev = new FeatureEvent();
        ev->tag             = tag;
        ev->ticket          = ticket;
        ev->result          = result;
        ev->free_result     = free_result;
        ev->user_input      = user_input;
        ev->free_user_input = free_user_input;

        if (cb_) cb_(cb_user_, ev);
        else     deleteFeatureEvent(ev);
    }

    // ---------- start ----------
    Status FeatureService::start() {
        ServiceState expected = ServiceState::NotRunning;
        if (!state_.compare_exchange_strong(expected, ServiceState::Initializing))
            return Status::Ok; // already Initializing or Running

        stop_flag_.store(false);
        authKeywords_.clear();
        { std::lock_guard<std::mutex> lk(handlers_mtx_); dispatch_table_ = handlers_; }

        Task* initTask = new (std::nothrow) Task();
        if (!initTask) {
            state_.store(ServiceState::NotRunning);
            return Status::Internal;
        }
        initTask->id              = 0;
        initTask->tag             = nullptr;
        initTask->user_input      = nullptr;
        initTask->free_user_input = nullptr;
        initTask->internal        = true;
        initTask->fn = [this](std::vector<uint8_t>&) {
            // Phase 1: null-guard all libs
            for (size_t i = 0; i < libs_.size(); ++i) {
                if (!libs_[i]) {
                    ServiceState exp = ServiceState::Initializing;
                    if (state_.compare_exchange_strong(exp, ServiceState::NotRunning)) {
                        dispatch(TAG_STOP, 0, nullptr, nullptr);
                        stop_flag_.store(true);
                    }
                    return;
                }
            }

            // Phase 2: init all libs, collect keywords
            for (size_t i = 0; i < libs_.size(); ++i) {
                if (stop_flag_.load()) { authKeywords_.clear(); return; }
                if (!libs_[i]->init(opts_)) {
                    ServiceState exp = ServiceState::Initializing;
                    if (state_.compare_exchange_strong(exp, ServiceState::NotRunning)) {
                        authKeywords_.clear();
                        dispatch(TAG_STOP, 0, nullptr, nullptr);
                        stop_flag_.store(true);
                    }
                    return;
                }
                std::vector<std::string> kw = libs_[i]->getKeywords();
                for (size_t j = 0; j < kw.size(); ++j) authKeywords_.push_back(kw[j]);
            }

            if (stop_flag_.load()) { authKeywords_.clear(); return; }

            std::sort(authKeywords_.begin(), authKeywords_.end());
            authKeywords_.erase(std::unique(authKeywords_.begin(), authKeywords_.end()),
                                authKeywords_.end());

            // Phase 3: transition to Running
            ServiceState exp = ServiceState::Initializing;
            if (!state_.compare_exchange_strong(exp, ServiceState::Running)) {
                authKeywords_.clear();
                return;
            }
            dispatch(TAG_START, 0, nullptr, nullptr);
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

        return Status::Ok;
    }

    // ---------- stop ----------
    Status FeatureService::stop(bool join) {
        ServiceState old = state_.load();
        bool wasActive = false;
        for (;;) {
            if (old == ServiceState::NotRunning) break;
            if (state_.compare_exchange_weak(old, ServiceState::NotRunning)) {
                wasActive = true; break;
            }
        }

        stop_flag_.store(true);
        cv_.notify_all();

        if (join && worker_.joinable()) worker_.join();

        if (!wasActive) return Status::Ok;

        {
            std::lock_guard<std::mutex> lk(mtx_);
            for (Task* t : queue_) {
                // Free user_input for cancelled user tasks
                if (!t->internal && t->free_user_input && t->user_input)
                    t->free_user_input(t->user_input);
                delete t;
            }
            queue_.clear();
            tasks_.clear();
            current_ = nullptr;
        }
        authKeywords_.clear();

        if (!join) dispatch(TAG_STOP, 0, nullptr, nullptr);
        return Status::Ok;
    }

    // ---------- submit ----------
    // On success: service takes ownership of params (freed via free_params after callback).
    // On failure: returns 0 and does NOT free params; caller is responsible.
    FeatureTicket FeatureService::submit(const char* tag, void* params, void(*free_params)(void*)) {
        if (!tag) return 0;
        if (state_.load() != ServiceState::Running) return 0;
        if (verifier_ && !verifier_->isAuthorized(authKeywords_)) return 0;

        Task* t = new (std::nothrow) Task();
        if (!t) return 0;

        t->id              = next_ticket_.fetch_add(1);
        t->tag             = tag;
        t->user_input      = params;
        t->free_user_input = free_params;
        t->internal        = false;

        {
            std::lock_guard<std::mutex> lk(mtx_);
            if (stop_flag_.load()) { delete t; return 0; }
            queue_.push_back(t);
            tasks_[t->id] = t;
        }
        cv_.notify_one();
        return t->id;
    }

    // ---------- cancel ----------
    Status FeatureService::cancel(FeatureTicket ticket) {
        void*  ui  = nullptr;
        void (*fui)(void*) = nullptr;
        bool pendingCancelled = false;

        {
            std::lock_guard<std::mutex> lk(mtx_);
            auto it = tasks_.find(ticket);
            if (it == tasks_.end()) return Status::NotFound;

            for (std::deque<Task*>::iterator qit = queue_.begin(); qit != queue_.end(); ++qit) {
                if ((*qit)->id == ticket) {
                    Task* t = *qit;
                    ui  = t->user_input;
                    fui = t->free_user_input;
                    queue_.erase(qit);
                    tasks_.erase(ticket);
                    delete t;
                    pendingCancelled = true;
                    break;
                }
            }

            // If not found in queue, it must be the currently running task
            if (!pendingCancelled && current_ != it->second)
                return Status::NotFound;
        }

        // Pending-cancel: fire TAG_CANCEL with echoed user_input so caller can inspect/free it.
        // Running-cancel: handled by lower libs; worker will dispatch TAG_RESULT when done.
        if (pendingCancelled)
            dispatch(TAG_CANCEL, ticket, ui, fui);

        return Status::Ok;
    }

    // ---------- workerLoop ----------
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

            if (t->internal) {
                std::vector<uint8_t> out;
                try { t->fn(out); } catch (...) {}
                delete t;
            } else {
                // Transfer ownership of user_input to the event before dispatch.
                void*  ui  = t->user_input;
                void (*fui)(void*) = t->free_user_input;
                t->user_input      = nullptr;
                t->free_user_input = nullptr;

                // dispatch calls biz_func(ui) if a handler is registered for t->tag,
                // then fires the callback with event{tag, ticket, result, user_input=ui}.
                dispatch(TAG_RESULT, t->id, ui, fui);

                {
                    std::lock_guard<std::mutex> lk(mtx_);
                    if (current_ == t) current_ = nullptr;
                    tasks_.erase(t->id);
                }
                delete t;
            }
        }
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

    bool FeatureService::parseBool(const std::string& s, bool* ok) {
        std::string t; t.reserve(s.size());
        for (size_t i = 0; i < s.size(); ++i) { char c = s[i]; if (c >= 'A' && c <= 'Z') c = char(c - 'A' + 'a'); t.push_back(c); }
        if (t == "1" || t == "true" || t == "yes" || t == "y") { if (ok)*ok = true; return true; }
        if (t == "0" || t == "false" || t == "no" || t == "n") { if (ok)*ok = true; return false; }
        if (ok)*ok = false; return false;
    }
    int       FeatureService::parseInt(const std::string& s, bool* ok) { char* e = 0; errno = 0; long v = strtol(s.c_str(), &e, 10); if (e == s.c_str() || *e != '\0' || errno == ERANGE || v<INT_MIN || v>INT_MAX) { if (ok)*ok = false; return 0; } if (ok)*ok = true; return (int)v; }
    long long FeatureService::parseLongLong(const std::string& s, bool* ok) { char* e = 0; errno = 0; long long v = strtoll(s.c_str(), &e, 10); if (e == s.c_str() || *e != '\0' || errno == ERANGE) { if (ok)*ok = false; return 0; } if (ok)*ok = true; return v; }
    double    FeatureService::parseDouble(const std::string& s, bool* ok) { char* e = 0; errno = 0; double v = strtod(s.c_str(), &e); if (e == s.c_str() || *e != '\0' || errno == ERANGE) { if (ok)*ok = false; return 0.0; } if (ok)*ok = true; return v; }

    double FeatureService::getDoubleOr(const std::string& key, double fallback) const {
        auto it = opts_.find(key); if (it == opts_.end()) return fallback;
        bool ok = false; double v = parseDouble(it->second, &ok); return ok ? v : fallback;
    }
    long long FeatureService::getLongLongOr(const std::string& key, long long fallback) const {
        auto it = opts_.find(key); if (it == opts_.end()) return fallback;
        bool ok = false; long long v = parseLongLong(it->second, &ok); return ok ? v : fallback;
    }
    int FeatureService::getIntOr(const std::string& key, int fallback) const {
        auto it = opts_.find(key); if (it == opts_.end()) return fallback;
        bool ok = false; int v = parseInt(it->second, &ok); return ok ? v : fallback;
    }
    bool FeatureService::getBoolOr(const std::string& key, bool fallback) const {
        auto it = opts_.find(key); if (it == opts_.end()) return fallback;
        bool ok = false; bool v = parseBool(it->second, &ok); return ok ? v : fallback;
    }

} // namespace feat
