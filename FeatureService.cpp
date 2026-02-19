#include "FeatureService.h"
#include <algorithm>
#include <cerrno>
#include <climits>
#include <cfloat>
#include <cstring>

namespace feat {

    // ---------- ctor / dtor ----------
    FeatureService::FeatureService(const FeatureOptions& opts,
                                   const std::vector<IFeatureLib*>& libs,
                                   IFeatureVerifier* verifier)
        : state_(ServiceState::NotRunning)
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
    void FeatureService::on(const char* tag, FeatureHandler handler) {
        if (!tag || !handler) return;
        std::lock_guard<std::mutex> lk(handlers_mtx_);
        handlers_[tag].push_back(std::move(handler));
    }

    // ---------- dispatch ----------
    // Snapshots the handler list under the lock so handlers may safely call on()
    // or stop() without deadlocking. Frees info after all handlers return.
    void FeatureService::dispatch(const char* tag, FeatureTicket ticket,
                                  void* info, void(*free_info)(void*)) {
        std::vector<FeatureHandler> fns;
        {
            std::lock_guard<std::mutex> lk(handlers_mtx_);
            auto it = handlers_.find(tag);
            if (it != handlers_.end()) fns = it->second;
        }

        FeatureEvent ev;
        ev.tag       = tag;
        ev.ticket    = ticket;
        ev.info      = info;
        ev.free_info = free_info;

        for (size_t i = 0; i < fns.size(); ++i) fns[i](ev);

        if (free_info && info) free_info(info);
    }

    // ---------- start ----------
    Status FeatureService::start() {
        ServiceState expected = ServiceState::NotRunning;
        if (!state_.compare_exchange_strong(expected, ServiceState::Initializing))
            return Status::Ok; // already Initializing or Running

        stop_flag_.store(false);
        authKeywords_.clear();

        Task* initTask = new (std::nothrow) Task();
        if (!initTask) {
            state_.store(ServiceState::NotRunning);
            return Status::Internal;
        }
        initTask->id       = 0;
        initTask->internal = true;
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
            for (Task* t : queue_) delete t;
            queue_.clear();
            tasks_.clear();
            current_ = nullptr;
        }
        authKeywords_.clear();

        if (!join) dispatch(TAG_STOP, 0, nullptr, nullptr);
        return Status::Ok;
    }

    // ---------- submit ----------
    FeatureTicket FeatureService::submit(const std::function<void(std::vector<uint8_t>& out)>& fn) {
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

    // ---------- cancel ----------
    Status FeatureService::cancel(FeatureTicket ticket) {
        bool pendingCancelled = false;

        {
            std::lock_guard<std::mutex> lk(mtx_);
            auto it = tasks_.find(ticket);
            if (it == tasks_.end()) return Status::NotFound;

            for (std::deque<Task*>::iterator qit = queue_.begin(); qit != queue_.end(); ++qit) {
                if ((*qit)->id == ticket) {
                    queue_.erase(qit);
                    tasks_.erase(ticket);
                    pendingCancelled = true;
                    break;
                }
            }

            if (!pendingCancelled && current_ != it->second)
                return Status::NotFound;
        }

        // Pending-cancel: notify with TAG_CANCEL (no info payload).
        // Running-cancel: handled by lower libs; worker will dispatch TAG_RESULT when done.
        if (pendingCancelled)
            dispatch(TAG_CANCEL, ticket, nullptr, nullptr);

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

            std::vector<uint8_t> out;
            try { t->fn(out); } catch (...) { out.clear(); }

            if (t->internal) {
                delete t;
            } else {
                // Build FeatureResultInfo and dispatch TAG_RESULT.
                FeatureResultInfo* ri = static_cast<FeatureResultInfo*>(
                    std::malloc(sizeof(FeatureResultInfo)));
                if (ri) {
                    if (!out.empty()) {
                        void* buf = std::malloc(out.size());
                        if (buf) { std::memcpy(buf, out.data(), out.size()); ri->data = buf; ri->size = out.size(); }
                        else     { ri->data = nullptr; ri->size = 0; }
                    } else {
                        ri->data = nullptr; ri->size = 0;
                    }
                }

                void(*free_ri)(void*) = nullptr;
                if (ri) {
                    free_ri = [](void* p) {
                        FeatureResultInfo* r = static_cast<FeatureResultInfo*>(p);
                        if (r->data) std::free(r->data);
                        std::free(r);
                    };
                }

                dispatch(TAG_RESULT, t->id, ri, free_ri);

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
