#include "FeatureService.h"
#include <algorithm>
#include <cerrno>
#include <climits>
#include <cfloat>
#include <cstring>

namespace feat {

    // ---------- deleteFeatureInput ----------
    void deleteFeatureInput(void* p) {
        FeatureInput* fi = static_cast<FeatureInput*>(p);
        if (!fi) return;
        if (fi->free_data && fi->data) fi->free_data(fi->data);
        std::free(fi);
    }

    // ---------- deleteFeatureOutput ----------
    void deleteFeatureOutput(void* p) {
        FeatureOutput* fo = static_cast<FeatureOutput*>(p);
        if (!fo) return;
        if (fo->free_data && fo->data) fo->free_data(fo->data);
        delete fo;
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
    void FeatureService::on(const char* tag,
                            FeatureHandlerFn  biz_func,
                            FeatureHandlerDel free_input,
                            FeatureHandlerDel free_output) {
        if (!tag || !biz_func) return;
        std::lock_guard<std::mutex> lk(handlers_mtx_);
        handlers_[tag] = { biz_func, free_input, free_output };
    }

    // ---------- makeFeatureInput ----------
    // Looks up free_input from the on() registration for tag, then heap-allocates
    // a FeatureInput* wrapping (tag, data, free_input).
    void* FeatureService::makeFeatureInput(const char* tag, void* data) {
        if (!tag) return nullptr;

        FeatureHandlerDel free_data = nullptr;
        {
            std::lock_guard<std::mutex> lk(handlers_mtx_);
            auto it = handlers_.find(tag);
            if (it == handlers_.end()) return nullptr;
            free_data = it->second.free_input;
        }

        FeatureInput* fi = static_cast<FeatureInput*>(std::malloc(sizeof(FeatureInput)));
        if (!fi) return nullptr;
        fi->tag       = tag;
        fi->data      = data;
        fi->free_data = free_data;
        return static_cast<void*>(fi);
    }

    // ---------- dispatchServiceEvent ----------
    // Fires TAG_START or TAG_STOP with ticket=0 and no data.
    void FeatureService::dispatchServiceEvent(const char* tag) {
        FeatureOutput* out = new FeatureOutput();
        out->tag       = tag;
        out->ticket    = 0;
        out->status    = Status::Ok;
        out->data      = nullptr;
        out->free_data = nullptr;
        if (cb_) cb_(cb_user_, static_cast<void*>(out));
        else     deleteFeatureOutput(static_cast<void*>(out));
    }

    // ---------- dispatchTask ----------
    // Takes ownership of inp.
    // If status==Ok: calls biz_func(inp->data), wraps actual_output* in FeatureOutput.
    // If status==Cancelled: builds FeatureOutput with null data.
    // Either way: frees inp (including inp->data via inp->free_data), then fires cb_.
    void FeatureService::dispatchTask(FeatureInput* inp, FeatureTicket ticket, Status status) {
        void*             out_data  = nullptr;
        FeatureHandlerDel free_out  = nullptr;

        if (status == Status::Ok) {
            auto it = dispatch_table_.find(inp->tag);
            if (it != dispatch_table_.end()) {
                out_data = it->second.biz_func(inp->data);   // actual_input* -> actual_output*
                free_out = it->second.free_output;
            }
        }

        // Capture tag before freeing inp
        const char*   tag    = inp->tag;
        FeatureTicket tick   = ticket;

        // Service frees the input: free actual_input* then the wrapper
        deleteFeatureInput(static_cast<void*>(inp));

        FeatureOutput* out = new FeatureOutput();
        out->tag       = tag;
        out->ticket    = tick;
        out->status    = status;
        out->data      = out_data;
        out->free_data = free_out;

        if (cb_) cb_(cb_user_, static_cast<void*>(out));
        else     deleteFeatureOutput(static_cast<void*>(out));
    }

    // ---------- start ----------
    Status FeatureService::start() {
        ServiceState expected = ServiceState::NotRunning;
        if (!state_.compare_exchange_strong(expected, ServiceState::Initializing))
            return Status::Ok;  // already Initializing or Running

        stop_flag_.store(false);
        authKeywords_.clear();
        { std::lock_guard<std::mutex> lk(handlers_mtx_); dispatch_table_ = handlers_; }

        Task* initTask = new (std::nothrow) Task();
        if (!initTask) {
            state_.store(ServiceState::NotRunning);
            return Status::Internal;
        }
        initTask->id       = 0;
        initTask->input    = nullptr;
        initTask->internal = true;
        initTask->fn = [this](std::vector<uint8_t>&) {
            // Phase 1: null-guard all libs
            for (size_t i = 0; i < libs_.size(); ++i) {
                if (!libs_[i]) {
                    ServiceState exp = ServiceState::Initializing;
                    if (state_.compare_exchange_strong(exp, ServiceState::NotRunning)) {
                        dispatchServiceEvent(TAG_STOP);
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
                        dispatchServiceEvent(TAG_STOP);
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
            dispatchServiceEvent(TAG_START);
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
                if (!t->internal && t->input)
                    deleteFeatureInput(t->input);
                delete t;
            }
            queue_.clear();
            tasks_.clear();
            current_ = nullptr;
        }
        authKeywords_.clear();

        if (!join) dispatchServiceEvent(TAG_STOP);
        return Status::Ok;
    }

    // ---------- submit ----------
    // On success: service takes ownership of input (FeatureInput*).
    // On failure: returns 0; caller must call deleteFeatureInput(input).
    FeatureTicket FeatureService::submit(void* input) {
        if (!input) return 0;
        FeatureInput* fi = static_cast<FeatureInput*>(input);
        if (!fi->tag) return 0;
        if (state_.load() != ServiceState::Running) return 0;
        if (verifier_ && !verifier_->isAuthorized(authKeywords_)) return 0;

        Task* t = new (std::nothrow) Task();
        if (!t) return 0;

        t->id       = next_ticket_.fetch_add(1);
        t->input    = input;
        t->internal = false;

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
        FeatureInput* inp = nullptr;
        bool pendingCancelled = false;

        {
            std::lock_guard<std::mutex> lk(mtx_);
            auto it = tasks_.find(ticket);
            if (it == tasks_.end()) return Status::NotFound;

            for (std::deque<Task*>::iterator qit = queue_.begin(); qit != queue_.end(); ++qit) {
                if ((*qit)->id == ticket) {
                    Task* t = *qit;
                    inp = static_cast<FeatureInput*>(t->input);
                    queue_.erase(qit);
                    tasks_.erase(ticket);
                    delete t;
                    pendingCancelled = true;
                    break;
                }
            }

            // Not in queue: must be the currently running task; can't interrupt it
            if (!pendingCancelled && current_ != it->second)
                return Status::NotFound;
        }

        // dispatchTask frees inp and fires callback with status=Cancelled
        if (pendingCancelled)
            dispatchTask(inp, ticket, Status::Cancelled);

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
                // Extract before delete so we can call dispatchTask after releasing t
                FeatureInput* inp = static_cast<FeatureInput*>(t->input);
                FeatureTicket id  = t->id;

                // dispatchTask calls biz_func(inp->data), builds {tag, ticket, status, out_data},
                // frees inp, then fires cb_((void*)out)
                dispatchTask(inp, id, Status::Ok);

                {
                    std::lock_guard<std::mutex> lk(mtx_);
                    if (current_ == t) current_ = nullptr;
                    tasks_.erase(id);
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
