#ifndef FEATURE_TEST_LIB_H
#define FEATURE_TEST_LIB_H

#include "FeatureService.h"
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

namespace feat {

// ─── Wire types ──────────────────────────────────────────────────────────────
//
// Caller workflow:
//   TestLibInput raw{ value, "label" };
//   void* data = makeFeatureInput(&raw, sizeof(raw));
//   void* evt  = makeInputEvt("test", data);
//   FeatureTicket t = svc.submit(evt);
//
// Callback receives FeatureOutput::data as TestLibOutput*.

struct TestLibInput {
    int  value;       // operand: biz() returns value * 2
    char label[64];   // pass-through identifier, echoed in output
};

struct TestLibOutput {
    int  result;      // value * 2, or 0 if aborted
    char label[64];   // echoed from input
    bool aborted;     // true when exitFlag fired before biz() finished
};

// ─── FeatureTestLib ──────────────────────────────────────────────────────────
//
// Concrete IFeatureLib that processes TestLibInput → TestLibOutput.
//
// biz() doubles input.value, echoes input.label, and polls exitFlag so
// cancellation works correctly.  outputDeleter() returns std::free so the
// service can release the heap-allocated TestLibOutput.
//
// Introspection helpers let unit-tests verify service behaviour:
//   lib.initCalled()   – whether the service called init()
//   lib.lastOpts()     – FeatureOptions the service passed to init()
//   lib.bizCallCount() – number of completed biz() calls

class FeatureTestLib : public IFeatureLib {
public:
    // keywords: tags this lib claims; defaults to {"test"}
    // failInit:  if true, init() returns false (simulates broken lib)
    explicit FeatureTestLib(std::vector<std::string> keywords = {"test"},
                            bool failInit = false)
        : keywords_(std::move(keywords))
        , failInit_(failInit)
        , exitFlag_(nullptr)
        , initCalled_(false)
        , bizCallCount_(0)
    {}

    // ── IFeatureLib ──────────────────────────────────────────────────────────

    bool init(const FeatureOptions& opts) override {
        initCalled_ = true;
        lastOpts_   = opts;
        return !failInit_;
    }

    std::vector<std::string> getKeywords() const override {
        return keywords_;
    }

    // Service calls inject(&task->exitFlag) before biz() and inject(nullptr) after.
    void inject(std::atomic<char>* flag) override {
        exitFlag_ = flag;
    }

    // actual_input*  → TestLibInput*
    // actual_output* → heap-allocated TestLibOutput* (freed by outputDeleter)
    void* biz(void* input) override {
        ++bizCallCount_;

        auto* out = static_cast<TestLibOutput*>(std::malloc(sizeof(TestLibOutput)));
        if (!out) return nullptr;

        // Early cancellation: exitFlag already raised before we start
        if (exitFlag_ && exitFlag_->load(std::memory_order_relaxed) != 0) {
            out->result   = 0;
            out->aborted  = true;
            out->label[0] = '\0';
            return out;
        }

        auto* in = static_cast<TestLibInput*>(input);
        out->result  = in->value * 2;
        out->aborted = false;
        std::strncpy(out->label, in->label, sizeof(out->label) - 1);
        out->label[sizeof(out->label) - 1] = '\0';

        return out;
    }

    // Captureless lambda converts to FeatureHandlerDel (void(*)(void*))
    FeatureHandlerDel outputDeleter() const override {
        return [](void* p) { std::free(p); };
    }

    // ── Test introspection ───────────────────────────────────────────────────
    bool                  initCalled()   const { return initCalled_;   }
    const FeatureOptions& lastOpts()     const { return lastOpts_;     }
    int                   bizCallCount() const { return bizCallCount_; }

private:
    std::vector<std::string> keywords_;
    bool                     failInit_;
    std::atomic<char>*       exitFlag_;    // pointer lent by the service; not owned
    bool                     initCalled_;
    int                      bizCallCount_;
    FeatureOptions           lastOpts_;
};

} // namespace feat

#endif // FEATURE_TEST_LIB_H
