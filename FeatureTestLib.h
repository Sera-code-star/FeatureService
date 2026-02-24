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
//   TestLibInput Raw{ Value, "Label" };
//   void* Data = MakeFeatureInput(&Raw, sizeof(Raw));
//   void* Evt  = MakeInputEvt("test", Data);
//   FeatureTicket T = Svc.Submit(Evt);
//
// Callback receives FeatureOutput::Data as TestLibOutput*.

struct TestLibInput {
    int  Value;       // operand: Biz() returns Value * 2
    char Label[64];   // pass-through identifier, echoed in output
};

struct TestLibOutput {
    int  Result;      // Value * 2, or 0 if aborted
    char Label[64];   // echoed from input
    bool Aborted;     // true when ExitFlag fired before Biz() finished
};

// ─── FeatureTestLib ──────────────────────────────────────────────────────────
//
// Concrete IFeatureLib that processes TestLibInput → TestLibOutput.
//
// Biz() doubles Input.Value, echoes Input.Label, and polls ExitFlag so
// cancellation works correctly.  OutputDeleter() returns free so the
// service can release the heap-allocated TestLibOutput.
//
// Introspection helpers let unit-tests verify service behaviour:
//   Lib.InitCalled()   – whether the service called Init()
//   Lib.LastOpts()     – str_opt the service passed to Init()
//   Lib.BizCallCount() – number of completed Biz() calls

class FeatureTestLib : public IFeatureLib {
public:
    // Keywords: tags this lib claims; defaults to {"test"}
    // FailInit:  if true, Init() returns false (simulates broken lib)
    explicit FeatureTestLib(vector<string> Keywords = {"test"},
                            bool FailInit = false)
        : _Keywords(move(Keywords))
        , _FailInit(FailInit)
        , _ExitFlag(nullptr)
        , _InitCalled(false)
        , _BizCallCount(0)
    {}

    // ── IFeatureLib ──────────────────────────────────────────────────────────

    bool Init(const str_opt& Opts) override {
        _InitCalled = true;
        _LastOpts   = Opts;
        return !_FailInit;
    }

    vector<string> GetKeywords() const override {
        return _Keywords;
    }

    // Service calls Inject(&Task->ExitFlag) before Biz() and Inject(nullptr) after.
    void Inject(atomic<char>* Flag) override {
        _ExitFlag = Flag;
    }

    // actual_input*  → TestLibInput*
    // actual_output* → heap-allocated TestLibOutput* (freed by OutputDeleter)
    void* Biz(void* Input) override {
        ++_BizCallCount;

        auto* Out = static_cast<TestLibOutput*>(malloc(sizeof(TestLibOutput)));
        if (!Out) return nullptr;

        // Early cancellation: ExitFlag already raised before we start
        if (_ExitFlag && _ExitFlag->load(memory_order_relaxed) != 0) {
            Out->Result   = 0;
            Out->Aborted  = true;
            Out->Label[0] = '\0';
            return Out;
        }

        auto* In = static_cast<TestLibInput*>(Input);
        Out->Result  = In->Value * 2;
        Out->Aborted = false;
        strncpy(Out->Label, In->Label, sizeof(Out->Label) - 1);
        Out->Label[sizeof(Out->Label) - 1] = '\0';

        return Out;
    }

    // Captureless lambda converts to FeatureHandlerDel (void(*)(void*))
    FeatureHandlerDel OutputDeleter() const override {
        return [](void* P) { free(P); };
    }

    // ── Test introspection ───────────────────────────────────────────────────
    bool                  InitCalled()   const { return _InitCalled;   }
    const str_opt&        LastOpts()     const { return _LastOpts;     }
    int                   BizCallCount() const { return _BizCallCount; }

private:
    vector<string>    _Keywords;
    bool              _FailInit;
    atomic<char>*     _ExitFlag;    // pointer lent by the service; not owned
    bool              _InitCalled;
    int               _BizCallCount;
    str_opt           _LastOpts;
};

} // namespace feat

#endif // FEATURE_TEST_LIB_H
