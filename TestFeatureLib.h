#ifndef TEST_FEATURE_LIB_H
#define TEST_FEATURE_LIB_H

#include "FeatureService.h"
#include <string>
#include <vector>

namespace feat {

// A minimal IFeatureLib implementation for testing.
//
// Usage:
//   TestFeatureLib Lib({"kw_a", "kw_b"});           // always succeeds
//   TestFeatureLib Lib({"kw_a"}, /*FailInit=*/true); // Init() returns false
//
// After a successful Init(), InitCalled() and LastOpts() let tests verify
// what the service passed in.
class TestFeatureLib : public IFeatureLib {
public:
    explicit TestFeatureLib(const vector<string>& Keywords,
                            bool FailInit = false)
        : _Keywords(Keywords)
        , _FailInit(FailInit)
        , _InitCalled(false)
    {}

    // IFeatureLib
    bool Init(const str_opt& Opts) override {
        _InitCalled = true;
        _LastOpts   = Opts;
        return !_FailInit;
    }

    vector<string> GetKeywords() const override {
        return _Keywords;
    }

    void Inject(atomic<char>* Flag) override { (void)Flag; }

    // Returns nullptr by default; tests needing real processing should subclass.
    void* Biz(void*) override { return nullptr; }

    // No output allocation, so no deleter needed.
    FeatureHandlerDel OutputDeleter() const override { return nullptr; }

    // Test accessors
    bool           InitCalled() const { return _InitCalled; }
    const str_opt& LastOpts()   const { return _LastOpts;   }

private:
    vector<string> _Keywords;
    bool           _FailInit;
    bool           _InitCalled;
    str_opt        _LastOpts;
};

} // namespace feat

#endif // TEST_FEATURE_LIB_H
