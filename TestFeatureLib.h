#ifndef TEST_FEATURE_LIB_H
#define TEST_FEATURE_LIB_H

#include "FeatureService.h"
#include <string>
#include <vector>

namespace feat {

// A minimal IFeatureLib implementation for testing.
//
// Usage:
//   TestFeatureLib lib({"kw_a", "kw_b"});          // always succeeds
//   TestFeatureLib lib({"kw_a"}, /*failInit=*/true); // init() returns false
//
// After a successful init(), initCalled() and opts() let tests verify
// what the service passed in.
class TestFeatureLib : public IFeatureLib {
public:
    explicit TestFeatureLib(const vector<string>& keywords,
                            bool failInit = false)
        : keywords_(keywords)
        , failInit_(failInit)
        , initCalled_(false)
    {}

    // IFeatureLib
    bool init(const str_opt& opts) override {
        initCalled_ = true;
        lastOpts_   = opts;
        return !failInit_;
    }

    vector<string> getKeywords() const override {
        return keywords_;
    }

    void inject(atomic<char>* flag) override { (void)flag; }

    // Returns nullptr by default; tests needing real processing should subclass.
    void* biz(void*) override { return nullptr; }

    // No output allocation, so no deleter needed.
    FeatureHandlerDel outputDeleter() const override { return nullptr; }

    // Test accessors
    bool                initCalled() const { return initCalled_; }
    const str_opt& lastOpts() const { return lastOpts_;   }

private:
    vector<string> keywords_;
    bool                     failInit_;
    bool                     initCalled_;
    str_opt           lastOpts_;
};

} // namespace feat

#endif // TEST_FEATURE_LIB_H
