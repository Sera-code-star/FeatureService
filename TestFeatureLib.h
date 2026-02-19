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
    explicit TestFeatureLib(const std::vector<std::string>& keywords,
                            bool failInit = false)
        : keywords_(keywords)
        , failInit_(failInit)
        , initCalled_(false)
    {}

    // IFeatureLib
    bool init(const FeatureOptions& opts) override {
        initCalled_ = true;
        lastOpts_   = opts;
        return !failInit_;
    }

    std::vector<std::string> getKeywords() const override {
        return keywords_;
    }

    // Test accessors
    bool                initCalled() const { return initCalled_; }
    const FeatureOptions& lastOpts() const { return lastOpts_;   }

private:
    std::vector<std::string> keywords_;
    bool                     failInit_;
    bool                     initCalled_;
    FeatureOptions           lastOpts_;
};

} // namespace feat

#endif // TEST_FEATURE_LIB_H
