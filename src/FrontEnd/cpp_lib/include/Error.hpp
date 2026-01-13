#pragma once
#include <string>
#include <vector>

struct SourceLocation {
    size_t line{1};
    size_t column{1};
};

struct ParseError {
    std::string message;
    SourceLocation location;
    std::string snippet;

    std::string format() const {
        std::string result = "Error at line " + std::to_string(location.line) +
                             ", column " + std::to_string(location.column) + ": " + message;
        if (!snippet.empty()) {
            result += "\n  " + snippet;
        }
        return result;
    }
};

class ErrorReporter {
public:
    void report(ParseError error) { errors_.push_back(std::move(error)); }
    [[nodiscard]] bool hasErrors() const { return !errors_.empty(); }
    [[nodiscard]] const std::vector<ParseError>& errors() const { return errors_; }
    void clear() { errors_.clear(); }

    std::string formatAll() const {
        std::string result;
        for (const auto& err : errors_) {
            if (!result.empty()) result += "\n";
            result += err.format();
        }
        return result;
    }

private:
    std::vector<ParseError> errors_;
};
