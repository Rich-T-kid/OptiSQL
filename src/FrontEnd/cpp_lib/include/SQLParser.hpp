#pragma once

#include "Lexer.hpp"
#include "Parser.hpp"
#include "Optimizer.hpp"
#include "JSONSerializer.hpp"
#include <string>
#include <expected>

namespace optisql {

struct ParseResult {
    json ast;
    bool optimized{false};
};

class SQLParser {
public:
    std::expected<ParseResult, ParseError> parse(const std::string& sql, bool optimize = true);
    std::string parseToJson(const std::string& sql, bool optimize = true);

private:
    Lexer lexer_{""};
    Parser parser_{{}};
    Optimizer optimizer_;
    JSONSerializer serializer_;
};

} // namespace optisql
