output/test_integration.o: tests/test_integration.cpp \
  lib/catch2/catch_amalgamated.hpp include/SQLParser.hpp \
  include/Lexer.hpp include/Token.hpp include/Error.hpp \
  include/Parser.hpp include/AST.hpp include/Optimizer.hpp \
  include/JSONSerializer.hpp lib/nlohmann/json.hpp
