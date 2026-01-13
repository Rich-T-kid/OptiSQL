output/SQLParser.o: src/SQLParser.cpp include/SQLParser.hpp \
  include/Lexer.hpp include/Token.hpp include/Error.hpp \
  include/Parser.hpp include/AST.hpp include/Optimizer.hpp \
  include/JSONSerializer.hpp lib/nlohmann/json.hpp
