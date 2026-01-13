CXX = clang++-20
CXXFLAGS := -std=c++23 -stdlib=libc++ -Wall -Wextra -g
LFLAGS = -stdlib=libc++

OUTPUT := output
SRC := src
INCLUDE := include
LIB := lib
TESTS := tests

INCLUDES := -I$(INCLUDE) -I$(LIB)

MAIN_SOURCES := $(wildcard $(SRC)/*.cpp)
MAIN_OBJECTS := $(patsubst $(SRC)/%.cpp,$(OUTPUT)/%.o,$(MAIN_SOURCES))

LIB_SOURCES := $(filter-out $(SRC)/main.cpp $(SRC)/bindings.cpp,$(MAIN_SOURCES))
LIB_OBJECTS := $(patsubst $(SRC)/%.cpp,$(OUTPUT)/%.o,$(LIB_SOURCES))

TEST_SOURCES := $(wildcard $(TESTS)/*.cpp)
TEST_OBJECTS := $(patsubst $(TESTS)/%.cpp,$(OUTPUT)/%.o,$(TEST_SOURCES))

CATCH_OBJ := $(OUTPUT)/catch_amalgamated.o

MAIN := main
TEST_RUNNER := test_runner

PYTHON_CONFIG := python3-config
PYTHON_INCLUDES := $(shell $(PYTHON_CONFIG) --includes)
PYTHON_LDFLAGS := $(shell $(PYTHON_CONFIG) --ldflags --embed 2>/dev/null || $(PYTHON_CONFIG) --ldflags)
PYTHON_EXT_SUFFIX := $(shell $(PYTHON_CONFIG) --extension-suffix)
PYTHON_MODULE := optisql_parser$(PYTHON_EXT_SUFFIX)

.PHONY: all clean test test-verbose run python

all: $(OUTPUT) $(OUTPUT)/$(MAIN)
	@echo "Build complete!"

$(OUTPUT):
	mkdir -p $(OUTPUT)

$(OUTPUT)/$(MAIN): $(MAIN_OBJECTS) | $(OUTPUT)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -o $@ $^ $(LFLAGS)

$(OUTPUT)/%.o: $(SRC)/%.cpp | $(OUTPUT)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c -MMD $< -o $@

$(OUTPUT)/%.o: $(TESTS)/%.cpp | $(OUTPUT)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c -MMD $< -o $@

$(CATCH_OBJ): $(LIB)/catch2/catch_amalgamated.cpp | $(OUTPUT)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

test: $(OUTPUT)/$(TEST_RUNNER)
	./$(OUTPUT)/$(TEST_RUNNER)

test-verbose: $(OUTPUT)/$(TEST_RUNNER)
	./$(OUTPUT)/$(TEST_RUNNER) --success

$(OUTPUT)/$(TEST_RUNNER): $(LIB_OBJECTS) $(TEST_OBJECTS) $(CATCH_OBJ) | $(OUTPUT)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -o $@ $^ $(LFLAGS)

$(OUTPUT)/bindings.o: $(SRC)/bindings.cpp | $(OUTPUT)
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(PYTHON_INCLUDES) -fPIC -c $< -o $@

$(OUTPUT)/%.pic.o: $(SRC)/%.cpp | $(OUTPUT)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -fPIC -c $< -o $@

LIB_PIC_OBJECTS := $(patsubst $(SRC)/%.cpp,$(OUTPUT)/%.pic.o,$(LIB_SOURCES))

python: $(OUTPUT)/$(PYTHON_MODULE)
	@echo "Python module built: $(OUTPUT)/$(PYTHON_MODULE)"

check-python:
	@which python3-config > /dev/null 2>&1 || (echo "Error: python3-config not found. Install python3-dev package." && exit 1)

$(OUTPUT)/$(PYTHON_MODULE): $(OUTPUT)/bindings.o $(LIB_PIC_OBJECTS) | $(OUTPUT)
	$(CXX) $(CXXFLAGS) -shared -fPIC $(INCLUDES) -o $@ $^ $(LFLAGS) $(PYTHON_LDFLAGS)

clean:
	rm -rf $(OUTPUT)
	@echo "Cleanup complete!"

run: all
	./$(OUTPUT)/$(MAIN)

compdb:
	bear -- make clean
	bear -- make all
	@echo "compile_commands.json updated!"

-include $(OUTPUT)/*.d
