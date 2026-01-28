.PHONY: help setup test grpc-gen cpp-parser clean run

PROTO_DIR := ../Contract
GRPC_OUT := app/grpc_gen
CPP_LIB := cpp_lib
CONDA_ENV := optisql

help:
	@echo "Available targets:"
	@echo "  make setup      - Setup conda environment and install dependencies"
	@echo "  make test       - Run Python tests"
	@echo "  make grpc-gen   - Generate gRPC Python code from proto files"
	@echo "  make cpp-parser - Build the C++ parser Python module"
	@echo "  make clean      - Clean generated files"
	@echo "  make run        - Run the FastAPI server"
	@echo "  make all        - Setup, build C++ parser, generate gRPC, and run tests"

setup:
	@echo "Setting up conda environment..."
	@conda create -n $(CONDA_ENV) python=3.10 -y || true
	@echo "Installing dependencies..."
	conda run -n $(CONDA_ENV) pip install --upgrade pip
	conda run -n $(CONDA_ENV) pip install -r requirements.txt
	@echo "Setup complete! Activate with: conda activate $(CONDA_ENV)"

test:
	@echo "Running Python tests..."
	conda run -n $(CONDA_ENV) pytest -v

grpc-gen:
	@echo "Generating gRPC Python code..."
	@mkdir -p $(GRPC_OUT)
	conda run -n $(CONDA_ENV) python -m grpc_tools.protoc \
		-I$(PROTO_DIR) \
		--python_out=$(GRPC_OUT) \
		--grpc_python_out=$(GRPC_OUT) \
		$(PROTO_DIR)/operation.proto
	@echo "Fixing imports in generated files..."
	@sed -i 's/import operation_pb2/from app.grpc_gen import operation_pb2/' $(GRPC_OUT)/operation_pb2_grpc.py 2>/dev/null || \
		sed -i '' 's/import operation_pb2/from app.grpc_gen import operation_pb2/' $(GRPC_OUT)/operation_pb2_grpc.py
	@echo "gRPC code generated in $(GRPC_OUT)"

cpp-parser:
	@echo "Building C++ parser Python module..."
	$(MAKE) -C $(CPP_LIB) python

cpp-test:
	@echo "Running C++ parser tests..."
	$(MAKE) -C $(CPP_LIB) test

clean:
	@echo "Cleaning generated files..."
	rm -f $(GRPC_OUT)/operation_pb2.py $(GRPC_OUT)/operation_pb2_grpc.py
	$(MAKE) -C $(CPP_LIB) clean
	@echo "Clean complete!"

run:
	@echo "Starting FastAPI server..."
	conda run -n $(CONDA_ENV) uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

all: setup cpp-parser grpc-gen test
	@echo "All setup complete!"
