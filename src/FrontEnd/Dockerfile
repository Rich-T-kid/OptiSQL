FROM python:3.10-slim

WORKDIR /app

# Install system dependencies for gRPC and C++ runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    libc++1 \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY FrontEnd/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy proto files and generate gRPC code
COPY Contract /contract
RUN mkdir -p app/grpc_gen && \
    python -m grpc_tools.protoc \
        -I/contract \
        --python_out=app/grpc_gen \
        --grpc_python_out=app/grpc_gen \
        /contract/operation.proto && \
    sed -i 's/import operation_pb2/from app.grpc_gen import operation_pb2/' app/grpc_gen/operation_pb2_grpc.py && \
    touch app/grpc_gen/__init__.py

# Copy application code
COPY FrontEnd/app ./app

# Copy pre-built C++ parser module
COPY FrontEnd/cpp_lib/output/optisql_parser.cpython-310-x86_64-linux-gnu.so ./cpp_lib/output/

# Expose port
EXPOSE 8000

# Run the application with uvicorn
CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
