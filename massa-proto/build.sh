#!/bin/bash

# Function to generate documentation
generate_docs() {
  local proto_dir=$1
  local doc_type=$2
  local doc_name=$3

  echo "Generating ${doc_type} ${doc_name} documentation..."
  protoc $(find "${proto_dir}" -name '*.proto') \
    --proto_path="${proto_dir}" \
    --proto_path=./proto/commons \
    --proto_path=./proto/third_party \
    --doc_out=./doc/ \
    --doc_opt="${doc_type},${doc_name}"
}

echo "Building Massa proto..."

echo "Linting and formatting proto files..."
buf lint && buf format -w
echo "Proto files linted and formatted successfully."

# Generate ABI documentation
echo "Generating ABI documentation..."
generate_docs "./proto/abis" "html" "abi.html"
generate_docs "./proto/abis" "markdown" "abi.md"
echo "ABI documentation generated successfully."

# Generate API documentation
echo "Generating API documentation..."
generate_docs "./proto/apis" "html" "api.html"
generate_docs "./proto/apis" "markdown" "api.md"
echo "API documentation generated successfully."

# Generate COMMONS documentation
echo "Generating COMMONS documentation..."
generate_docs "./proto/commons" "html" "commons.html"
generate_docs "./proto/commons" "markdown" "commons.md"
echo "COMMONS documentation generated successfully."

echo "Massa proto build finished!"
