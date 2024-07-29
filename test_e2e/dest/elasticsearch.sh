#!/usr/bin/env bash

set -e

DEST_PATH=$(dirname "$(realpath "$0")")
SCRIPT_DIR=$(dirname "$DEST_PATH")
cd "$SCRIPT_DIR"/.. || exit 1
OUTPUT_FOLDER_NAME=elasticsearch-dest
OUTPUT_ROOT=${OUTPUT_ROOT:-$SCRIPT_DIR}
OUTPUT_DIR=$OUTPUT_ROOT/structured-output/$OUTPUT_FOLDER_NAME
WORK_DIR=$OUTPUT_ROOT/workdir/$OUTPUT_FOLDER_NAME
CI=${CI:-"false"}
max_processes=${MAX_PROCESSES:=$(python3 -c "import os; print(os.cpu_count())")}

# shellcheck disable=SC1091
source "$SCRIPT_DIR"/cleanup.sh
# shellcheck disable=SC1091
source "$SCRIPT_DIR"/env_setup/elasticsearch/common/es-dest-ingest-test-creds.env
function cleanup {
  # Index cleanup
  echo "Stopping Elasticsearch Docker container"
  docker-compose -f "$SCRIPT_DIR"/env_setup/elasticsearch/common/docker-compose.yaml down --remove-orphans -v

  # Local file cleanup
  cleanup_dir "$WORK_DIR"
  cleanup_dir "$OUTPUT_DIR"
  if [ "$CI" == "true" ]; then
    cleanup_dir "$DOWNLOAD_DIR"
  fi
}

trap cleanup EXIT

echo "Creating elasticsearch instance"
# shellcheck source=/dev/null
"$SCRIPT_DIR"/env_setup/elasticsearch/destination_connector/create-elasticsearch-instance.sh
wait

PYTHONPATH=. ./unstructured_ingest/main.py \
  local \
  --num-processes "$max_processes" \
  --output-dir "$OUTPUT_DIR" \
  --strategy fast \
  --verbose \
  --reprocess \
  --input-path example-docs/book-war-and-peace-1225p.txt \
  --work-dir "$WORK_DIR" \
  --chunking-strategy by_title \
  --chunk-combine-text-under-n-chars 200 \
  --chunk-new-after-n-chars 2500 \
  --chunk-max-characters 38000 \
  --chunk-multipage-sections \
  --embedding-provider "langchain-huggingface" \
  elasticsearch \
  --hosts http://localhost:9200 \
  --index-name ingest-test-destination \
  --username "$ELASTIC_USER" \
  --password "$ELASTIC_PASSWORD" \
  --batch-size-bytes 15000000 \
  --num-threads "$max_processes"

desired_count=$(cat "$WORK_DIR"/upload_stage/* | jq 'length')
desired_embeddings=$(cat "$WORK_DIR"/upload_stage/* | jq '.[0]._source.embeddings' | tr -d '\n')

PYTHONPATH=. "$SCRIPT_DIR"/env_setup/elasticsearch/destination_connector/test-ingest-elasticsearch-output.py \
  --num-elements "$desired_count" \
  --embeddings "$desired_embeddings"