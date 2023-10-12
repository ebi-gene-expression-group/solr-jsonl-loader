#!/usr/bin/env bash
set -e

scriptDir=$(cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $scriptDir/common_routines.sh

require_env_var INPUT_JSONL
require_env_var SOLR_COLLECTION
require_env_var SCHEMA_VERSION

COLLECTION=${SOLR_COLLECTION}-v${SCHEMA_VERSION}
# Space separated list of hosts
SOLR_HOSTS=${SOLR_HOSTS:-'localhost:8983'}
SOLR_USER=${SOLR_USER:-"solr"}
SOLR_PASS=${SOLR_PASS:-"SolrRocks"}
SOLR_AUTH="-u $SOLR_USER:$SOLR_PASS"
# SOLR_PROCESSORS must be null or a comma-separated list of processors to use during an update
if [[ $SOLR_PROCESSORS ]]
then
  PROCESSOR="?processor=${SOLR_PROCESSORS}"
fi

SOLR_HOSTS_ARR=(${SOLR_HOSTS})

commit() {
  echo "Committing files on ${1}..."
  curl --retry 20 --retry-all-errors \
  --silent --show-error \
  $SOLR_AUTH \
  "http://${1}/solr/${COLLECTION}/update?commit=true"
}

post_json() {
  # The update/json/docs handler supports both regular JSON and JSON Lines:
  # https://solr.apache.org/guide/7_1/transforming-and-indexing-custom-json.html#multiple-documents-in-a-single-payload
  curl --retry 20 --retry-all-errors \
  --silent --show-error \
  $SOLR_AUTH \
  "http://${1}/solr/${COLLECTION}/update/json/docs$PROCESSOR" \
  --data-binary "@${2}" \
  -H 'Content-type:application/json'
}


COMMIT_DOCS=${SOLR_COMMIT_DOCS:-1000000}
echo "Loading $INPUT_JSONL into hosts ${SOLR_HOSTS_ARR[@]} collection $COLLECTION committing every ${COMMIT_DOCS} docs..."

CHUNK_PREFIX=${CHUNK_PREFIX:-`basename -s .jsonl ${INPUT_JSONL}`-}

NUM_DOCS_PER_BATCH=${NUM_DOCS_PER_BATCH:-50000}
split -a 3 -l $NUM_DOCS_PER_BATCH $INPUT_JSONL $CHUNK_PREFIX --additional-suffix .jsonl
CHUNK_FILES=$(ls $CHUNK_PREFIX*)

cleanup() {
  rm ${CHUNK_FILES}
}


trap cleanup exit
# I is used to print the progress of a chunk file (e.g. 1/10)
# J is used to round-robin the hosts
I=0
J=0
for CHUNK_FILE in $CHUNK_FILES
do
  SOLR_HOST=${SOLR_HOSTS_ARR[(( ${J} % ${#SOLR_HOSTS_ARR[@]} ))]}
  J=$(( $J + 1 ))

  I=$(( $I + 1 ))
  echo "$CHUNK_FILE ${I}/$(wc -w <<< $CHUNK_FILES) -> ${SOLR_HOST}"

  post_json ${SOLR_HOST} ${CHUNK_FILE}

  if [[ $(( $I % ( $COMMIT_DOCS / $NUM_DOCS_PER_BATCH) )) == 0 ]]
  then
    # Make the commit in the next host
    J=$(( $J + 1 ))
    SOLR_HOST=${SOLR_HOSTS_ARR[(( ${J} % ${#SOLR_HOSTS_ARR[@]} ))]}
    commit ${SOLR_HOST}
  fi
done

J=$(( $J + 1 ))
SOLR_HOST=${SOLR_HOSTS_ARR[(( ${J} % ${#SOLR_HOSTS_ARR[@]} ))]}
commit ${SOLR_HOST}
