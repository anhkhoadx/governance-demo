#!/usr/bin/env bash
set -euo pipefail

export GOVDEMO_ROLE=${GOVDEMO_ROLE:-data_engineer}
export PII_TOKEN_SECRET=${PII_TOKEN_SECRET:-dev-secret-change-me}

govdemo init
govdemo seed
govdemo ingest --source app
govdemo clean
govdemo curate
govdemo serve
govdemo build-identity

export GOVDEMO_ROLE=activation_service
govdemo export-audience --min-events 2

export GOVDEMO_ROLE=data_engineer
govdemo gdpr request --user-id u1 --mode delete

echo "Demo completed."
