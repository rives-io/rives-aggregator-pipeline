.ONESHELL:

.PHONY: dev build-image

dev:
	dagster dev -m agg_pipelines.definitions

build-image:
	IMAGE_VERSION=$$(git log -1 --format="%at" | xargs -I{} date -d @{} +%Y%m%d.%H%M).$$(git rev-parse --short HEAD)
	IMAGE_TAG=ghcr.io/rives-io/reader-node-watcher:$$IMAGE_VERSION
	echo $$IMAGE_TAG > .rives-core.tag
	docker build -f Dockerfile . \
		-t $$IMAGE_TAG \
		--label "org.opencontainers.image.title=rives-aggregator-pipeline" \
		--label "org.opencontainers.image.description=RIVES Aggregator Pipeline" \
		--label "org.opencontainers.image.source=https://github.com/rives-io/rives-aggregator-pipeline" \
		--label "org.opencontainers.image.revision=$$(git rev-parse HEAD)" \
		--label "org.opencontainers.image.created=$$(date -Iseconds --utc)" \
		--label "org.opencontainers.image.licenses=Apache-2.0" \
		--label "org.opencontainers.image.url=https://rives.io" \
		--label "org.opencontainers.image.version=$$IMAGE_VERSION"
