# syntax=docker/dockerfile:1
FROM registry.gitlab.com/health-ec/platform/infrastructure/build-images/golang-migrate-alpine:latest

# Switch to the working directory
WORKDIR /opt/migrate

COPY --chown=app ./migration/migrations migrations/