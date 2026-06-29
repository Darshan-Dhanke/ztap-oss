ARG REPOSITORY=ghcr.io/neondatabase
ARG COMPUTE_IMAGE=compute-node-v14
ARG TAG=latest

FROM $REPOSITORY/${COMPUTE_IMAGE}:$TAG

ARG COMPUTE_IMAGE

USER root
RUN echo 'Acquire::Retries "5";' > /etc/apt/apt.conf.d/80-retries && \
    apt-get update &&       \
    apt-get install -y curl \
                       jq   \
                       netcat-openbsd
#This is required for the pg_hintplan test
RUN mkdir -p /ext-src/pg_hint_plan-src /postgres/contrib/file_fdw /ext-src/postgis-src/ && chown postgres /ext-src/pg_hint_plan-src /postgres/contrib/file_fdw /ext-src/postgis-src

# Bake the launch script + spec into the image so a branch compute can be
# started with `docker run` (no bind mounts). The compose service still mounts
# these over the top for the main compute — same files, so no conflict.
COPY shell/ /shell/
COPY var/db/postgres/configs/ /var/db/postgres/configs/
RUN chmod -R a+rx /shell && chmod -R a+r /var/db/postgres/configs

USER postgres
