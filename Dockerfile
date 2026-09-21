# Compile redfish_exporter
FROM python:3.12.7-alpine3.20 as build

LABEL Name=builder
LABEL appVersion=${appVersion}
LABEL maintainer=${maintainer}

ENV TZ=Asia/Ho_Chi_Minh

COPY ./src /opt/Configurable_Redfish_Exporter/src
COPY MANIFEST.in /opt/Configurable_Redfish_Exporter/MANIFEST.in
COPY setup.py /opt/Configurable_Redfish_Exporter/setup.py

RUN pip3 install setuptools
WORKDIR /opt/Configurable_Redfish_Exporter
RUN python3 setup.py sdist --formats=gztar

FROM python:3.12.7-alpine3.20

LABEL Name=redfish_exporter
LABEL appVersion=${appVersion}
LABEL maintainer=${maintainer}

ENV TZ=Asia/Ho_Chi_Minh
# Runtime never writes .pyc files, so a read-only root filesystem (see
# deployment.yml/redfish-exporter-shard.yaml.j2 container securityContext)
# never hits a permission error trying to cache bytecode into site-packages.
ENV PYTHONDONTWRITEBYTECODE=1

# COPY --from=build /opt/Configurable_Redfish_Exporter/src/redfish_exporter/templates /opt/Configurable_Redfish_Exporter/templates
COPY --from=build /opt/Configurable_Redfish_Exporter/dist/*.tar.gz /tmp/redfish_exporter/physical-exporter.tar.gz
RUN apk add tzdata && \
    echo $TZ > /etc/timezone && \
    pip install --no-cache-dir /tmp/redfish_exporter/physical-exporter.tar.gz && \
    rm -rf /tmp/* && \
    mkdir -p /opt/redfish_exporter && \
    ln -s /usr/local/lib/python3.12/site-packages/redfish_collector/core/templates /opt/redfish_exporter/templates && \
    addgroup -g 10001 exporter && \
    adduser -D -H -u 10001 -G exporter exporter
# Fixed, verifiable non-root identity (UID/GID 10001) — deployment
# manifests' container securityContext.runAsUser/runAsGroup and pod
# securityContext.fsGroup must match this exact value, not an arbitrary one.
# HOME is repointed to the writable /tmp mount: `adduser -H` skips creating
# a home directory, so the default /home/exporter would otherwise be an
# unwritable, nonexistent path under the read-only root filesystem.
ENV HOME=/tmp
USER 10001:10001
ENTRYPOINT ["redfish-exporter"]
EXPOSE 9814