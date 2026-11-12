# =============================================================================
# HYDRA-UMC-TELEMETRY-COLLECTOR - Container Build: Dockerfile
# Copyright (C) 2026 JuanenRac (Electro Hobby 3D) <electrohobby3d@gmail.com>
# GPL-3.0-or-later - see LICENSE
# =============================================================================
# The collector as a long-running service, like the rest of the family. It is a single static binary with no runtime
# dependency. The health check only opens the port: GET /health is closed unless the operator opens it.
#
# Inside a container the service must listen on every interface to be reached through a published port, and it refuses to do
# that without a bearer token: pass it as HYDRA_UMC_TELEMETRY_COLLECTOR_TOKEN (the variable the program reads). Point it at the data lake
# with --datalake-url. Samples that wait for a data lake that is down live in memory unless --spool names a file in the /data
# volume. Publish the port on the loopback of the host (-p 127.0.0.1:8092:8092) unless something with its own authentication
# sits in front.
#
#   docker build -t hydra-umc-telemetry-collector .
#   docker run --rm -e HYDRA_UMC_TELEMETRY_COLLECTOR_TOKEN='a-secret-of-16-or-more-characters' -p 127.0.0.1:8092:8092 \
#       -v telemetry-collector-data:/data hydra-umc-telemetry-collector --datalake-url http://datalake:8095 --spool /data/spool.bin

FROM golang:1.24-alpine AS build

WORKDIR /src
COPY src ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/telemetry-collector .

FROM alpine:3.20

RUN adduser -S -u 10001 hydra && mkdir /data && chown hydra /data
COPY --from=build /out/telemetry-collector /usr/local/bin/telemetry-collector
USER hydra
VOLUME ["/data"]

EXPOSE 8092
HEALTHCHECK --interval=15s --timeout=5s --retries=5 CMD ["nc", "-z", "127.0.0.1", "8092"]
ENTRYPOINT ["telemetry-collector"]
CMD ["--addr", "0.0.0.0:8092"]
