# See https://containers.dev/guide/dockerfile
# https://hub.docker.com/_/microsoft-devcontainers-python
ARG VARIANT=3.12-bookworm
FROM mcr.microsoft.com/devcontainers/python:${VARIANT}

# Install dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    build-essential \
    libssl-dev git libffi-dev libgfortran5 pkg-config cmake gcc \
    && apt-get clean -y \
    && pip3 install --upgrade pip wheel

# Install Jupiter Lab
# RUN pip3 install jupyterlab jupyterlab-git
