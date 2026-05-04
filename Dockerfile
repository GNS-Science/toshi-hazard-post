# `python-base` sets up all our shared environment variables
FROM python:3.10-slim as python-base
LABEL GNS Science, NSHM Project <chrisbc@artisan.co.nz>

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/application_root \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    VIRTUAL_ENVIRONMENT_PATH="/app/.venv"

ENV PATH="$VIRTUAL_ENVIRONMENT_PATH/bin:$PATH"

# `builder-base` stage installs uv + builds the venv
FROM python-base as builder-base
RUN apt-get update \
    && apt-get install --no-install-recommends -y \
        curl \
        build-essential

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

WORKDIR /app

RUN uv --version
ADD pyproject.toml ./
ADD uv.lock ./
ADD toshi_hazard_post toshi_hazard_post
ADD tests tests
ADD demo demo
ADD dist dist
ADD README.md ./

RUN uv sync --no-dev --frozen

ADD scripts scripts
ADD pynamodb_settings.py pynamodb_settings.py
ENV PYNAMODB_CONFIG=/app/pynamodb_settings.py
RUN chmod +x /app/scripts/container_task.sh

WORKDIR /WORKING

RUN echo 'source ${VIRTUAL_ENVIRONMENT_PATH}/bin/activate' >> ~/.bashrc

ENTRYPOINT ["/bin/bash", "-c"]
