# `python-base` sets up all our shared environment variables
FROM python:3.10-slim as python-base
LABEL GNS Science, NSHM Project <nshm@gns.cri.nz>

# ref https://stackoverflow.com/a/69094575 for templates

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_VERSION=1.4.0 \
    POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    PYTHONPATH=/application_root \
    VIRTUAL_ENVIRONMENT_PATH="/app/.venv"

ENV PATH="$VIRTUAL_ENVIRONMENT_PATH/bin:$PATH"

# `builder-base` stage is used to build deps + create our virtual environment
FROM python-base as builder-base
RUN apt-get update \
    && apt-get install --no-install-recommends -y \
        # deps for installing poetry
        curl \
        # deps for building python deps
        build-essential



# Install Poetry
RUN curl -sSL https://install.python-poetry.org | POETRY_HOME=$POETRY_HOME python3 - --version $POETRY_VERSION
ENV PATH "$PATH:/root/.local/bin/:$POETRY_HOME/bin"

# RUN apt-get install --no-install-recommends -y git   # deps for poetry seems 1.1.14 needs git !

WORKDIR /app

# Install Poetry packages (maybe remove the poetry.lock line if you don't want/have a lock file)
RUN poetry --version
ADD pyproject.toml ./
ADD poetry.lock ./
ADD toshi_hazard_post toshi_hazard_post
ADD tests tests
ADD demo demo
ADD dist dist
ADD README.md ./

# RUN poetry install --no-interaction --no-dev
RUN poetry install --no-interaction --only main

# Clean up project files. You can add them with a Docker mount later.
# RUN rm pyproject.toml poetry.lock

ADD scripts scripts
ADD pynamodb_settings.py pynamodb_settings.py
ENV PYNAMODB_CONFIG=/app/pynamodb_settings.py
RUN chmod +x /app/scripts/container_task.sh

WORKDIR /WORKING

# Hide virtual env prompt
# ENV VIRTUAL_ENV_DISABLE_PROMPT 1

# Start virtual env when bash starts
RUN echo 'source ${VIRTUAL_ENVIRONMENT_PATH}/bin/activate' >> ~/.bashrc

ENTRYPOINT ["/bin/bash", "-c"]
