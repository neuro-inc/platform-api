ARG PYTHON_VERSION=3.8.12
ARG PYTHON_BASE=buster

FROM python:${PYTHON_VERSION} AS installer

ENV PATH=/root/.local/bin:$PATH

# Copy to tmp folder to don't pollute home dir
RUN mkdir -p /tmp/dist
COPY dist /tmp/dist

RUN ls /tmp/dist
RUN pip install --user --find-links /tmp/dist platform-api

FROM python:${PYTHON_VERSION}-${PYTHON_BASE} AS service

LABEL org.opencontainers.image.source = "https://github.com/neuro-inc/platform-api"

WORKDIR /app

COPY --from=installer /root/.local/ /root/.local/
COPY alembic.ini alembic.ini
COPY alembic alembic

ENV PATH=/root/.local/bin:$PATH
ENV NP_API_PORT=8080
EXPOSE $NP_API_PORT

CMD platform-api
