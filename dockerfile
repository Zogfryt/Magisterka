#syntax=docker/dockerfile:1.5

FROM python:3.12-bullseye as builder

RUN pip install poetry==1.4.2

ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

COPY pyproject.toml ./
RUN touch README.md

RUN poetry install --without cuda --no-root && rm -rf $POETRY_CACHE_DIR

FROM python:3.12-slim-bullseye as runtime

WORKDIR /app

ENV VIRTUAL_ENV=/.venv

COPY ./app /app
COPY --from=builder ${VIRTUAL_ENV} ${VIRTUAL_ENV}

RUN /.venv/bin/python -m spacy download pl_core_news_lg
RUN echo aga

EXPOSE 8501

ENTRYPOINT ["/bin/sh","-c","/.venv/bin/streamlit run main.py --server.port=8501 --server.address=0.0.0.0"]