# Use a slim Python image
FROM python:3.11-slim-bookworm

# Release to install. The Docker workflow passes it as a build-arg, derived
# from the git tag the PyPI workflow just published. There is deliberately no
# default: a build with no --build-arg fails loudly below rather than
# silently producing an image of whatever version PyPI happens to serve.
#
# To try the working tree instead of a release, run the service directly --
# `uv run --extra ingest python -m th2pulse.ingest`. This image is for
# published versions only, so that what it reports as its version is what it
# actually contains.
ARG TH2PULSE_VERSION

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Set working directory
WORKDIR /app

# Enable bytecode compilation
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV UV_NO_CACHE=1
ENV UV_PROJECT_ENVIRONMENT=/opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Dependencies first, from uv.lock, so the two published architectures and
# any later rebuild of the same tag get the same transitive versions. The
# "ingest" extra is not optional here: the CMD below runs the ingest
# service, which imports fastapi/uvicorn/asyncpg. Leaving it out is what
# made the 0.1.2 image exit 1 on start with "No module named 'fastapi'".
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --extra ingest --no-install-project

# Then the package itself, from PyPI at the tagged version. Installing the
# published wheel rather than building the working tree is what makes the
# version the image reports match the tag it is published under: the release
# workflow rewrites pyproject.toml's placeholder version in flight, for the
# wheel only, so a source build bakes in the placeholder.
RUN test -n "${TH2PULSE_VERSION}" \
    || { echo "TH2PULSE_VERSION build-arg is required, e.g. --build-arg TH2PULSE_VERSION=0.1.3" >&2; exit 1; } \
    && uv pip install --no-cache-dir "th2pulse[ingest]==${TH2PULSE_VERSION}"

# The library binds 127.0.0.1 by default, which is right for a process
# started on a developer machine and useless inside a container: nothing
# outside the container could reach it. Reaching past loopback is also what
# turns the tokens from optional hardening into a requirement, so the
# service now refuses to start on a reachable bind unless
# TH2PULSE_INGEST_TOKEN and TH2PULSE_QUERY_TOKEN are set (or
# TH2PULSE_ALLOW_UNAUTHENTICATED=1 says authorization lives upstream).
ENV TH2PULSE_INGEST_HOST=0.0.0.0
ENV TH2PULSE_INGEST_PORT=4319
EXPOSE 4319

# Run the application. Plain `python`, not `uv run`: `uv run` re-resolves the
# project environment on every container start, which needs network access
# from the running container and pulls the dev dependency group into a
# production image.
CMD ["python", "-m", "th2pulse.ingest"]
