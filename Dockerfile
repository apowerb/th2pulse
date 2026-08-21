# Use a slim Python image
FROM python:3.11-slim-bookworm

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Set working directory
WORKDIR /app

# Enable bytecode compilation
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install dependencies using uv
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev

# Copy source code
COPY src/ ./src/

# Run the application
CMD ["uv", "run", "python", "-m", "th2pulse.ingest"]
