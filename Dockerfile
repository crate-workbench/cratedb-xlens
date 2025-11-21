# Multi-stage build for minimal production image
FROM docker.io/python:3.12-slim-bookworm AS builder

# Configure build environment.
ENV PIP_ROOT_USER_ACTION=ignore
ENV UV_COMPILE_BYTECODE=true
ENV UV_LINK_MODE=copy
ENV UV_PYTHON_DOWNLOADS=never
ENV UV_SYSTEM_PYTHON=true

# Install build dependencies
RUN apt-get update && apt-get install -y \
  build-essential \
  && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy dependency files
COPY pyproject.toml ./

# Install the `uv` package manager.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install dependencies using uv sync (without installing the project itself)
RUN uv sync --no-dev --no-install-project

# Production stage
FROM docker.io/python:3.12-slim-bookworm

# Configure build environment.
ENV PIP_ROOT_USER_ACTION=ignore
ENV UV_COMPILE_BYTECODE=true
ENV UV_LINK_MODE=copy
ENV UV_PYTHON_DOWNLOADS=never
ENV UV_SYSTEM_PYTHON=true

# Install the `uv` package manager.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install `curl`.
RUN true && \
    apt-get update && apt-get install --yes curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create non-root user for security
RUN groupadd --gid 1000 xmover && \
  useradd --uid 1000 --gid xmover --shell /bin/bash --create-home xmover

# Copy virtual environment from builder
COPY --from=builder /app/.venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Set working directory
WORKDIR /app

# Copy application code
COPY --chown=xmover:xmover src/ ./src/
COPY --chown=xmover:xmover pyproject.toml README.md ./

# Install application in editable mode
# TODO: Why not install the wheel package from the previous build step?
RUN uv pip install -e .

# Switch to non-root user
USER xmover

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD python -c "import xmover; print('XMover OK')" || exit 1

# Set default command
ENTRYPOINT ["xmover"]
CMD ["--help"]

# Labels for metadata
LABEL org.opencontainers.image.title="XMover"
LABEL org.opencontainers.image.description="CrateDB Shard Analyzer and Movement Tool"
LABEL org.opencontainers.image.version="v0.0.0"
LABEL org.opencontainers.image.vendor="XMover Team"
LABEL org.opencontainers.image.licenses="MIT"
