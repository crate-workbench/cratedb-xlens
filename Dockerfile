# Multi-stage build for minimal production image
FROM python:3.12-slim AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
  build-essential \
  && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Install uv for fast dependency management
RUN pip install uv

# Install dependencies using uv sync (without installing the project itself)
RUN uv sync --frozen --no-dev --no-install-project

# Production stage
FROM python:3.12-slim

# Create non-root user for security
RUN groupadd --gid 1000 xmover && \
  useradd --uid 1000 --gid xmover --shell /bin/bash --create-home xmover

# Install runtime dependencies only
RUN apt-get update && apt-get install -y \
  curl \
  && rm -rf /var/lib/apt/lists/* \
  && apt-get clean

# Copy virtual environment from builder
COPY --from=builder /app/.venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Set working directory
WORKDIR /app

# Copy application code
COPY --chown=xmover:xmover src/ ./src/
COPY --chown=xmover:xmover pyproject.toml README.md ./

# Install application in editable mode
RUN pip install -e .

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
LABEL org.opencontainers.image.version="v0.0.1"
LABEL org.opencontainers.image.vendor="XMover Team"
LABEL org.opencontainers.image.licenses="MIT"
