# JM Comment: Multi-stage, production hardened container for KubeRay RayJob execution
FROM rayproject/ray:2.35.0-py310 AS base

# JM Comment: Zero-trust container hardening: use non-root ray user (UID 1000)
USER root

WORKDIR /home/ray/app

# Install security updates & curl for healthchecks
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency specifications
COPY requirements.txt /home/ray/app/requirements.txt

# Install python dependencies without cache to minimize container layer attack surface
RUN pip install --no-cache-dir -r requirements.txt

# Copy pipeline application code and synthetic corporate datasets
COPY ray_pipeline.py /home/ray/app/ray_pipeline.py
COPY data/ /home/ray/app/data/

# Set ownership to unprivileged ray user
RUN chown -R ray:users /home/ray/app

# Switch back to non-root ray user
USER 1000

# Set environment defaults
ENV PYTHONUNBUFFERED=1 \
    RAY_DISABLE_DOCKER_CPU_WARNING=1 \
    ICEBERG_CATALOG_TYPE=glue

# JM Comment: Default CMD allows container to run as KubeRay daemon or execute batch jobs
CMD ["/bin/bash"]
