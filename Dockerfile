# ── Stage: runtime ────────────────────────────────────────────────────────────
FROM python:3.9-slim

# Set a non-root working directory
WORKDIR /app

# Install Python dependencies first (layer-cached unless requirements change)
COPY requirement.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy source + data files
COPY run.py        .
COPY Config.yaml   config.yaml
# We only copy data.csv if it exists (using a wildcard trick, though here we'll just omit it or let user provide it)
COPY *.csv ./

# Default command: run the pipeline, print metrics to stdout, write logs
CMD ["python", "run.py", \
     "--input",    "data.csv", \
     "--config",   "config.yaml", \
     "--output",   "metrics.json", \
     "--log-file", "run.log"]