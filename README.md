# AlphaEngine

AlphaEngine is a containerized data processing pipeline designed to read inputs, process them according to configurable parameters, and output formatted metrics and execution logs.

---

## 🚀 Features

* **Containerized Execution:** Fully encapsulated in Docker for consistent execution across environments.
* **Configurable Data Pipeline:** Driven by YAML configuration for flexible metric tracking.
* **Automated Output:** Generates structured `metrics.json` directly from input datasets.

## 📋 Prerequisites

Ensure you have the following installed on your local machine:
* [Docker](https://www.docker.com/products/docker-desktop)
* [Docker Compose](https://docs.docker.com/compose/install/)

## 🛠️ Project Structure

```text
AlphaEngine/
├── Dockerfile          # Docker image configuration instructions
├── docker-compose.yml  # Compose file for service orchestration
├── run.py              # Main Python execution script
├── Config.yaml         # Application configuration settings
├── requirement.txt     # Python dependencies 
└── data.csv            # Input dataset (Required)
```

> **Note:** The `Dockerfile` currently expects the dependency and config files to be named `requirements.txt` and `config.yaml`. Ensure you match these names in your file directory before building.

## ⚙️ Setup and Installation

1. Navigate to the `AlphaEngine` directory in your terminal.
2. Ensure `data.csv`, `requirements.txt`, and `config.yaml` are present.
3. Build and start the container in detached mode:

   ```bash
   docker compose up -d --build
   ```

## 📊 Monitoring Logs

To view the real-time execution logs of the container:

```bash
docker compose logs -f
```

## 🛑 Stopping the Engine

To shut down and remove the running containers, use:

```bash
docker compose down
```