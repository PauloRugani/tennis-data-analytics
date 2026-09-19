FROM apache/airflow:2.9.3-python3.11

USER root

ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

RUN apt-get update && apt-get install -y --no-install-recommends \
    xvfb \
    xauth \
    openjdk-17-jre-headless \
    procps \
    build-essential \
    libffi-dev \
    libatk-bridge2.0-0 \
    libcairo2 \
    libpango-1.0-0 \
    libglib2.0-0 \
    libnspr4 \
    libnss3 \
    libatk1.0-0 \
    libdbus-1-3 \
    libx11-6 \
    libxcomposite1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libxcb1 \
    libxkbcommon0 \
    libasound2 \
    libatspi2.0-0 \
    && mkdir -p /ms-playwright \
    && chmod -R 777 /ms-playwright \
    && mkdir -p /opt/airflow/data \
    && chmod -R 777 /opt/airflow/data \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    playwright==1.61.0 \
    pyspark==3.5.0 \
    pandas \
    pyarrow \
    tqdm \
    python-dotenv \
    beautifulsoup4 \
    curl_cffi \
    && playwright install chromium
