FROM apache/airflow:2.9.1-python3.12

ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/app"
ENV PATH=/opt/airflow/.local/bin:$PATH

USER root

RUN apt-get clean && rm -rf /var/lib/apt/lists/* \
    && apt-get update \
    && apt-get install -y \
    curl \
    git \
    unzip \
    libnss3 \
    libxss1 \
    libasound2 \
    fonts-liberation \
    libgbm-dev \
    libpangocairo-1.0-0 \
    libpangoft2-1.0-0 \
    libdbus-1-3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libxkbcommon0 \
    libatspi2.0-0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2

USER airflow

RUN pip install --upgrade pip \
    && pip install playwright mysql-connector-python pandasql pymysql aiomysql

# Playwright 브라우저 설치
RUN playwright install