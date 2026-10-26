FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    TZ=Asia/Shanghai

WORKDIR /opt/stock_data_manage

COPY pyproject.toml README.md ./
COPY src ./src

RUN pip install --no-cache-dir --editable '.[web]' \
    && useradd --create-home --uid 1000 app

EXPOSE 8765

CMD ["stock-data-web", "--host", "0.0.0.0", "--port", "8765", "--allow-network", "--config-root", "/opt/stock_data_manage/config", "--data-root", "/opt/stock_data_manage/data"]
