FROM python:3.12-slim

WORKDIR /opt/apex

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY pyproject.toml README.md ./
COPY src ./src
COPY api ./api

RUN pip install --no-cache-dir -e .[api]

CMD ["uvicorn", "api.app.main:app", "--host", "0.0.0.0", "--port", "8000"]
