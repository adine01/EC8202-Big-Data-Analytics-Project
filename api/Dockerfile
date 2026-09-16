FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /srv

COPY api/requirements.txt /tmp/requirements.txt
COPY common /tmp/common
RUN pip install --no-cache-dir -r /tmp/requirements.txt /tmp/common \
    && rm -rf /tmp/common /tmp/requirements.txt

COPY api/app /srv/app

RUN useradd --system --uid 10001 --no-create-home api
USER api

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
