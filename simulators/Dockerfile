FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /srv

COPY simulators/requirements.txt /tmp/requirements.txt
COPY common /tmp/common
RUN pip install --no-cache-dir -r /tmp/requirements.txt /tmp/common \
    && rm -rf /tmp/common /tmp/requirements.txt

COPY simulators/ward_sim /srv/ward_sim

RUN useradd --system --uid 10001 --no-create-home sim
USER sim

CMD ["python", "-m", "ward_sim.vitals_producer"]
