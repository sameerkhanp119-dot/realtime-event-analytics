FROM python:3.11-slim
RUN pip install --no-cache-dir confluent-kafka==2.6.0 && useradd -r app
WORKDIR /app
COPY src ./src
ENV PYTHONPATH=/app/src PYTHONUNBUFFERED=1
USER app
CMD ["python", "-m", "rta.producer"]
