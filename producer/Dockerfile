FROM python:3.12-slim
WORKDIR /app
COPY producer/requirements.txt producer/requirements.txt
RUN pip install --no-cache-dir -r producer/requirements.txt
COPY shared shared
COPY producer producer
CMD ["python", "-m", "producer.main"]
