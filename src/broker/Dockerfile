FROM python:3.13-slim

WORKDIR /app

RUN pip install --no-cache-dir pyzmq==27.2.0

COPY broker.py ./

CMD ["python", "-u", "broker.py"]
