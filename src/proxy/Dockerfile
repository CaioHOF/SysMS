FROM python:3.13-slim

WORKDIR /app

RUN pip install --no-cache-dir pyzmq==27.2.0

COPY proxy.py ./

CMD ["python", "-u", "proxy.py"]
