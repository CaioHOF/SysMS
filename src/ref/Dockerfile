FROM python:3.13-slim

WORKDIR /app

RUN pip install --no-cache-dir pyzmq==27.2.0 msgpack==1.2.4

COPY ref.py ./

CMD ["python", "-u", "ref.py"]
