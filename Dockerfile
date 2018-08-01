FROM python:3

RUN mkdir -p /poc
WORKDIR /poc

COPY requirements.txt /poc

RUN pip install --no-cache-dir -r requirements.txt

COPY . /poc