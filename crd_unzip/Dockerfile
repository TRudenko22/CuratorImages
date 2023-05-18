FROM python:3.8-slim-buster

WORKDIR /app

RUN apt update 
COPY requirements.txt requirements.txt

RUN apt install -y libpq-dev gcc
RUN pip3 install -r requirements.txt

COPY . .

