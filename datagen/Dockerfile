FROM python:3.11-slim

WORKDIR /usr/src/app

COPY . .

RUN pip install -r requirements.txt

CMD [ "python", "./kafka_push.py" ]








