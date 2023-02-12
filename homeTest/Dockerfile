FROM python:3.11

MAINTAINER "ido" "12ido350@gmail.com"

RUN apt-get update -y && \
    apt-get install -y python3-pip python-dev

# We copy just the requirements.txt first to leverage Docker cache
COPY requirements.txt /app/requirements.txt

WORKDIR /app

RUN pip3 install -r requirements.txt

COPY . /app

ENTRYPOINT [ "python" ]

CMD [ "producerConsumer.py" ]


#to run it: sudo docker build -t memphis:latest .