FROM python:3.11

RUN apt-get update && apt-get install -y netcat-openbsd curl

WORKDIR /app

COPY requirements.txt .

RUN pip install --upgrade pip \
     && pip install -r requirements.txt --no-cache-dir

COPY . /app

COPY start.sh create_index.sh ./

ENTRYPOINT ["./start.sh"]
