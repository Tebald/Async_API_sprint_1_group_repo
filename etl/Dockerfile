FROM python:3.11

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip \
     && pip install --no-cache-dir -r requirements.txt

COPY . /app

COPY start.sh create_index.sh ./

ENTRYPOINT ["./start.sh"]
