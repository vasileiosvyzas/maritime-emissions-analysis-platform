FROM python:3.9

WORKDIR /app

COPY requirements.txt /app/requirements.txt

RUN pip install --upgrade pip
RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt

COPY ./src/data_acquisition.py /app/data_acquisition.py
COPY .env /app/.env

CMD ["python", "data_acquisition.py"]
