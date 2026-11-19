FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY velib_producer ./velib_producer
RUN useradd --create-home producer
USER producer
EXPOSE 8000
CMD ["python", "-m", "velib_producer.main"]
