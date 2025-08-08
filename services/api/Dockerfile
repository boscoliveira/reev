FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY genomics_api /app/genomics_api

EXPOSE 8000
CMD ["python", "-m", "genomics_api.main"]