FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Dependências do sistema (ffmpeg para pydub)
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copia o código
COPY . /app

# Instala dependências Python
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir flask redis pydub pillow

# Comando padrão (sobrescrito pelo docker-compose)
CMD ["python", "server.py"]
