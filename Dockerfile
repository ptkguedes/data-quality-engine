# Imagem de runtime do servico de ingestao (decisao D16).
# A versao bate com a 3.12.3 usada no desenvolvimento e nos testes.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# As dependencias sao copiadas e instaladas ANTES do codigo-fonte: a camada de
# dependencias fica cacheada e so e reconstruida quando requirements.txt muda.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# Execucao como usuario sem privilegios.
RUN useradd --create-home --uid 1000 appuser \
    && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
