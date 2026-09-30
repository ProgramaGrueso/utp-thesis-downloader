# 📚 UTP Thesis Downloader

Script en Python que descarga las tesis de **Ingeniería de Software** e **Ingeniería de Sistemas e Informática** publicadas desde 2025 en el [Repositorio Institucional UTP](https://repositorio.utp.edu.pe/), usando la **API REST de DSpace 7**.

Es la primera etapa de un pipeline para construir un dataset de fine-tuning a partir de tesis en PDF.

## Qué hace

- Recorre las colecciones con paginación y filtro por fecha de publicación.
- Descarga los PDF de cada tesis con reintentos automáticos (backoff exponencial).
- Descarga en paralelo con un número de hilos configurable y pausas entre requests para no saturar el servidor.
- Genera `resumen_descargas.csv` con colección, año, autores, título, handle, URL y estado de cada descarga.

## Uso

```bash
pip install requests
python3 descargar_tesis.py --dry-run          # solo lista las tesis
python3 descargar_tesis.py --output descargas --workers 2
```

## Uso responsable

El script se identifica con su propio User-Agent y hace pausas entre requests. Las tesis son propiedad de sus autores y de la universidad: úsalas según la licencia de cada documento y los términos del repositorio.
