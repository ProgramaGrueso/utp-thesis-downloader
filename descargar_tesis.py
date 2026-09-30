#!/usr/bin/env python3
"""
Descargador de tesis del Repositorio Institucional UTP
======================================================
Descarga las tesis de Ingeniería de Sistemas e Informática e
Ingeniería de Software, con fecha de publicación >= 2025.

Usa la API REST de DSpace 7 (backend) del repositorio.

Uso:
    python3 descargar_tesis.py [--dry-run] [--output DIR] [--workers N]

Flags:
    --dry-run     Solo lista las tesis sin descargar nada.
    --output DIR  Directorio destino (default: ./descargas).
    --workers N   Descargas simultáneas (default: 2).
"""

import argparse
import csv
import json
import logging
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ─────────────────────────────────────────────
# Configuración
# ─────────────────────────────────────────────

BASE_URL = "https://repositorio.utp.edu.pe/backend/api"

# Collections a descargar
COLLECTIONS = {
    # Ingeniería de Software — Tesis para Título Profesional
    "a5caa788-b793-459c-8f67-09eaa5299209": "Ingenieria_de_Software",
    # Ingeniería de Sistemas e Informática — Tesis para Título Profesional
    "b1e8b80a-a493-4571-ba85-32e03fe81377": "Ingenieria_de_Sistemas",
}

DATE_FILTER = "[2025 TO 2030]"  # >= 2025
PAGE_SIZE = 20

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "utp-thesis-downloader/1.0 (+https://github.com/ProgramaGrueso/utp-thesis-downloader)",
    "Referer": "https://repositorio.utp.edu.pe/",
}

# ─────────────────────────────────────────────
# Tipos
# ─────────────────────────────────────────────


@dataclass
class TesisInfo:
    uuid: str
    titulo: str
    autores: list[str]
    fecha: str
    coleccion: str
    handle: str
    bitstreams: list[dict] = field(default_factory=list)


# ─────────────────────────────────────────────
# HTTP con soporte Multi-thread y reintentos
# ─────────────────────────────────────────────

_thread_local = threading.local()


def crear_sesion() -> requests.Session:
    """Crea una sesión HTTP con reintentos automáticos."""
    session = requests.Session()
    session.headers.update(HEADERS)
    retry = Retry(
        total=5,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def get_thread_session() -> requests.Session:
    """Retorna una sesión única y segura por cada hilo."""
    if not hasattr(_thread_local, "session"):
        _thread_local.session = crear_sesion()
    return _thread_local.session


# ─────────────────────────────────────────────
# API helpers
# ─────────────────────────────────────────────

log = logging.getLogger("utp_downloader")


def obtener_items_coleccion(
    session: requests.Session,
    collection_id: str,
    collection_name: str,
) -> list[TesisInfo]:
    """Pagina sobre la API de discovery para obtener todos los items de una colección."""
    items: list[TesisInfo] = []
    page = 0

    while True:
        url = (
            f"{BASE_URL}/discover/search/objects"
            f"?scope={collection_id}"
            f"&page={page}&size={PAGE_SIZE}"
            f"&sort=dc.date.issued,DESC"
            f"&f.dateIssued={DATE_FILTER},equals"
        )

        log.info("  Consultando pág %d de %s...", page, collection_name)
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        search_result = data["_embedded"]["searchResult"]
        objects = search_result["_embedded"].get("objects", [])

        if not objects:
            break

        for obj in objects:
            idx_obj = obj["_embedded"]["indexableObject"]
            meta = idx_obj.get("metadata", {})

            autores = [
                a["value"]
                for a in meta.get("dc.contributor.author", [])
            ]
            fecha_raw = meta.get("dc.date.issued", [{}])
            fecha = fecha_raw[0]["value"] if fecha_raw else "sin-fecha"

            tesis = TesisInfo(
                uuid=idx_obj["uuid"],
                titulo=idx_obj.get("name", "sin-titulo"),
                autores=autores,
                fecha=fecha,
                coleccion=collection_name,
                handle=idx_obj.get("handle", ""),
            )
            items.append(tesis)

        total_pages = search_result["page"]["totalPages"]
        page += 1
        if page >= total_pages:
            break

        time.sleep(1.0)

    return items


def obtener_bitstreams(session: requests.Session, item_uuid: str) -> list[dict]:
    """Obtiene los bitstreams del bundle ORIGINAL de un item."""
    url = f"{BASE_URL}/core/items/{item_uuid}/bundles"
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    bundles = data.get("_embedded", {}).get("bundles", [])
    original_bundle = None
    for bundle in bundles:
        if bundle.get("name") == "ORIGINAL":
            original_bundle = bundle
            break

    if not original_bundle:
        return []

    bundle_uuid = original_bundle["uuid"]
    bs_url = f"{BASE_URL}/core/bundles/{bundle_uuid}/bitstreams"
    resp = session.get(bs_url, timeout=30)
    resp.raise_for_status()
    bs_data = resp.json()

    result = []
    for bs in bs_data.get("_embedded", {}).get("bitstreams", []):
        result.append({
            "uuid": bs["uuid"],
            "name": bs.get("name", "archivo.pdf"),
            "size": bs.get("sizeBytes", 0),
            "download_url": f"{BASE_URL}/core/bitstreams/{bs['uuid']}/content",
        })

    return result


# ─────────────────────────────────────────────
# Descarga
# ─────────────────────────────────────────────


def sanitizar_nombre(nombre: str, max_len: int = 120) -> str:
    """Limpia un string para usarlo de forma segura como nombre de archivo."""
    nombre = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', nombre)
    nombre = re.sub(r'\s+', ' ', nombre).strip()
    if len(nombre) > max_len:
        nombre = nombre[:max_len].rstrip(" ._")
    return nombre


def descargar_tesis_worker(
    tesis: TesisInfo,
    output_dir: Path,
) -> tuple[TesisInfo, bool, str, list[str]]:
    """
    Descarga los bitstreams de una tesis utilizando la sesión thread-local.
    Omite archivos administrativos restringidos (formularios, similitud).
    Si la tesis está bajo embargo institucional (401), se registra adecuadamente.
    Retorna (tesis, exito, mensaje, archivos_guardados).
    """
    session = get_thread_session()
    col_dir = output_dir / tesis.coleccion
    col_dir.mkdir(parents=True, exist_ok=True)

    archivos_guardados = []

    try:
        if not tesis.bitstreams:
            tesis.bitstreams = obtener_bitstreams(session, tesis.uuid)
    except Exception as e:
        return (tesis, False, f"Error obteniendo metadatos de archivos: {e}", [])

    if not tesis.bitstreams:
        return (tesis, False, "Sin archivos en bundle ORIGINAL", [])

    autor_str = sanitizar_nombre(tesis.autores[0] if tesis.autores else "sin-autor", 50)
    titulo_str = sanitizar_nombre(tesis.titulo, 90)
    base_name = f"{tesis.fecha} - {autor_str} - {titulo_str}"

    # Filtrar primero los bitstreams relevantes (priorizar la tesis y descartar formularios administrativos)
    bitstreams_a_descargar = []
    for bs in tesis.bitstreams:
        nombre_bs = bs.get("name", "").lower()
        # Omitir formularios internos y reportes de similitud que siempre están restringidos
        if any(palabra in nombre_bs for palabra in ["formulario", "similitud", "autorizacion", "autorización"]):
            continue
        bitstreams_a_descargar.append(bs)

    # Si todos eran formularios (caso raro), intentar con todos
    if not bitstreams_a_descargar:
        bitstreams_a_descargar = tesis.bitstreams

    total_bs = len(bitstreams_a_descargar)
    errores_401 = 0

    for idx, bs in enumerate(bitstreams_a_descargar):
        ext = Path(bs["name"]).suffix or ".pdf"
        suffix_extra = f"_p{idx+1}" if total_bs > 1 else ""
        filename = f"{base_name}{suffix_extra}{ext}"
        filepath = col_dir / filename

        # Si ya existe y tiene contenido válido (> 10 KB), se da por completado
        if filepath.exists() and filepath.stat().st_size > 10240:
            archivos_guardados.append(filename)
            continue

        try:
            resp = session.get(
                bs["download_url"],
                timeout=120,
                stream=True,
                headers={**HEADERS, "Accept": "*/*"},
            )

            # Si está restringido o bajo embargo institucional
            if resp.status_code in (401, 403):
                errores_401 += 1
                continue

            resp.raise_for_status()

            with open(filepath, "wb") as f:
                for chunk in resp.iter_content(chunk_size=16384):
                    f.write(chunk)

            if filepath.stat().st_size > 0:
                archivos_guardados.append(filename)
            else:
                filepath.unlink(missing_ok=True)

        except Exception as e:
            if filepath.exists():
                filepath.unlink(missing_ok=True)
            log.warning("    Error al descargar %s (%s): %s", bs["name"], tesis.handle, e)

        time.sleep(1.0)

    if archivos_guardados:
        return (tesis, True, f"Descargado ({len(archivos_guardados)} archivo/s)", archivos_guardados)

    if errores_401 > 0:
        return (tesis, False, "EMBARGADA (Acceso restringido institucionalmente por la UTP/autor)", [])

    return (tesis, False, "No se pudo descargar ningún archivo", [])


# ─────────────────────────────────────────────
# Orquestación principal
# ─────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Descarga tesis del repositorio UTP (Ing. Sistemas + Software, desde 2025)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Solo listar tesis sin descargar",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="./descargas",
        help="Directorio destino (default: ./descargas)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=2,
        help="Descargas simultáneas (default: 2)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    session = crear_sesion()

    # ── Fase 1: Recopilar catálogo ──
    todas_las_tesis: list[TesisInfo] = []

    for col_id, col_name in COLLECTIONS.items():
        log.info("Consultando colección: %s", col_name)
        items = obtener_items_coleccion(session, col_id, col_name)
        log.info("  → %d tesis encontradas en %s (>= 2025)", len(items), col_name)
        todas_las_tesis.extend(items)

    log.info("═" * 60)
    log.info("Total de tesis identificadas: %d", len(todas_las_tesis))
    log.info("═" * 60)

    # Guardar catálogo JSON
    catalogo_path = output_dir / "catalogo_tesis.json"
    catalogo = [
        {
            "uuid": t.uuid,
            "titulo": t.titulo,
            "autores": t.autores,
            "fecha": t.fecha,
            "coleccion": t.coleccion,
            "handle": t.handle,
            "url": f"https://repositorio.utp.edu.pe/handle/{t.handle}",
        }
        for t in todas_las_tesis
    ]
    with open(catalogo_path, "w", encoding="utf-8") as f:
        json.dump(catalogo, f, ensure_ascii=False, indent=2)
    log.info("Catálogo guardado en: %s", catalogo_path)

    if args.dry_run:
        log.info("\n─── MODO DRY-RUN: listado de tesis ───")
        for i, t in enumerate(todas_las_tesis, 1):
            autores = ", ".join(t.autores) if t.autores else "sin autor"
            print(f"  {i:3d}. [{t.fecha}] [{t.coleccion}] {autores} — {t.titulo}")
        log.info("Dry-run completo. No se descargó nada.")
        return

    # Preparar archivo CSV resumen
    csv_path = output_dir / "resumen_descargas.csv"
    csv_file = open(csv_path, "w", newline="", encoding="utf-8-sig")
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(["Coleccion", "Año", "Autores", "Titulo", "Handle", "URL", "Estado", "Archivos", "Detalle"])
    csv_lock = threading.Lock()

    # ── Fase 2: Descarga concurrente ──
    exitos = 0
    fallos = 0
    total = len(todas_las_tesis)

    log.info("Iniciando descarga con %d hilos en paralelo...", args.workers)

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(descargar_tesis_worker, tesis, output_dir): tesis
            for tesis in todas_las_tesis
        }

        for i, future in enumerate(as_completed(futures), 1):
            tesis = futures[future]
            try:
                t, ok, msg, archivos = future.result()
                status_icon = "✓" if ok else "✗"
                log.info("[%d/%d] %s %s | %s", i, total, status_icon, t.coleccion, t.titulo[:75])
                
                estado_str = "OK" if ok else ("EMBARGADA" if "EMBARGADA" in msg else "ERROR")
                with csv_lock:
                    csv_writer.writerow([
                        t.coleccion,
                        t.fecha,
                        "; ".join(t.autores),
                        t.titulo,
                        t.handle,
                        f"https://repositorio.utp.edu.pe/handle/{t.handle}",
                        estado_str,
                        "; ".join(archivos),
                        msg
                    ])
                    csv_file.flush()

                if ok:
                    exitos += 1
                else:
                    fallos += 1
            except Exception as e:
                log.error("[%d/%d] ✗ Error en '%s': %s", i, total, tesis.titulo[:60], e)
                with csv_lock:
                    csv_writer.writerow([
                        tesis.coleccion,
                        tesis.fecha,
                        "; ".join(tesis.autores),
                        tesis.titulo,
                        tesis.handle,
                        f"https://repositorio.utp.edu.pe/handle/{tesis.handle}",
                        "ERROR",
                        "",
                        str(e)
                    ])
                    csv_file.flush()
                fallos += 1

    csv_file.close()

    # ── Resumen final ──
    log.info("═" * 60)
    log.info("DESCARGA COMPLETADA")
    log.info("  Exitosas: %d", exitos)
    log.info("  Fallidas: %d", fallos)
    log.info("  CSV: %s", csv_path.resolve())
    log.info("  Directorio: %s", output_dir.resolve())
    log.info("═" * 60)


if __name__ == "__main__":
    main()
