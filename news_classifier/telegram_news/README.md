telegram_news
==============

Objetivo: herramienta sencilla para descargar mensajes de canales de Telegram y guardarlos en local en SQLite, sin infraestructura compleja.

Requisitos
- Python 3.10+ (recomendado 3.12)
- Cuenta de Telegram y credenciales de API:
  - Consigue `TELEGRAM_API_ID` y `TELEGRAM_API_HASH` en https://my.telegram.org
- Paquetes:
  - Ver `requirements.txt`

Instalación
1) Crear y activar entorno (opcional):
   - python -m venv .venv && source .venv/bin/activate
2) Instalar dependencias:
   - pip install -r telegram_news/requirements.txt
3) Configurar entorno:
   - Copia `telegram_news/.env.example` a `telegram_news/.env`
   - Rellena `TELEGRAM_API_ID`, `TELEGRAM_API_HASH` y (opcional) `TELEGRAM_PHONE`
4) Lista de canales:
   - Edita `telegram_news/channels.txt` y añade uno por línea, ej.:
     - @binance_announcements
     - t.me/cointelegraph
     - BBCBreaking

Uso
- Primera ejecución (pedirá el código de verificación en tu terminal):
  - python telegram_news/main.py

- Opciones:
  - --limit N       Descarga como máximo N mensajes nuevos por canal (por defecto ilimitado).
  - --db PATH       Ruta del archivo SQLite (por defecto: telegram_news/news.db).

Salida
- Base de datos: `telegram_news/news.db`
  - Tabla: `messages(channel TEXT, id INTEGER, date_unix INTEGER, sender_id TEXT, sender TEXT, views INTEGER, forwards INTEGER, replies INTEGER, text TEXT, PRIMARY KEY(channel,id))`
  - Incremental: inserta solo mensajes con `id` superior al último guardado por canal (upsert con `INSERT OR IGNORE`).

Consultas útiles (sqlite3):
```
sqlite3 telegram_news/news.db
sqlite> .headers on
sqlite> .mode column
sqlite> SELECT channel, COUNT(*) AS n FROM messages GROUP BY channel ORDER BY n DESC;
sqlite> SELECT datetime(date_unix, 'unixepoch') AS ts, substr(text,1,60) AS snippet FROM messages WHERE channel='BBCBreaking' ORDER BY id DESC LIMIT 5;
```

Notas
- Si un canal no es accesible o no existe, se registrará en el log y se continúa con el resto.
- La sesión de Telegram se guarda en `telegram_news/telegram_news.session`. Mantén este archivo privado.
- Para regenerar la sesión, borra el archivo `.session` y vuelve a ejecutar.


