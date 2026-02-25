import sqlite3
import os
import shutil
import uuid
from datetime import datetime
import time

DB_PATH = os.path.join("DATI", "database.db")


# ------------------------
# BACKUP DATABASE
# ------------------------

def backup_database(max_backups: int = 30):
    """
    Crea una copia del DB in DATI/backup/ ad ogni avvio.
    Mantiene al massimo gli ultimi `max_backups` file.
    """
    if not os.path.isfile(DB_PATH):
        return

    os.makedirs(os.path.join("DATI", "backup"), exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = os.path.join("DATI", "backup", f"database_{ts}.db")

    try:
        shutil.copy2(DB_PATH, dest)
    except:
        return

    # Cleanup vecchi backup
    try:
        backup_dir = os.path.join("DATI", "backup")
        files = []
        for f in os.listdir(backup_dir):
            if f.lower().startswith("database_") and f.lower().endswith(".db"):
                files.append(os.path.join(backup_dir, f))
        files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
        for old in files[max_backups:]:
            try:
                os.remove(old)
            except:
                pass
    except:
        pass


# ------------------------
# DB HELPERS
# ------------------------

def _conn():
    return sqlite3.connect(DB_PATH)


def _ensure_column(cur, table, col, coltype):
    cur.execute(f"PRAGMA table_info({table})")
    cols = [r[1] for r in cur.fetchall()]
    if col not in cols:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype}")


def _taglie_for_tipo(tipo_taglie: str):
    return ["XS", "S", "M", "L", "XL"] if (tipo_taglie or "").upper() == "LETTERE" else ["38", "40", "42", "44", "46", "48", "50", "52"]


# ------------------------
# INIZIALIZZAZIONE + MIGRAZIONI DATABASE
# ------------------------

def init_database():
    os.makedirs("DATI", exist_ok=True)
    conn = _conn()
    cur = conn.cursor()

    # Tabelle base
    cur.execute("""
    CREATE TABLE IF NOT EXISTS categorie (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT UNIQUE
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS prodotti (
        codice TEXT PRIMARY KEY,
        nome TEXT,
        categoria TEXT,
        colore TEXT,              -- legacy: colore singolo (serve per migrazione/compatibilità)
        materiali TEXT,
        descrizione TEXT,
        costo_unitario REAL,
        prezzo_range TEXT,
        produzione TEXT,
        tipo_taglie TEXT,
        venduti INTEGER DEFAULT 0,
        created_at TEXT,
        updated_at TEXT
    )
    """)

    # Colori multipli (nuovo)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS colori_prodotti (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        codice_prodotto TEXT,
        colore TEXT,
        UNIQUE(codice_prodotto, colore),
        FOREIGN KEY (codice_prodotto) REFERENCES prodotti(codice)
    )
    """)

    # Stock per colore + taglia (stessa tabella, ma con colonna colore)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS taglie_prodotti (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        codice_prodotto TEXT,
        colore TEXT,
        taglia TEXT,
        quantita INTEGER,
        FOREIGN KEY (codice_prodotto) REFERENCES prodotti(codice)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS immagini_prodotti (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        codice_prodotto TEXT,
        percorso_file TEXT,
        ordine INTEGER,
        principale INTEGER,
        FOREIGN KEY (codice_prodotto) REFERENCES prodotti(codice)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS utenti (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        created_at TEXT
    )
    """)

    # --- MIGRAZIONI ---
    _ensure_column(cur, "prodotti", "categoria", "TEXT")
    _ensure_column(cur, "prodotti", "colore", "TEXT")
    _ensure_column(cur, "prodotti", "created_at", "TEXT")
    _ensure_column(cur, "prodotti", "updated_at", "TEXT")

    _ensure_column(cur, "taglie_prodotti", "colore", "TEXT")  # fondamentale per multi-colore

    # Inizializza timestamp su vecchi record
    cur.execute("""
        UPDATE prodotti
        SET created_at = COALESCE(created_at, datetime('now')),
            updated_at = COALESCE(updated_at, datetime('now'))
    """)

    # --- MIGRAZIONE DATI: da colore singolo -> colori_prodotti + taglie_prodotti.colore ---
    # Idempotente: INSERT OR IGNORE, UPDATE solo quando colore è NULL/vuoto.
    try:
        cur.execute("SELECT codice, COALESCE(colore,'') FROM prodotti")
        for cod, legacy_col in cur.fetchall():
            col = (legacy_col or "").strip() or "DEFAULT"

            # registra colore nella tabella colori
            cur.execute(
                "INSERT OR IGNORE INTO colori_prodotti(codice_prodotto, colore) VALUES(?,?)",
                (cod, col)
            )

            # assegna colore alle righe stock vecchie (se ancora NULL/empty)
            cur.execute("""
                UPDATE taglie_prodotti
                SET colore = ?
                WHERE codice_prodotto = ? AND (colore IS NULL OR TRIM(colore) = '')
            """, (col, cod))
    except:
        pass

    conn.commit()
    conn.close()


# ------------------------
# COLORI (NUOVO)
# ------------------------

def lista_colori(codice_prodotto: str):
    conn = _conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT colore
        FROM colori_prodotti
        WHERE codice_prodotto = ?
        ORDER BY colore COLLATE NOCASE
    """, (codice_prodotto,))
    rows = [r[0] for r in cur.fetchall()]
    conn.close()
    colori = [c for c in colori if c != "DEFAULT"]
    return rows


def aggiungi_colore(codice_prodotto: str, colore: str):
    colore = (colore or "").strip()
    if not colore:
        return
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO colori_prodotti(codice_prodotto, colore) VALUES(?,?)",
        (codice_prodotto, colore)
    )
    conn.commit()
    conn.close()


def rinomina_colore(codice_prodotto: str, vecchio: str, nuovo: str):
    vecchio = (vecchio or "").strip()
    nuovo = (nuovo or "").strip()
    if not vecchio or not nuovo:
        return
    conn = _conn()
    cur = conn.cursor()

    # se esiste già nuovo, unisci (evita duplicati)
    cur.execute("""
        SELECT COUNT(*) FROM colori_prodotti
        WHERE codice_prodotto=? AND colore=?
    """, (codice_prodotto, nuovo))
    exists_new = int(cur.fetchone()[0] or 0) > 0

    if not exists_new:
        cur.execute("""
            UPDATE colori_prodotti
            SET colore=?
            WHERE codice_prodotto=? AND colore=?
        """, (nuovo, codice_prodotto, vecchio))
    else:
        # se nuovo esiste, elimina vecchio e sposta stock sul nuovo
        cur.execute("""
            DELETE FROM colori_prodotti
            WHERE codice_prodotto=? AND colore=?
        """, (codice_prodotto, vecchio))

    # aggiorna stock
    cur.execute("""
        UPDATE taglie_prodotti
        SET colore=?
        WHERE codice_prodotto=? AND colore=?
    """, (nuovo, codice_prodotto, vecchio))

    # aggiornamento timestamp
    cur.execute("""
        UPDATE prodotti SET updated_at=datetime('now') WHERE codice=?
    """, (codice_prodotto,))

    conn.commit()
    conn.close()


def elimina_colore(codice_prodotto: str, colore: str):
    colore = (colore or "").strip()
    if not colore:
        return
    conn = _conn()
    cur = conn.cursor()

    cur.execute("DELETE FROM taglie_prodotti WHERE codice_prodotto=? AND colore=?", (codice_prodotto, colore))
    cur.execute("DELETE FROM colori_prodotti WHERE codice_prodotto=? AND colore=?", (codice_prodotto, colore))

    cur.execute("""
        UPDATE prodotti SET updated_at=datetime('now') WHERE codice=?
    """, (codice_prodotto,))

    conn.commit()
    conn.close()


def ensure_stock_colore(codice_prodotto: str, colore: str, tipo_taglie: str):
    """
    Assicura che per (prodotto, colore) esistano tutte le righe stock per le taglie del tipo scelto.
    Non modifica quantità esistenti.
    """
    colore = (colore or "").strip() or "DEFAULT"
    taglie = _taglie_for_tipo(tipo_taglie)

    conn = _conn()
    cur = conn.cursor()

    # assicura che il colore esista anche nella tabella colori
    cur.execute(
        "INSERT OR IGNORE INTO colori_prodotti(codice_prodotto, colore) VALUES(?,?)",
        (codice_prodotto, colore)
    )

    for t in taglie:
        cur.execute("""
            SELECT COUNT(*)
            FROM taglie_prodotti
            WHERE codice_prodotto=? AND colore=? AND taglia=?
        """, (codice_prodotto, colore, t))
        if int(cur.fetchone()[0] or 0) == 0:
            cur.execute("""
                INSERT INTO taglie_prodotti(codice_prodotto, colore, taglia, quantita)
                VALUES(?,?,?,0)
            """, (codice_prodotto, colore, t))

    cur.execute("""
        UPDATE prodotti SET updated_at=datetime('now') WHERE codice=?
    """, (codice_prodotto,))

    conn.commit()
    conn.close()

def reset_stock_prodotto(codice_prodotto: str, tipo_taglie: str):
    """
    Quando cambia il tipo taglie (NUMERI/LETTERE), azzera e ricrea lo stock del prodotto.
    """
    conn = _conn()
    cur = conn.cursor()

    # Cancella tutte le righe stock del prodotto
    cur.execute("DELETE FROM taglie_prodotti WHERE codice_prodotto=?", (codice_prodotto,))

    # Prende i colori (NOTA: colori_prodotti NON ha colonna 'ordine', quindi ordiniamo per colore)
    cur.execute(
        "SELECT colore FROM colori_prodotti WHERE codice_prodotto=? ORDER BY colore COLLATE NOCASE",
        (codice_prodotto,)
    )
    colori = [r[0] for r in cur.fetchall()]

    conn.commit()
    conn.close()

    # Ricrea righe stock per ogni colore
    for c in colori:
        ensure_stock_colore(codice_prodotto, c, tipo_taglie)

def get_taglie_colore(codice_prodotto: str, colore: str):
    conn = _conn()
    cur = conn.cursor()

    # Recupera tipo_taglie del prodotto
    cur.execute(
        "SELECT tipo_taglie FROM prodotti WHERE codice=?",
        (codice_prodotto,)
    )
    row = cur.fetchone()
    tipo_taglie = (row[0] or "NUMERI").upper() if row else "NUMERI"

    if tipo_taglie == "LETTERE":
        # Ordine logico per taglie lettere
        cur.execute("""
            SELECT taglia, quantita
            FROM taglie_prodotti
            WHERE codice_prodotto=? AND colore=?
            ORDER BY
              CASE taglia
                WHEN 'XS' THEN 1
                WHEN 'S'  THEN 2
                WHEN 'M'  THEN 3
                WHEN 'L'  THEN 4
                WHEN 'XL' THEN 5
                WHEN 'XXL' THEN 6
                ELSE 999
              END
        """, (codice_prodotto, colore))
    else:
        # Numeri ordinati normalmente
        cur.execute("""
            SELECT taglia, quantita
            FROM taglie_prodotti
            WHERE codice_prodotto=? AND colore=?
            ORDER BY CAST(taglia AS INTEGER)
        """, (codice_prodotto, colore))

    rows = cur.fetchall()
    conn.close()
    return rows


def get_rimanenza_colore(codice_prodotto: str, colore: str) -> int:
    colore = (colore or "").strip() or "DEFAULT"
    conn = _conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT COALESCE(SUM(quantita),0)
        FROM taglie_prodotti
        WHERE codice_prodotto=? AND colore=?
    """, (codice_prodotto, colore))
    tot = int(cur.fetchone()[0] or 0)
    conn.close()
    return tot


def get_rimanenza_totale(codice_prodotto: str) -> int:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT COALESCE(SUM(quantita),0)
        FROM taglie_prodotti
        WHERE codice_prodotto=?
    """, (codice_prodotto,))
    tot = int(cur.fetchone()[0] or 0)
    conn.close()
    return tot


# ------------------------
# STOCK (PER COLORE) + WRAPPER COMPATIBILITÀ
# ------------------------

def aggiorna_taglia_colore(codice_prodotto: str, colore: str, taglia: str, nuova_quantita: int):
    colore = (colore or "").strip() or "DEFAULT"
    conn = _conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE taglie_prodotti
        SET quantita = ?
        WHERE codice_prodotto = ? AND colore = ? AND taglia = ?
    """, (int(nuova_quantita), codice_prodotto, colore, str(taglia)))
    cur.execute("UPDATE prodotti SET updated_at=datetime('now') WHERE codice=?", (codice_prodotto,))
    conn.commit()
    conn.close()


def carica_stock_colore(codice_prodotto: str, colore: str, taglia: str, qty: int) -> int:
    qty = int(qty)
    if qty < 1:
        raise ValueError("La quantità da caricare deve essere almeno 1.")
    colore = (colore or "").strip() or "DEFAULT"

    conn = _conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT quantita
        FROM taglie_prodotti
        WHERE codice_prodotto=? AND colore=? AND taglia=?
    """, (codice_prodotto, colore, str(taglia)))
    row = cur.fetchone()
    if not row:
        conn.close()
        raise ValueError("Taglia non trovata per questo prodotto/colore. Salva prima il prodotto e assicurati che il colore esista.")

    cur.execute("""
        UPDATE taglie_prodotti
        SET quantita = COALESCE(quantita, 0) + ?
        WHERE codice_prodotto=? AND colore=? AND taglia=?
    """, (qty, codice_prodotto, colore, str(taglia)))

    cur.execute("UPDATE prodotti SET updated_at=datetime('now') WHERE codice=?", (codice_prodotto,))

    cur.execute("""
        SELECT quantita
        FROM taglie_prodotti
        WHERE codice_prodotto=? AND colore=? AND taglia=?
    """, (codice_prodotto, colore, str(taglia)))
    new_q = int(cur.fetchone()[0] or 0)

    conn.commit()
    conn.close()
    return new_q


def scarica_vendita_colore(codice_prodotto: str, colore: str, taglia: str, qty: int) -> int:
    qty = int(qty)
    if qty < 1:
        raise ValueError("La quantità da scaricare deve essere almeno 1.")
    colore = (colore or "").strip() or "DEFAULT"

    conn = _conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT quantita
        FROM taglie_prodotti
        WHERE codice_prodotto=? AND colore=? AND taglia=?
    """, (codice_prodotto, colore, str(taglia)))
    row = cur.fetchone()
    if not row:
        conn.close()
        raise ValueError("Taglia non trovata per questo prodotto/colore. Salva prima il prodotto e assicurati che il colore esista.")

    attuale = int(row[0] or 0)
    if attuale < qty:
        conn.close()
        raise ValueError(f"Stock insufficiente: disponibili {attuale}, richiesti {qty}.")

    nuova_q = attuale - qty

    cur.execute("""
        UPDATE taglie_prodotti
        SET quantita = ?
        WHERE codice_prodotto=? AND colore=? AND taglia=?
    """, (nuova_q, codice_prodotto, colore, str(taglia)))

    cur.execute("""
        UPDATE prodotti
        SET venduti = COALESCE(venduti, 0) + ?,
            updated_at = datetime('now')
        WHERE codice = ?
    """, (qty, codice_prodotto))

    conn.commit()
    conn.close()
    return nuova_q


# --- FUNZIONI "LEGACY" (compatibilità) ---
def aggiorna_taglia(codice_prodotto, taglia, nuova_quantita, colore=None):
    """
    Compatibilità: se colore è None aggiorna la prima occorrenza (o DEFAULT).
    Consigliato usare aggiorna_taglia_colore.
    """
    if colore is None:
        colori = lista_colori(codice_prodotto)
        colore = colori[0] if colori else "DEFAULT"
    aggiorna_taglia_colore(codice_prodotto, colore, taglia, nuova_quantita)


def carica_stock(codice_prodotto, taglia, qty, colore=None):
    if colore is None:
        colori = lista_colori(codice_prodotto)
        colore = colori[0] if colori else "DEFAULT"
    return carica_stock_colore(codice_prodotto, colore, taglia, qty)


def scarica_vendita(codice_prodotto, taglia, qty, colore=None):
    if colore is None:
        colori = lista_colori(codice_prodotto)
        colore = colori[0] if colori else "DEFAULT"
    return scarica_vendita_colore(codice_prodotto, colore, taglia, qty)


def get_taglie(codice_prodotto):
    """
    Compatibilità: ritorna le taglie del primo colore.
    """
    colori = lista_colori(codice_prodotto)
    col = colori[0] if colori else "DEFAULT"
    return get_taglie_colore(codice_prodotto, col)


# ------------------------
# CREAZIONE / AGGIORNAMENTO PRODOTTO
# ------------------------

def crea_prodotto(codice, nome, categoria, colore_legacy, materiali, descrizione,
                  costo, prezzo_range, produzione, tipo_taglie):
    """
    - Mantiene 'prodotti.colore' come legacy (può essere vuoto).
    - Assicura almeno 1 colore in colori_prodotti (usa colore_legacy se dato, altrimenti DEFAULT).
    - Assicura righe stock per quel colore.
    - Se cambia tipo_taglie, ricrea righe stock per OGNI colore (quantità a 0).
    """
    codice = (codice or "").strip()
    if not codice:
        raise ValueError("Codice prodotto mancante.")

    costo_val = float(costo) if costo not in (None, "") else 0.0
    col_default = (colore_legacy or "").strip() or "DEFAULT"

    conn = _conn()
    cur = conn.cursor()

    cur.execute("SELECT tipo_taglie FROM prodotti WHERE codice = ?", (codice,))
    row = cur.fetchone()

    if row is None:
        cur.execute("""
        INSERT INTO prodotti (
            codice, nome, categoria, colore, materiali, descrizione,
            costo_unitario, prezzo_range, produzione, tipo_taglie, venduti,
            created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, datetime('now'), datetime('now'))
        """, (
            codice, nome, categoria, (colore_legacy or "").strip(),
            materiali, descrizione,
            costo_val, prezzo_range, produzione, tipo_taglie
        ))

        # crea almeno un colore + stock
        cur.execute("INSERT OR IGNORE INTO colori_prodotti(codice_prodotto, colore) VALUES(?,?)", (codice, col_default))
        conn.commit()
        conn.close()

        ensure_stock_colore(codice, col_default, tipo_taglie)
        return

    # UPDATE
    tipo_precedente = row[0]
    cur.execute("""
        UPDATE prodotti
        SET nome=?, categoria=?, colore=?, materiali=?, descrizione=?,
            costo_unitario=?, prezzo_range=?,
            produzione=?, tipo_taglie=?,
            updated_at = datetime('now')
        WHERE codice=?
    """, (
        nome, categoria, (colore_legacy or "").strip(), materiali, descrizione,
        costo_val, prezzo_range,
        produzione, tipo_taglie, codice
    ))

    # assicura almeno un colore se manca
    cur.execute("SELECT COUNT(*) FROM colori_prodotti WHERE codice_prodotto=?", (codice,))
    ncol = int(cur.fetchone()[0] or 0)
    if ncol == 0:
        cur.execute("INSERT OR IGNORE INTO colori_prodotti(codice_prodotto, colore) VALUES(?,?)", (codice, col_default))

    conn.commit()
    conn.close()

    # Se cambia tipo taglie: ricrea stock per ogni colore (reset quantità)
    if (tipo_precedente or "").upper() != (tipo_taglie or "").upper():
        colori = lista_colori(codice)
        if not colori:
            colori = [col_default]

        conn = _conn()
        cur = conn.cursor()
        # cancella stock del prodotto e ricrea per tutti i colori
        cur.execute("DELETE FROM taglie_prodotti WHERE codice_prodotto=?", (codice,))
        conn.commit()
        conn.close()

        for c in colori:
            ensure_stock_colore(codice, c, tipo_taglie)


# ------------------------
# LISTA PRODOTTI (OVERVIEW)
# ------------------------

def lista_prodotti_overview(search_text: str = ""):
    """
    Colonne (ordine fisso):
    (codice, nome, categoria, colori, produzione, tipo_taglie, rimanenza_totale, venduti, costo_unitario, prezzo_range)
    """
    conn = _conn()
    cur = conn.cursor()

    q = (search_text or "").strip().lower()

    base_sql = """
        SELECT
            p.codice,
            p.nome,
            p.categoria,
            COALESCE((
                SELECT GROUP_CONCAT(DISTINCT cp.colore)
                FROM colori_prodotti cp
                WHERE cp.codice_prodotto = p.codice
            ), '') AS colori,
            p.produzione,
            p.tipo_taglie,
            COALESCE((
                SELECT SUM(t.quantita)
                FROM taglie_prodotti t
                WHERE t.codice_prodotto = p.codice
            ), 0) AS rimanenza_totale,
            p.venduti,
            p.costo_unitario,
            p.prezzo_range
        FROM prodotti p
    """

    if q:
        cur.execute(
            base_sql + """
            WHERE LOWER(p.codice) LIKE ? OR LOWER(p.nome) LIKE ?
            ORDER BY p.nome COLLATE NOCASE
            """,
            (f"%{q}%", f"%{q}%")
        )
    else:
        cur.execute(
            base_sql + """
            ORDER BY p.nome COLLATE NOCASE
            """
        )

    rows = cur.fetchall()
    conn.close()
    return rows


# ------------------------
# PRODOTTO
# ------------------------

def get_prodotto(codice_prodotto):
    """
    Ritorna la tupla:
    (codice, nome, categoria, colore_legacy, materiali, descrizione, produzione, tipo_taglie, venduti, costo_unitario, prezzo_range)
    """
    conn = _conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            codice,
            nome,
            categoria,
            colore,
            materiali,
            descrizione,
            produzione,
            tipo_taglie,
            venduti,
            costo_unitario,
            prezzo_range
        FROM prodotti
        WHERE codice = ?
    """, (codice_prodotto,))
    r = cur.fetchone()
    conn.close()
    return r


def aggiorna_venduti(codice_prodotto, venduti):
    conn = _conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE prodotti
        SET venduti = ?,
            updated_at = datetime('now')
        WHERE codice = ?
    """, (int(venduti), codice_prodotto))
    conn.commit()
    conn.close()


def elimina_prodotto(codice_prodotto):
    """
    Elimina TUTTO: stock, immagini, colori, prodotto.
    """
    conn = _conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM taglie_prodotti WHERE codice_prodotto = ?", (codice_prodotto,))
    cur.execute("DELETE FROM immagini_prodotti WHERE codice_prodotto = ?", (codice_prodotto,))
    cur.execute("DELETE FROM colori_prodotti WHERE codice_prodotto = ?", (codice_prodotto,))
    cur.execute("DELETE FROM prodotti WHERE codice = ?", (codice_prodotto,))
    conn.commit()
    conn.close()


# ------------------------
# IMMAGINI
# ------------------------

def aggiungi_immagine(codice_prodotto, percorso_originale):
    os.makedirs(os.path.join("DATI", "immagini_prodotti"), exist_ok=True)

    est = os.path.splitext(percorso_originale)[1].lower()
    if est not in [".jpg", ".jpeg", ".png", ".webp"]:
        raise ValueError("Formato immagine non supportato (usa jpg, png, webp).")

    nome_file = f"{codice_prodotto}_{uuid.uuid4().hex}{est}"
    destinazione = os.path.join("DATI", "immagini_prodotti", nome_file)
    shutil.copy2(percorso_originale, destinazione)

    conn = _conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT COALESCE(MAX(ordine), 0) + 1
        FROM immagini_prodotti
        WHERE codice_prodotto = ?
    """, (codice_prodotto,))
    ordine = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*)
        FROM immagini_prodotti
        WHERE codice_prodotto = ?
    """, (codice_prodotto,))
    cnt = cur.fetchone()[0]
    principale = 1 if cnt == 0 else 0

    cur.execute("""
        INSERT INTO immagini_prodotti (codice_prodotto, percorso_file, ordine, principale)
        VALUES (?, ?, ?, ?)
    """, (codice_prodotto, destinazione, ordine, principale))

    conn.commit()
    conn.close()
    return destinazione


def lista_immagini(codice_prodotto):
    conn = _conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, percorso_file, ordine, principale
        FROM immagini_prodotti
        WHERE codice_prodotto = ?
        ORDER BY ordine ASC
    """, (codice_prodotto,))
    rows = cur.fetchall()
    conn.close()
    return rows


def get_immagine_principale(codice_prodotto):
    conn = _conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, percorso_file
        FROM immagini_prodotti
        WHERE codice_prodotto = ? AND principale = 1
        LIMIT 1
    """, (codice_prodotto,))
    row = cur.fetchone()
    conn.close()
    return row


def elimina_immagine(id_immagine):
    conn = _conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT codice_prodotto, percorso_file
        FROM immagini_prodotti
        WHERE id = ?
    """, (id_immagine,))
    row = cur.fetchone()

    if not row:
        conn.close()
        return None

    codice_prodotto, path = row
    cur.execute("DELETE FROM immagini_prodotti WHERE id = ?", (id_immagine,))

    if path and os.path.isfile(path):
        try:
            os.remove(path)
        except:
            pass

    # setta come principale la prima rimasta
    cur.execute("""
        SELECT id
        FROM immagini_prodotti
        WHERE codice_prodotto = ?
        ORDER BY ordine ASC
        LIMIT 1
    """, (codice_prodotto,))
    r = cur.fetchone()
    if r:
        cur.execute("UPDATE immagini_prodotti SET principale = 1 WHERE id = ?", (r[0],))

    conn.commit()
    conn.close()
    return (codice_prodotto, path)


# ------------------------
# CATEGORIE (gestione completa)
# ------------------------

def lista_categorie():
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT nome FROM categorie ORDER BY nome COLLATE NOCASE")
    rows = [r[0] for r in cur.fetchall()]
    conn.close()
    return rows


def aggiungi_categoria(nome):
    nome = (nome or "").strip()
    if not nome:
        return
    conn = _conn()
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO categorie(nome) VALUES(?)", (nome,))
    conn.commit()
    conn.close()


def categoria_in_uso(nome) -> int:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM prodotti WHERE categoria = ?", (nome,))
    n = int(cur.fetchone()[0] or 0)
    conn.close()
    return n


def sposta_prodotti_categoria(cat_vecchia: str, cat_nuova: str):
    conn = _conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE prodotti
        SET categoria = ?,
            updated_at = datetime('now')
        WHERE categoria = ?
    """, (cat_nuova, cat_vecchia))
    conn.commit()
    conn.close()


def rinomina_categoria(vecchia: str, nuova: str):
    vecchia = (vecchia or "").strip()
    nuova = (nuova or "").strip()
    if not vecchia or not nuova:
        return

    conn = _conn()
    cur = conn.cursor()

    cur.execute("UPDATE categorie SET nome = ? WHERE nome = ?", (nuova, vecchia))
    cur.execute("""
        UPDATE prodotti
        SET categoria = ?,
            updated_at = datetime('now')
        WHERE categoria = ?
    """, (nuova, vecchia))

    conn.commit()
    conn.close()


def elimina_categoria(nome):
    nome = (nome or "").strip()
    if not nome:
        return
    conn = _conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM categorie WHERE nome=?", (nome,))
    conn.commit()
    conn.close()


# ------------------------
# BACKUP LIST + RIPRISTINO
# ------------------------

def lista_backup_database():
    backup_dir = os.path.join("DATI", "backup")
    if not os.path.isdir(backup_dir):
        return []

    files = []
    for f in os.listdir(backup_dir):
        if f.lower().startswith("database_") and f.lower().endswith(".db"):
            files.append(os.path.join(backup_dir, f))

    files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return files


def ripristina_da_backup(backup_path: str):
    """
    Ripristina DB da un file di backup.
    Prima crea una copia di sicurezza del DB attuale in DATI/backup/.
    """
    if not backup_path or not os.path.isfile(backup_path):
        raise ValueError("Backup non trovato.")

    os.makedirs("DATI", exist_ok=True)

    # Backup di sicurezza del DB attuale
    if os.path.isfile(DB_PATH):
        os.makedirs(os.path.join("DATI", "backup"), exist_ok=True)
        ts = time.strftime("%Y%m%d_%H%M%S")
        safety = os.path.join("DATI", "backup", f"database_safety_{ts}.db")
        try:
            shutil.copy2(DB_PATH, safety)
        except:
            pass

    # Ripristino
    shutil.copy2(backup_path, DB_PATH)

    # Dopo ripristino, assicurati che le tabelle/migrazioni siano allineate
    init_database()

def get_utente(username: str):
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT id, username, password_hash FROM utenti WHERE username = ?", (username,))
    row = cur.fetchone()
    conn.close()
    return row

def crea_utente(username: str, password_hash: str):
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO utenti(username, password_hash, created_at) VALUES(?,?,datetime('now'))",
        (username, password_hash)
    )
    conn.commit()
    conn.close()

def set_stock_colore(codice_prodotto: str, colore: str, mappa_taglie_quantita: dict):
    colore = (colore or "").strip() or "DEFAULT"
    conn = _conn()
    cur = conn.cursor()

    for taglia, qta in mappa_taglie_quantita.items():
        taglia = (taglia or "").strip()
        if not taglia:
            continue
        try:
            q = int(qta)
        except Exception:
            q = 0
        if q < 0:
            q = 0

        cur.execute("""
            UPDATE taglie_prodotti
            SET quantita=?
            WHERE codice_prodotto=? AND colore=? AND taglia=?
        """, (q, codice_prodotto, colore, taglia))

        if cur.rowcount == 0:
            cur.execute("""
                INSERT INTO taglie_prodotti(codice_prodotto, colore, taglia, quantita)
                VALUES (?,?,?,?)
            """, (codice_prodotto, colore, taglia, q))

    conn.commit()
    conn.close()

