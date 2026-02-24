from __future__ import annotations

import os
from functools import wraps

from flask import (
    Flask, render_template, request, redirect, url_for,
    session, send_from_directory
)
from werkzeug.security import generate_password_hash, check_password_hash

import backend

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-key")

backend.init_database()


# -------------------------
# UTENTE ADMIN DI DEFAULT
# -------------------------
def ensure_admin():
    try:
        u = backend.get_utente("admin")
    except Exception:
        u = None
    if not u:
        backend.crea_utente("admin", generate_password_hash("admin123"))


ensure_admin()


# -------------------------
# HELPERS
# -------------------------
def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not session.get("user_id"):
            return redirect(url_for("login"))
        return fn(*args, **kwargs)
    return wrapper


def parse_int(val: str | None, default: int = 0) -> int:
    try:
        s = (val or "").strip()
        return int(s) if s else default
    except Exception:
        return default


def parse_float(val: str | None, default: float = 0.0) -> float:
    try:
        s = (val or "").strip().replace(",", ".")
        return float(s) if s else default
    except Exception:
        return default


def _filter_default_color(colori: list[str]) -> list[str]:
    return [c for c in colori if (c or "").strip().upper() != "DEFAULT"]


# -------------------------
# LOGIN / LOGOUT
# -------------------------
@app.get("/login")
def login():
    return render_template("login.html", error=None)


@app.post("/login")
def login_post():
    username = (request.form.get("username") or "").strip()
    password = request.form.get("password") or ""

    u = backend.get_utente(username)
    if not u:
        return render_template("login.html", error="Credenziali non valide.")

    user_id, uname, pw_hash = u
    if not check_password_hash(pw_hash, password):
        return render_template("login.html", error="Credenziali non valide.")

    session["user_id"] = user_id
    session["username"] = uname
    return redirect(url_for("products"))


@app.get("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


# -------------------------
# HOME
# -------------------------
@app.get("/")
def root():
    if not session.get("user_id"):
        return redirect(url_for("login"))
    return redirect(url_for("products"))


# -------------------------
# SERVE IMMAGINI
# -------------------------
@app.get("/img/<path:filename>")
@login_required
def serve_img(filename):
    folder = os.path.join("DATI", "immagini_prodotti")
    return send_from_directory(folder, filename)


# -------------------------
# LISTA PRODOTTI
# -------------------------
@app.get("/products")
@login_required
def products():
    q = (request.args.get("q") or "").strip()
    rows = backend.lista_prodotti_overview(q)
    return render_template("products.html", rows=rows, q=q)


# -------------------------
# CATEGORIE
# -------------------------
@app.route("/categories", methods=["GET", "POST"])
@login_required
def categories():
    if request.method == "POST":
        nome = (request.form.get("nome") or "").strip()
        if nome:
            backend.aggiungi_categoria(nome)
        return redirect(url_for("categories"))

    cats = backend.lista_categorie()
    return render_template("categories.html", cats=cats)


# -------------------------
# NUOVO PRODOTTO
# -------------------------
@app.route("/product/new", methods=["GET", "POST"])
@login_required
def product_new():
    categorie = backend.lista_categorie()

    if request.method == "GET":
        p = {
            "codice": "",
            "nome": "",
            "categoria": "",
            "materiali": "",
            "descrizione": "",
            "produzione": "",
            "venduti": "0",
            "costo": "0",
            "prezzo_range": "",
            "tipo_taglie": "NUMERI",
        }
        return render_template(
            "product_form.html",
            p=p,
            categorie=categorie,
            is_new=True,
            colori=[],
            colore="",
            taglie=[],
            immagini=[],
            msg=None,
            error=None,
        )

    try:
        codice = (request.form.get("codice") or "").strip()
        nome = (request.form.get("nome") or "").strip()
        if not codice or not nome:
            raise ValueError("Codice e Nome sono obbligatori.")

        categoria = (request.form.get("categoria") or "").strip()
        materiali = (request.form.get("materiali") or "").strip()
        descrizione = (request.form.get("descrizione") or "").strip()
        produzione = (request.form.get("produzione") or "").strip()
        venduti = parse_int(request.form.get("venduti"), 0)
        costo = parse_float(request.form.get("costo"), 0.0)
        prezzo_range = (request.form.get("prezzo_range") or "").strip()
        tipo_taglie = (request.form.get("tipo_taglie") or "NUMERI").strip().upper()

        backend.crea_prodotto(
            codice, nome, categoria, "",
            materiali, descrizione,
            costo, prezzo_range, produzione, tipo_taglie
        )
        backend.aggiorna_venduti(codice, venduti)

        return redirect(url_for("product_edit", codice=codice))

    except Exception as e:
        p = {
            "codice": request.form.get("codice", ""),
            "nome": request.form.get("nome", ""),
            "categoria": request.form.get("categoria", ""),
            "materiali": request.form.get("materiali", ""),
            "descrizione": request.form.get("descrizione", ""),
            "produzione": request.form.get("produzione", ""),
            "venduti": request.form.get("venduti", "0"),
            "costo": request.form.get("costo", "0"),
            "prezzo_range": request.form.get("prezzo_range", ""),
            "tipo_taglie": request.form.get("tipo_taglie", "NUMERI"),
        }
        return render_template(
            "product_form.html",
            p=p,
            categorie=categorie,
            is_new=True,
            colori=[],
            colore="",
            taglie=[],
            immagini=[],
            msg=None,
            error=str(e),
        )


# -------------------------
# SCHEDA PRODOTTO (GET/POST) + STOCK SOTTO LA SCHEDA
# -------------------------
@app.route("/product/<path:codice>", methods=["GET", "POST"])
@login_required
def product_edit(codice):
    prod = backend.get_prodotto(codice)
    if not prod:
        return redirect(url_for("products"))

    categorie = backend.lista_categorie()
    colori = _filter_default_color(backend.lista_colori(codice))

    colore = (request.args.get("colore") or "").strip()
    msg = (request.args.get("msg") or "").strip()

    tipo_taglie_db = (prod[7] or "NUMERI").upper()

    # immagini
    immagini = []
    try:
        imgs_raw = backend.lista_immagini(codice)
        for (img_id, path_file, ordine, principale) in imgs_raw:
            filename = os.path.basename(path_file)
            immagini.append({"id": img_id, "filename": filename, "ordine": ordine, "principale": principale})
    except Exception:
        immagini = []

    # taglie (solo se colore selezionato)
    taglie = []
    if colore:
        backend.ensure_stock_colore(codice, colore, tipo_taglie_db)
        raw = backend.get_taglie_colore(codice, colore)
        taglie = [{"taglia": r[0], "quantita": r[1]} for r in raw]

    # -------- GET --------
    if request.method == "GET":
        p = {
            "codice": prod[0] or "",
            "nome": prod[1] or "",
            "categoria": prod[2] or "",
            "materiali": prod[4] or "",
            "descrizione": prod[5] or "",
            "produzione": prod[6] or "",
            "tipo_taglie": tipo_taglie_db,
            "venduti": str(prod[8] or 0),
            "costo": str(prod[9] or 0),
            "prezzo_range": prod[10] or "",
        }
        return render_template(
            "product_form.html",
            p=p,
            categorie=categorie,
            is_new=False,
            colori=colori,
            colore=colore,
            taglie=taglie,
            immagini=immagini,
            msg=msg or None,
            error=None,
        )

    # -------- POST (salva) --------
    try:
        tipo_vecchio = tipo_taglie_db

        nome = (request.form.get("nome") or "").strip()
        if not nome:
            raise ValueError("Il campo Nome è obbligatorio.")

        categoria = (request.form.get("categoria") or "").strip()
        materiali = (request.form.get("materiali") or "").strip()
        descrizione = (request.form.get("descrizione") or "").strip()
        produzione = (request.form.get("produzione") or "").strip()

        venduti = parse_int(request.form.get("venduti"), 0)
        costo = parse_float(request.form.get("costo"), 0.0)
        prezzo_range = (request.form.get("prezzo_range") or "").strip()
        tipo_taglie_new = (request.form.get("tipo_taglie") or "NUMERI").strip().upper()

        backend.crea_prodotto(
            codice, nome, categoria, "",
            materiali, descrizione,
            costo, prezzo_range, produzione, tipo_taglie_new
        )
        backend.aggiorna_venduti(codice, venduti)

        # cambio tipo taglie => reset stock (quantità azzerate)
        if tipo_vecchio != tipo_taglie_new:
            backend.reset_stock_prodotto(codice, tipo_taglie_new)
            m = "✅ Tipo taglie cambiato: stock ricreato (quantità azzerate)."
        else:
            m = "✅ Salvato."

        if colore:
            return redirect(url_for("product_edit", codice=codice, colore=colore, msg=m))
        return redirect(url_for("product_edit", codice=codice, msg=m))

    except Exception as e:
        p = {
            "codice": codice,
            "nome": request.form.get("nome", ""),
            "categoria": request.form.get("categoria", ""),
            "materiali": request.form.get("materiali", ""),
            "descrizione": request.form.get("descrizione", ""),
            "produzione": request.form.get("produzione", ""),
            "tipo_taglie": request.form.get("tipo_taglie", tipo_taglie_db),
            "venduti": request.form.get("venduti", "0"),
            "costo": request.form.get("costo", "0"),
            "prezzo_range": request.form.get("prezzo_range", ""),
        }
        return render_template(
            "product_form.html",
            p=p,
            categorie=categorie,
            is_new=False,
            colori=colori,
            colore=colore,
            taglie=taglie,
            immagini=immagini,
            msg=None,
            error=str(e),
        )


# -------------------------
# UPLOAD IMMAGINE
# -------------------------
@app.post("/product/<path:codice>/image/upload")
@login_required
def product_image_upload(codice):
    f = request.files.get("image")
    if not f or not f.filename:
        return redirect(url_for("product_edit", codice=codice))

    os.makedirs(os.path.join("DATI", "_tmp_upload"), exist_ok=True)
    tmp_path = os.path.join("DATI", "_tmp_upload", f.filename)
    f.save(tmp_path)

    try:
        backend.aggiungi_immagine(codice, tmp_path)
    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass

    return redirect(url_for("product_edit", codice=codice))


# -------------------------
# AGGIUNGI COLORE (crea anche le taglie)
# -------------------------
@app.post("/product/<path:codice>/color/add")
@login_required
def product_color_add(codice):
    nuovo_colore = (request.form.get("nuovo_colore") or "").strip()
    if not nuovo_colore:
        return redirect(url_for("product_edit", codice=codice))

    prod = backend.get_prodotto(codice)
    tipo_taglie = (prod[7] or "NUMERI").upper() if prod else "NUMERI"

    backend.aggiungi_colore(codice, nuovo_colore)
    backend.ensure_stock_colore(codice, nuovo_colore, tipo_taglie)

    return redirect(url_for("product_edit", codice=codice, colore=nuovo_colore))


# -------------------------
# CARICO/SCARICO
# -------------------------
@app.post("/product/<path:codice>/stock/move")
@login_required
def stock_move(codice):
    colore = (request.form.get("colore") or "").strip()
    taglia = (request.form.get("taglia") or "").strip()
    azione = (request.form.get("azione") or "").strip()
    qta = parse_int(request.form.get("qta"), 0)

    if not colore or not taglia or qta <= 0:
        return redirect(url_for("product_edit", codice=codice, colore=colore))

    try:
        prod = backend.get_prodotto(codice)
        tipo_taglie = (prod[7] or "NUMERI").upper() if prod else "NUMERI"
        backend.ensure_stock_colore(codice, colore, tipo_taglie)

        if azione == "carico":
            backend.carica_stock_colore(codice, colore, taglia, qta)
            m = f"✅ Caricati {qta} su {taglia} ({colore})"
        else:
            backend.scarica_vendita_colore(codice, colore, taglia, qta)
            m = f"✅ Scaricati {qta} su {taglia} ({colore})"

    except Exception as e:
        m = f"❌ Errore: {e}"

    return redirect(url_for("product_edit", codice=codice, colore=colore, msg=m))


if __name__ == "__main__":
    app.run()