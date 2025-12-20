import os
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple

import pandas as pd
from telegram import ReplyKeyboardMarkup, ReplyKeyboardRemove, Update
from telegram.ext import (
    Application,
    CommandHandler,
    ConversationHandler,
    MessageHandler,
    ContextTypes,
    filters,
)


ADD_TITLE, ADD_EXISTS_RATING, ADD_DATE, ADD_DURATION, ADD_NEW_DETAILS, ADD_NEW_RATING = range(
    6
)

DB_PATH = "tracker.db"
CATALOG_CSV = "imdb.csv"
BOT_TOKEN = ""


main_keyboard = [["/add", "/last"], ["/stats", "/recommend"], ["/progress", "/help"]]
main_markup = ReplyKeyboardMarkup(main_keyboard, one_time_keyboard=False, resize_keyboard=True)

def get_conn():
    return sqlite3.connect(DB_PATH)


def init_db() -> None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS catalog (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT COLLATE NOCASE,
            type TEXT,
            genre TEXT,
            certificate TEXT,
            imdb_rate REAL,
            votes INTEGER,
            episodes INTEGER
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS views (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            name TEXT,
            type TEXT,
            genre TEXT,
            certificate TEXT,
            imdb_rate REAL,
            user_rate REAL,
            view_date TEXT,
            duration_minutes INTEGER
        )
        """
    )
    conn.commit()
    conn.close()


def load_catalog_if_empty() -> None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM catalog")
    count = cur.fetchone()[0]
    if count > 0:
        conn.close()
        return

    if not os.path.exists(CATALOG_CSV):
        conn.close()
        return

    df = pd.read_csv(CATALOG_CSV)
    column_mapping = {}
    if "Data" in df.columns:
        column_mapping["Data"] = "Date"
    if "Nudity, violence.." in df.columns:
        column_mapping["Nudity, violence.."] = "Content_Rating"
    df = df.rename(columns=column_mapping)

    if "Rate" in df.columns:
        df["Rate"] = pd.to_numeric(df["Rate"], errors="coerce")
    if "Votes" in df.columns:
        df["Votes"] = pd.to_numeric(df["Votes"], errors="coerce")
    if "Episodes" in df.columns:
        df["Episodes"] = pd.to_numeric(df["Episodes"], errors="coerce").fillna(1)

    rows: List[Tuple[Any, ...]] = []
    for _, row in df.iterrows():
        rows.append(
            (
                row.get("Name", ""),
                row.get("Type", ""),
                row.get("Genre", ""),
                row.get("Certificate", ""),
                row.get("Rate", None),
                row.get("Votes", None),
                row.get("Episodes", None),
            )
        )
    cur.executemany(
        """
        INSERT INTO catalog (name, type, genre, certificate, imdb_rate, votes, episodes)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()
    conn.close()


def find_in_catalog(title: str):
    pattern = title.strip()
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT name, type, genre, certificate, imdb_rate
        FROM catalog
        WHERE name LIKE ? COLLATE NOCASE
        LIMIT 1
        """,
        (pattern,),
    )
    row = cur.fetchone()
    if not row:
        cur.execute(
            """
            SELECT name, type, genre, certificate, imdb_rate
            FROM catalog
            WHERE name LIKE ? COLLATE NOCASE
            ORDER BY imdb_rate DESC NULLS LAST
            LIMIT 1
            """,
            (f"%{pattern}%",),
        )
        row = cur.fetchone()
    conn.close()
    if row:
        return {
            "name": row[0],
            "type": row[1],
            "genre": row[2],
            "certificate": row[3],
            "imdb_rate": row[4],
        }
    return None


def fuzzy_catalog(title: str, limit: int = 5) -> List[Dict[str, Any]]:
    pattern = f"%{title.strip()}%"
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT name, type, genre, certificate, imdb_rate
        FROM catalog
        WHERE name LIKE ?
        ORDER BY imdb_rate DESC NULLS LAST
        LIMIT ?
        """,
        (pattern, limit),
    )
    rows = cur.fetchall()
    conn.close()
    return [
        {
            "name": r[0],
            "type": r[1],
            "genre": r[2],
            "certificate": r[3],
            "imdb_rate": r[4],
        }
        for r in rows
    ]


def insert_catalog_entry(entry: Dict[str, Any]) -> None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO catalog (name, type, genre, certificate, imdb_rate, votes, episodes)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            entry.get("name"),
            entry.get("type"),
            entry.get("genre"),
            entry.get("certificate"),
            entry.get("imdb_rate"),
            entry.get("votes"),
            entry.get("episodes"),
        ),
    )
    conn.commit()
    conn.close()


def insert_view(user_id: int, view: Dict[str, Any]) -> None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO views (user_id, name, type, genre, certificate, imdb_rate, user_rate, view_date, duration_minutes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            user_id,
            view.get("name"),
            view.get("type"),
            view.get("genre"),
            view.get("certificate"),
            view.get("imdb_rate"),
            view.get("user_rate"),
            view.get("view_date"),
            view.get("duration_minutes"),
        ),
    )
    conn.commit()
    conn.close()


def get_last_views(user_id: int, limit: int = 5) -> List[Dict[str, Any]]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT name, user_rate, type, genre, view_date, duration_minutes
        FROM views
        WHERE user_id = ?
        ORDER BY id DESC
        LIMIT ?
        """,
        (user_id, limit),
    )
    rows = cur.fetchall()
    conn.close()
    return [
        {
            "name": r[0],
            "user_rate": r[1],
            "type": r[2],
            "genre": r[3],
            "view_date": r[4],
            "duration_minutes": r[5],
        }
        for r in rows
    ]


def stats(user_id: int) -> Dict[str, Any]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT type, COUNT(*), SUM(duration_minutes), AVG(user_rate)
        FROM views
        WHERE user_id = ?
        GROUP BY type
        """,
        (user_id,),
    )
    per_type = cur.fetchall()

    cur.execute(
        """
        SELECT genre, COUNT(*) as cnt
        FROM views
        WHERE user_id = ?
        GROUP BY genre
        ORDER BY cnt DESC
        LIMIT 5
        """,
        (user_id,),
    )
    top_genres = cur.fetchall()
    conn.close()
    return {"per_type": per_type, "top_genres": top_genres}


def recommendations(user_id: int, limit: int = 5) -> List[Dict[str, Any]]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT DISTINCT name FROM views WHERE user_id = ?
        """,
        (user_id,),
    )
    watched = {row[0].lower() for row in cur.fetchall()}

    cur.execute(
        """
        SELECT genre, COUNT(*) as cnt
        FROM views
        WHERE user_id = ?
        GROUP BY genre
        ORDER BY cnt DESC
        LIMIT 3
        """,
        (user_id,),
    )
    fav_genres = [row[0] for row in cur.fetchall() if row[0]]

    placeholders = ",".join("?" * len(fav_genres)) if fav_genres else None
    if placeholders:
        query = f"""
            SELECT name, type, genre, certificate, imdb_rate
            FROM catalog
            WHERE LOWER(name) NOT IN ({",".join(["?"] * len(watched) or ["''"])})
              AND genre IN ({placeholders})
            ORDER BY imdb_rate DESC NULLS LAST
            LIMIT ?
        """
        params: List[Any] = list(watched) if watched else []
        params.extend(fav_genres)
        params.append(limit)
    else:
        query = """
            SELECT name, type, genre, certificate, imdb_rate
            FROM catalog
            WHERE LOWER(name) NOT IN (SELECT LOWER(name) FROM views WHERE user_id = ?)
            ORDER BY imdb_rate DESC NULLS LAST
            LIMIT ?
        """
        params = [user_id, limit]

    cur.execute(query, params)
    rows = cur.fetchall()
    conn.close()
    return [
        {
            "name": r[0],
            "type": r[1],
            "genre": r[2],
            "certificate": r[3],
            "imdb_rate": r[4],
        }
        for r in rows
    ]


def progress(user_id: int) -> Dict[str, Any]:
    today = datetime.utcnow().date()
    period_len = 30
    curr_start = today - timedelta(days=period_len)
    prev_start = curr_start - timedelta(days=period_len)

    def agg(start: datetime.date, end: datetime.date) -> Dict[str, Any]:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COUNT(*), AVG(user_rate), SUM(duration_minutes)
            FROM views
            WHERE user_id = ? AND date(view_date) BETWEEN ? AND ?
            """,
            (user_id, start.isoformat(), end.isoformat()),
        )
        row = cur.fetchone()
        conn.close()
        return {
            "count": row[0] or 0,
            "avg": round(row[1], 2) if row[1] else None,
            "minutes": row[2] or 0,
        }

    return {
        "current": agg(curr_start, today),
        "previous": agg(prev_start, curr_start - timedelta(days=1)),
    }

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привет! Я веду дневник просмотров и делаю рекомендации.\n"
        "Команды: /add /last /stats /recommend /progress /help",
        reply_markup=main_markup,
    )


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Доступные команды:\n"
        "/add — добавить просмотр\n"
        "/last — последние просмотры\n"
        "/stats — статистика по типам и жанрам\n"
        "/recommend — рекомендации по твоим любимым жанрам\n"
        "/progress — сравнение активности за 30 дней\n"
        "/help — помощь",
        reply_markup=main_markup,
    )


async def add_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Введи название фильма или сериала:", reply_markup=ReplyKeyboardRemove())
    return ADD_TITLE


async def add_title(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()

    # Если уже показывали варианты, обрабатываем выбор
    if "suggestions" in context.user_data:
        suggestions = context.user_data["suggestions"]
        lower = text.lower()
        if lower == "новый":
            await update.message.reply_text(
                "Напиши жанр, тип (Film/Series) и возрастной рейтинг через запятую, "
                "например: Drama, Film, PG-13"
            )
            return ADD_NEW_DETAILS
        if text.isdigit():
            idx = int(text) - 1
            if 0 <= idx < len(suggestions):
                context.user_data["content"] = suggestions[idx]
                await update.message.reply_text(
                    f"Выбран: {suggestions[idx]['name']} ({suggestions[idx].get('type','')}). "
                    "Оцени от 1 до 10:"
                )
                context.user_data.pop("suggestions", None)
                return ADD_EXISTS_RATING
        await update.message.reply_text("Не понял выбор. Введи номер варианта или 'новый'.")
        return ADD_TITLE

    title = text
    context.user_data["title"] = title

    exact = find_in_catalog(title)
    if exact:
        context.user_data["content"] = exact
        await update.message.reply_text(
            f"Нашёл в каталоге: {exact['name']} ({exact.get('type', 'N/A')}) "
            f"[IMDB {exact.get('imdb_rate', '—')}].\n"
            "Оцени от 1 до 10:",
        )
        return ADD_EXISTS_RATING

    suggestions = fuzzy_catalog(title)
    if suggestions:
        text_resp = "Не нашёл точного совпадения. Похожие варианты:\n"
        for idx, item in enumerate(suggestions, 1):
            text_resp += f"{idx}. {item['name']} ({item.get('type','')}) IMDB {item.get('imdb_rate','—')}\n"
        text_resp += "Если ни один не подходит, напиши 'новый'."
        context.user_data["suggestions"] = suggestions
        await update.message.reply_text(text_resp)
        return ADD_TITLE

    await update.message.reply_text(
        "В каталоге нет такого названия. Напиши жанр, тип (Film/Series) и возрастной рейтинг через запятую, "
        "например: Drama, Film, PG-13"
    )
    return ADD_NEW_DETAILS


async def add_new_details(update: Update, context: ContextTypes.DEFAULT_TYPE):
    parts = [p.strip() for p in update.message.text.split(",")]
    if len(parts) < 2:
        await update.message.reply_text("Укажи минимум жанр и тип, через запятую.")
        return ADD_NEW_DETAILS

    genre = parts[0]
    content_type = parts[1] if len(parts) > 1 else "Film"
    certificate = parts[2] if len(parts) > 2 else ""

    new_item = {
        "name": context.user_data["title"],
        "type": content_type,
        "genre": genre,
        "certificate": certificate,
        "imdb_rate": None,
    }
    context.user_data["content"] = new_item
    insert_catalog_entry(new_item)

    await update.message.reply_text("Принято. Теперь оцени от 1 до 10:")
    return ADD_NEW_RATING


async def add_rating(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        rating = float(update.message.text)
        if not 1 <= rating <= 10:
            raise ValueError
    except ValueError:
        await update.message.reply_text("Оценка должна быть числом от 1 до 10.")
        return ADD_EXISTS_RATING

    context.user_data["user_rate"] = rating
    await update.message.reply_text("Когда посмотрел? Введи дату в формате YYYY-MM-DD или 'сегодня'.")
    return ADD_DATE


async def add_new_rating(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        rating = float(update.message.text)
        if not 1 <= rating <= 10:
            raise ValueError
    except ValueError:
        await update.message.reply_text("Оценка должна быть числом от 1 до 10.")
        return ADD_NEW_RATING

    context.user_data["user_rate"] = rating
    await update.message.reply_text("Когда посмотрел? Введи дату в формате YYYY-MM-DD или 'сегодня'.")
    return ADD_DATE


async def add_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip().lower()
    if text == "сегодня":
        view_date = datetime.utcnow().date()
    else:
        try:
            view_date = datetime.strptime(text, "%Y-%m-%d").date()
        except ValueError:
            await update.message.reply_text("Формат даты: YYYY-MM-DD или 'сегодня'.")
            return ADD_DATE

    context.user_data["view_date"] = view_date.isoformat()
    await update.message.reply_text(
        "Сколько минут заняло? Для фильма можно ввести 120, для сериала — суммарно. Или напиши 'авто'."
    )
    return ADD_DURATION


async def add_duration(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip().lower()
    if text == "авто":
        duration = 120 if context.user_data["content"].get("type", "").lower() != "series" else 45
    else:
        try:
            duration = int(text)
        except ValueError:
            await update.message.reply_text("Нужно число минут или 'авто'.")
            return ADD_DURATION

    content = context.user_data["content"]
    view = {
        "name": content["name"],
        "type": content.get("type"),
        "genre": content.get("genre"),
        "certificate": content.get("certificate"),
        "imdb_rate": content.get("imdb_rate"),
        "user_rate": context.user_data["user_rate"],
        "view_date": context.user_data["view_date"],
        "duration_minutes": duration,
    }
    insert_view(update.effective_user.id, view)
    await update.message.reply_text("Добавил в дневник! Что дальше?", reply_markup=main_markup)
    return ConversationHandler.END


async def last_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    items = get_last_views(update.effective_user.id, limit=5)
    if not items:
        await update.message.reply_text("Пока нет просмотров. Используй /add.")
        return
    lines = []
    for item in items:
        lines.append(
            f"{item['name']} — {item['user_rate']}/10, {item.get('type','')} "
            f"({item.get('genre','')}) {item.get('view_date','')}"
        )
    await update.message.reply_text("\n".join(lines), reply_markup=main_markup)


async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = stats(update.effective_user.id)
    if not data["per_type"]:
        await update.message.reply_text("Нет данных. Добавь просмотры через /add.")
        return
    lines = ["По типам:"]
    for row in data["per_type"]:
        lines.append(
            f"{row[0]} — {row[1]} шт, {row[2] or 0} минут, средн. оценка {round(row[3],2) if row[3] else '—'}"
        )
    if data["top_genres"]:
        lines.append("\nТоп жанров:")
        for g in data["top_genres"]:
            lines.append(f"{g[0]} — {g[1]}")
    await update.message.reply_text("\n".join(lines), reply_markup=main_markup)


async def recommend_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    recs = recommendations(update.effective_user.id, limit=5)
    if not recs:
        await update.message.reply_text("Нужно больше данных о предпочтениях. Добавь просмотры через /add.")
        return
    lines = ["Рекомендую посмотреть:"]
    for r in recs:
        lines.append(
            f"{r['name']} ({r.get('type','')}) — {r.get('genre','')} "
            f"IMDB {r.get('imdb_rate','—')}, рейтинг {r.get('certificate','')}"
        )
    await update.message.reply_text("\n".join(lines), reply_markup=main_markup)


async def progress_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    p = progress(update.effective_user.id)
    curr, prev = p["current"], p["previous"]
    await update.message.reply_text(
        "Сравнение последних 30 дней с предыдущими 30:\n"
        f"Текущий период: {curr['count']} просмотров, ср. оценка {curr['avg']}, минут {curr['minutes']}\n"
        f"Предыдущий: {prev['count']} просмотров, ср. оценка {prev['avg']}, минут {prev['minutes']}",
        reply_markup=main_markup,
    )


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Отменено.", reply_markup=main_markup)
    return ConversationHandler.END


def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN не найден в переменных окружения.")

    init_db()
    load_catalog_if_empty()

    application = Application.builder().token(BOT_TOKEN).build()

    conv = ConversationHandler(
        entry_points=[CommandHandler("add", add_start)],
        states={
            ADD_TITLE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_title)],
            ADD_NEW_DETAILS: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_new_details)],
            ADD_EXISTS_RATING: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_rating)],
            ADD_NEW_RATING: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_new_rating)],
            ADD_DATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_date)],
            ADD_DURATION: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_duration)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True,
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_cmd))
    application.add_handler(CommandHandler("last", last_cmd))
    application.add_handler(CommandHandler("stats", stats_cmd))
    application.add_handler(CommandHandler("recommend", recommend_cmd))
    application.add_handler(CommandHandler("progress", progress_cmd))
    application.add_handler(conv)

    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, lambda u, c: u.message.reply_text("Используй меню команд."))
    )

    application.run_polling()


if __name__ == "__main__":

    main()
