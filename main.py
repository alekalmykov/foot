import logging
import os
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import BadRequest, Forbidden
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.getenv("DB_PATH", os.path.join(BASE_DIR, "bot.db"))
POLL_TITLE = "Кто идёт на футбол?"


@dataclass
class Settings:
    bot_token: str
    default_chat_id: int | None
    cron_schedule: str
    timezone: str


def load_settings() -> Settings:
    load_dotenv()

    bot_token = os.getenv("BOT_TOKEN", "").strip()
    if not bot_token:
        raise RuntimeError("BOT_TOKEN is required")

    default_chat_id_raw = os.getenv("DEFAULT_CHAT_ID", "").strip()
    default_chat_id = int(default_chat_id_raw) if default_chat_id_raw else None

    cron_schedule = os.getenv("CRON_SCHEDULE", "0 18 * * 0").strip()
    timezone = os.getenv("TIMEZONE", "Europe/Moscow").strip()

    return Settings(
        bot_token=bot_token,
        default_chat_id=default_chat_id,
        cron_schedule=cron_schedule,
        timezone=timezone,
    )


def get_db_connection() -> sqlite3.Connection:
    db_dir = os.path.dirname(DB_PATH)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db() -> None:
    with closing(get_db_connection()) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS polls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                is_closed INTEGER NOT NULL DEFAULT 0,
                legionnaires_count INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS poll_participants (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                poll_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                username TEXT,
                full_name TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (poll_id, user_id),
                FOREIGN KEY (poll_id) REFERENCES polls(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS poll_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                poll_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                UNIQUE (chat_id, message_id),
                FOREIGN KEY (poll_id) REFERENCES polls(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS chat_state (
                chat_id INTEGER PRIMARY KEY,
                current_poll_id INTEGER,
                FOREIGN KEY (current_poll_id) REFERENCES polls(id) ON DELETE SET NULL
            );
            """
        )

        poll_columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(polls)").fetchall()
        }
        if "legionnaires_count" not in poll_columns:
            conn.execute(
                "ALTER TABLE polls ADD COLUMN legionnaires_count INTEGER NOT NULL DEFAULT 0"
            )

        conn.commit()


def create_poll(chat_id: int | None = None) -> int:
    with closing(get_db_connection()) as conn:
        cursor = conn.execute(
            "INSERT INTO polls (created_at, is_closed) VALUES (?, 0)",
            (datetime.utcnow().isoformat(timespec="seconds"),),
        )
        poll_id = cursor.lastrowid

        if chat_id is not None:
            conn.execute(
                """
                INSERT INTO chat_state (chat_id, current_poll_id)
                VALUES (?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET current_poll_id = excluded.current_poll_id
                """,
                (chat_id, poll_id),
            )

        conn.commit()
        logging.info("Created poll %s for chat %s", poll_id, chat_id)
        return int(poll_id)


def add_participant(poll_id: int, user_id: int, username: str | None, full_name: str) -> None:
    with closing(get_db_connection()) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO poll_participants (poll_id, user_id, username, full_name, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (poll_id, user_id, username, full_name, datetime.utcnow().isoformat(timespec="seconds")),
        )
        conn.commit()


def remove_participant(poll_id: int, user_id: int) -> None:
    with closing(get_db_connection()) as conn:
        conn.execute(
            "DELETE FROM poll_participants WHERE poll_id = ? AND user_id = ?",
            (poll_id, user_id),
        )
        conn.commit()


def set_not_going(poll_id: int, user_id: int) -> bool:
    with closing(get_db_connection()) as conn:
        poll = conn.execute(
            "SELECT is_closed FROM polls WHERE id = ?",
            (poll_id,),
        ).fetchone()
        if poll is None:
            raise ValueError("Poll not found")
        if poll["is_closed"]:
            raise RuntimeError("Poll is closed")

        existing = conn.execute(
            "SELECT id FROM poll_participants WHERE poll_id = ? AND user_id = ?",
            (poll_id, user_id),
        ).fetchone()

        if existing:
            conn.execute(
                "DELETE FROM poll_participants WHERE poll_id = ? AND user_id = ?",
                (poll_id, user_id),
            )
            conn.commit()
            logging.info("Set not going for user_id=%s in poll_id=%s", user_id, poll_id)
            return True

        return False


def toggle_participant(poll_id: int, user_id: int, username: str | None, full_name: str) -> bool:
    with closing(get_db_connection()) as conn:
        poll = conn.execute(
            "SELECT is_closed FROM polls WHERE id = ?",
            (poll_id,),
        ).fetchone()
        if poll is None:
            raise ValueError("Poll not found")
        if poll["is_closed"]:
            raise RuntimeError("Poll is closed")

        existing = conn.execute(
            "SELECT id FROM poll_participants WHERE poll_id = ? AND user_id = ?",
            (poll_id, user_id),
        ).fetchone()

        if existing:
            conn.execute(
                "DELETE FROM poll_participants WHERE poll_id = ? AND user_id = ?",
                (poll_id, user_id),
            )
            conn.commit()
            logging.info("Removed participant user_id=%s from poll_id=%s", user_id, poll_id)
            return False

        conn.execute(
            """
            INSERT INTO poll_participants (poll_id, user_id, username, full_name, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (poll_id, user_id, username, full_name, datetime.utcnow().isoformat(timespec="seconds")),
        )
        conn.commit()
        logging.info("Added participant user_id=%s to poll_id=%s", user_id, poll_id)
        return True


def change_legionnaires(poll_id: int, delta: int) -> int:
    with closing(get_db_connection()) as conn:
        poll = conn.execute(
            "SELECT is_closed, legionnaires_count FROM polls WHERE id = ?",
            (poll_id,),
        ).fetchone()
        if poll is None:
            raise ValueError("Poll not found")
        if poll["is_closed"]:
            raise RuntimeError("Poll is closed")

        new_value = max(0, int(poll["legionnaires_count"]) + delta)
        conn.execute(
            "UPDATE polls SET legionnaires_count = ? WHERE id = ?",
            (new_value, poll_id),
        )
        conn.commit()
        logging.info(
            "Changed legionnaires for poll_id=%s by %s, new value=%s",
            poll_id,
            delta,
            new_value,
        )
        return new_value


def build_poll_text(poll_id: int) -> str:
    with closing(get_db_connection()) as conn:
        poll = conn.execute(
            "SELECT id, is_closed, legionnaires_count FROM polls WHERE id = ?",
            (poll_id,),
        ).fetchone()
        if poll is None:
            return "Опрос не найден."

        participants = conn.execute(
            """
            SELECT username, full_name
            FROM poll_participants
            WHERE poll_id = ?
            ORDER BY created_at ASC, id ASC
            """,
            (poll_id,),
        ).fetchall()

    lines = [POLL_TITLE]

    if poll["is_closed"]:
        lines.append("")
        lines.append("Статус: закрыт")

    lines.append("")
    lines.append("Участники:")

    if not participants:
        lines.append("— пока никого")
    else:
        for index, participant in enumerate(participants, start=1):
            lines.append(f"{index}. {participant['full_name']}")

    players_count = len(participants)
    legionnaires_count = int(poll["legionnaires_count"])
    total_count = players_count + legionnaires_count

    lines.append("")
    lines.append("Итог:")
    lines.append(f"Игроки: {players_count}")
    lines.append(f"Легионеры: {legionnaires_count}")
    lines.append(f"Всего: {total_count}")

    return "\n".join(lines)


def get_poll_keyboard(poll_id: int, is_closed: bool = False) -> InlineKeyboardMarkup | None:
    if is_closed:
        return None

    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ Иду", callback_data=f"poll_action:{poll_id}:go"),
                InlineKeyboardButton("❌ Не иду", callback_data=f"poll_action:{poll_id}:no"),
            ],
            [
                InlineKeyboardButton("Легионер +", callback_data=f"poll_action:{poll_id}:legion_plus"),
                InlineKeyboardButton("Легионер -", callback_data=f"poll_action:{poll_id}:legion_minus"),
            ],
        ]
    )


def get_current_poll_id(chat_id: int) -> int | None:
    with closing(get_db_connection()) as conn:
        row = conn.execute(
            "SELECT current_poll_id FROM chat_state WHERE chat_id = ?",
            (chat_id,),
        ).fetchone()
        if row is None:
            return None
        return row["current_poll_id"]


def get_poll(poll_id: int) -> sqlite3.Row | None:
    with closing(get_db_connection()) as conn:
        return conn.execute("SELECT * FROM polls WHERE id = ?", (poll_id,)).fetchone()


def save_poll_message(poll_id: int, chat_id: int, message_id: int) -> None:
    with closing(get_db_connection()) as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO poll_messages (poll_id, chat_id, message_id)
            VALUES (?, ?, ?)
            """,
            (poll_id, chat_id, message_id),
        )
        conn.commit()


def delete_poll_message(chat_id: int, message_id: int) -> None:
    with closing(get_db_connection()) as conn:
        conn.execute(
            "DELETE FROM poll_messages WHERE chat_id = ? AND message_id = ?",
            (chat_id, message_id),
        )
        conn.commit()


def close_current_poll(chat_id: int) -> int | None:
    with closing(get_db_connection()) as conn:
        row = conn.execute(
            "SELECT current_poll_id FROM chat_state WHERE chat_id = ?",
            (chat_id,),
        ).fetchone()

        if row is None or row["current_poll_id"] is None:
            return None

        poll_id = row["current_poll_id"]
        conn.execute("UPDATE polls SET is_closed = 1 WHERE id = ?", (poll_id,))
        conn.execute("UPDATE chat_state SET current_poll_id = NULL WHERE chat_id = ?", (chat_id,))
        conn.commit()
        logging.info("Closed current poll %s for chat %s", poll_id, chat_id)
        return poll_id


async def sync_poll_messages(application: Application, poll_id: int) -> None:
    poll = get_poll(poll_id)
    if poll is None:
        return

    text = build_poll_text(poll_id)
    reply_markup = get_poll_keyboard(poll_id, is_closed=bool(poll["is_closed"]))

    with closing(get_db_connection()) as conn:
        messages = conn.execute(
            "SELECT chat_id, message_id FROM poll_messages WHERE poll_id = ?",
            (poll_id,),
        ).fetchall()

    for message in messages:
        chat_id = message["chat_id"]
        message_id = message["message_id"]

        try:
            await application.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                reply_markup=reply_markup,
            )
        except BadRequest as exc:
            error_text = str(exc)
            if "Message is not modified" in error_text:
                continue
            if "message to edit not found" in error_text or "message can't be edited" in error_text:
                logging.warning(
                    "Removing stale poll message chat_id=%s message_id=%s: %s",
                    chat_id,
                    message_id,
                    error_text,
                )
                delete_poll_message(chat_id, message_id)
                continue
            logging.exception(
                "Failed to edit poll message chat_id=%s message_id=%s",
                chat_id,
                message_id,
            )
        except Forbidden:
            logging.warning(
                "Bot no longer has access to chat_id=%s, deleting message binding",
                chat_id,
            )
            delete_poll_message(chat_id, message_id)
        except Exception:
            logging.exception(
                "Unexpected error while syncing poll message chat_id=%s message_id=%s",
                chat_id,
                message_id,
            )


async def send_poll_message(application: Application, chat_id: int, poll_id: int) -> int:
    poll = get_poll(poll_id)
    if poll is None:
        raise ValueError(f"Poll {poll_id} not found")

    message = await application.bot.send_message(
        chat_id=chat_id,
        text=build_poll_text(poll_id),
        reply_markup=get_poll_keyboard(poll_id, is_closed=bool(poll["is_closed"])),
    )
    save_poll_message(poll_id, chat_id, message.message_id)
    return message.message_id


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (
        "Привет! Я бот для записи на футбол.\n\n"
        "Команды:\n"
        "/newpoll - создать новый опрос\n"
        "/current - показать текущий опрос в этом чате\n"
        "/closepoll - закрыть текущий опрос в этом чате"
    )
    await update.effective_message.reply_text(text)


async def new_poll_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    if chat is None:
        return

    poll_id = create_poll(chat.id)
    await send_poll_message(context.application, chat.id, poll_id)


async def current_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    if chat is None:
        return

    poll_id = get_current_poll_id(chat.id)
    if poll_id is None:
        await update.effective_message.reply_text("В этом чате сейчас нет активного опроса.")
        return

    await send_poll_message(context.application, chat.id, poll_id)


async def close_poll_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    if chat is None:
        return

    poll_id = close_current_poll(chat.id)
    if poll_id is None:
        await update.effective_message.reply_text("В этом чате нет активного опроса для закрытия.")
        return

    await sync_poll_messages(context.application, poll_id)
    await update.effective_message.reply_text(f"Опрос #{poll_id} закрыт для этого чата.")


async def poll_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query is None or query.from_user is None:
        return

    data = query.data or ""
    if not data.startswith("poll_action:"):
        await query.answer()
        return

    _, poll_id_raw, action = data.split(":", 2)
    poll_id = int(poll_id_raw)
    user = query.from_user

    try:
        if action == "go":
            is_added = toggle_participant(
                poll_id=poll_id,
                user_id=user.id,
                username=user.username,
                full_name=user.full_name,
            )
            answer_text = "Вы записались." if is_added else "Вы убраны из списка."
        elif action == "no":
            was_removed = set_not_going(
                poll_id=poll_id,
                user_id=user.id,
            )
            answer_text = "Вы отметились как не идущий." if was_removed else "Вы уже не в списке."
        elif action == "legion_plus":
            new_value = change_legionnaires(poll_id=poll_id, delta=1)
            answer_text = f"Легионеров: {new_value}"
        elif action == "legion_minus":
            new_value = change_legionnaires(poll_id=poll_id, delta=-1)
            answer_text = f"Легионеров: {new_value}"
        else:
            await query.answer()
            return
    except ValueError:
        await query.answer("Опрос не найден.", show_alert=True)
        return
    except RuntimeError:
        await query.answer("Этот опрос уже закрыт.", show_alert=True)
        return

    await sync_poll_messages(context.application, poll_id)
    await query.answer(answer_text)


async def scheduled_new_poll(application: Application) -> None:
    settings: Settings = application.bot_data["settings"]
    default_chat_id = settings.default_chat_id

    if default_chat_id is None:
        logging.warning("DEFAULT_CHAT_ID is not set, scheduled poll skipped")
        return

    poll_id = create_poll(default_chat_id)
    await send_poll_message(application, default_chat_id, poll_id)
    logging.info("Scheduled poll %s sent to chat %s", poll_id, default_chat_id)


async def scheduled_new_poll_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    await scheduled_new_poll(context.application)


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)


async def post_init(application: Application) -> None:
    scheduler: AsyncIOScheduler = application.bot_data["scheduler"]
    if not scheduler.running:
        scheduler.start()
        logging.info("Scheduler started")


async def post_shutdown(application: Application) -> None:
    scheduler: AsyncIOScheduler | None = application.bot_data.get("scheduler")
    if scheduler and scheduler.running:
        scheduler.shutdown(wait=False)
        logging.info("Scheduler stopped")


def build_application(settings: Settings) -> Application:
    timezone = ZoneInfo(settings.timezone)
    scheduler = AsyncIOScheduler(timezone=timezone)
    trigger = CronTrigger.from_crontab(settings.cron_schedule, timezone=timezone)

    application = (
        Application.builder()
        .token(settings.bot_token)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )
    application.bot_data["settings"] = settings
    application.bot_data["scheduler"] = scheduler

    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("newpoll", new_poll_command))
    application.add_handler(CommandHandler("current", current_command))
    application.add_handler(CommandHandler("closepoll", close_poll_command))
    application.add_handler(
        CallbackQueryHandler(poll_callback, pattern=r"^poll_action:\d+:[a-z_]+$")
    )

    scheduler.add_job(
        scheduled_new_poll,
        trigger=trigger,
        kwargs={"application": application},
        id="weekly_football_poll",
        replace_existing=True,
    )
    return application


def main() -> None:
    configure_logging()
    settings = load_settings()
    init_db()
    application = build_application(settings)

    logging.info(
        "Starting bot with timezone=%s cron=%s default_chat_id=%s",
        settings.timezone,
        settings.cron_schedule,
        settings.default_chat_id,
    )

    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
