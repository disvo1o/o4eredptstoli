import asyncio
import logging
import os
from datetime import datetime
from pathlib import Path

import aiosqlite
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
)
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_IDS = {
    int(value.strip())
    for value in os.getenv("ADMIN_IDS", "").split(",")
    if value.strip().isdigit()
}
DB_PATH = os.getenv("DATABASE_PATH", "/data/queue.db")
if not DB_PATH.startswith("/"):
    DB_PATH = str(Path(__file__).resolve().parent.parent / DB_PATH)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("tennis-queue")
dp = Dispatcher()
db_lock = asyncio.Lock()
bot: Bot


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def display_name(user) -> str:
    name = " ".join(part for part in [user.first_name, user.last_name] if part).strip()
    return name or (f"@{user.username}" if user.username else f"Игрок {user.id}")


async def init_db():
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS queue (
                user_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                joined_at TEXT NOT NULL
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS matches (
                table_no INTEGER PRIMARY KEY,
                player1_id INTEGER NOT NULL,
                player1_name TEXT NOT NULL,
                player2_id INTEGER NOT NULL,
                player2_name TEXT NOT NULL,
                started_at TEXT NOT NULL
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS cooldowns (
                user_id INTEGER PRIMARY KEY,
                cooldown_until_game INTEGER NOT NULL
            )
        """)
        await db.execute(
            "INSERT OR IGNORE INTO settings(key, value) VALUES('paused', '0')"
        )
        await db.execute(
            "INSERT OR IGNORE INTO settings(key, value) VALUES('completed_games', '0')"
        )
        await db.commit()


async def get_paused(db=None) -> bool:
    if db is not None:
        cur = await db.execute("SELECT value FROM settings WHERE key='paused'")
        row = await cur.fetchone()
        return bool(row and row[0] == "1")
    async with aiosqlite.connect(DB_PATH) as conn:
        cur = await conn.execute("SELECT value FROM settings WHERE key='paused'")
        row = await cur.fetchone()
        return bool(row and row[0] == "1")


async def assign_players():
    """Fill every free table with the next two people in FIFO order."""
    started = []
    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT value FROM settings WHERE key='paused'")
            paused_row = await cur.fetchone()
            if paused_row and paused_row["value"] == "1":
                return

            for table_no in (1, 2):
                cur = await db.execute(
                    "SELECT 1 FROM matches WHERE table_no=?", (table_no,)
                )
                if await cur.fetchone():
                    continue

                cur = await db.execute("""
                    SELECT q.user_id, q.name
                    FROM queue q
                    LEFT JOIN cooldowns c ON c.user_id = q.user_id
                    LEFT JOIN settings s ON s.key = 'completed_games'
                    WHERE c.user_id IS NULL
                       OR CAST(s.value AS INTEGER) >= c.cooldown_until_game
                    ORDER BY q.joined_at, q.user_id
                    LIMIT 2
                """)
                players = await cur.fetchall()
                if len(players) < 2:
                    break

                p1, p2 = players
                now = datetime.utcnow().isoformat(timespec="seconds")
                await db.execute(
                    "DELETE FROM queue WHERE user_id IN (?, ?)",
                    (p1["user_id"], p2["user_id"]),
                )
                await db.execute("""
                    INSERT INTO matches(
                        table_no, player1_id, player1_name,
                        player2_id, player2_name, started_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    table_no, p1["user_id"], p1["name"],
                    p2["user_id"], p2["name"], now
                ))
                started.append((table_no, p1["user_id"], p1["name"],
                                p2["user_id"], p2["name"]))
            await db.commit()

    for table_no, id1, name1, id2, name2 in started:
        message = (
            f"🏓 Ваша игра начинается!\n\n"
            f"Стол №{table_no}\n"
            f"{name1} vs {name2}\n\n"
            f"Когда закончите, нажмите «Завершить игру» в боте."
        )
        for uid in (id1, id2):
            try:
                await bot.send_message(uid, message)
            except (TelegramForbiddenError, TelegramBadRequest):
                log.info("Не удалось отправить уведомление игроку %s", uid)


async def get_snapshot():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM matches ORDER BY table_no")
        matches = await cur.fetchall()
        cur = await db.execute(
            "SELECT user_id, name FROM queue ORDER BY joined_at, user_id"
        )
        waiting = await cur.fetchall()
        cur = await db.execute("SELECT value FROM settings WHERE key='paused'")
        paused_row = await cur.fetchone()
        paused = bool(paused_row and paused_row["value"] == "1")
    return matches, waiting, paused


def status_text(matches, waiting, paused, viewer_id=None):
    lines = ["🏓 <b>ОЧЕРЕДЬ НА НАСТОЛЬНЫЙ ТЕННИС</b>", ""]
    lines.append("⏸ Очередь приостановлена" if paused else "🟢 Очередь работает")
    lines.append("")

    match_by_table = {row["table_no"]: row for row in matches}
    for table_no in (1, 2):
        row = match_by_table.get(table_no)
        if row:
            lines.append(f"🏓 <b>Стол №{table_no} — играют</b>")
            lines.append(f"   {row['player1_name']} · {row['player2_name']}")
            if viewer_id in (row["player1_id"], row["player2_id"]):
                lines.append("   <i>Вы играете за этим столом</i>")
        else:
            lines.append(f"⚪ <b>Стол №{table_no} — свободен</b>")
        lines.append("")

    lines.append(f"👥 <b>Ожидают: {len(waiting)}</b>")
    if waiting:
        for index, player in enumerate(waiting[:30], start=1):
            suffix = " ← следующий в паре" if index <= 2 else ""
            lines.append(f"{index}. {player['name']}{suffix}")
        if len(waiting) > 30:
            lines.append(f"…и ещё {len(waiting) - 30}")
    else:
        lines.append("Пока никого нет.")

    if viewer_id is not None:
        position = next(
            (i for i, player in enumerate(waiting, start=1)
             if player["user_id"] == viewer_id),
            None
        )
        if position:
            lines.extend(["", f"📍 <b>Ваша позиция:</b> {position}"])
        elif any(viewer_id in (m["player1_id"], m["player2_id"]) for m in matches):
            lines.extend(["", "🎾 <b>Вы сейчас играете.</b>"])
        else:
            lines.extend(["", "Вы пока не записаны."])

    return "\n".join(lines)


def main_keyboard(user_id: int, matches):
    rows = [
        [InlineKeyboardButton(text="➕ Записаться", callback_data="join"),
         InlineKeyboardButton(text="➖ Выйти", callback_data="leave")],
        [InlineKeyboardButton(text="🔄 Обновить очередь", callback_data="refresh")],
    ]
    for match in matches:
        if user_id in (match["player1_id"], match["player2_id"]) or is_admin(user_id):
            rows.append([InlineKeyboardButton(
                text=f"🏁 Завершить игру · стол {match['table_no']}",
                callback_data=f"finish:{match['table_no']}"
            )])
    if is_admin(user_id):
        rows.append([InlineKeyboardButton(text="⚙️ Панель администратора", callback_data="admin")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def render_for_message(message: Message, user_id: int, edit=False):
    matches, waiting, paused = await get_snapshot()
    text = status_text(matches, waiting, paused, user_id)
    markup = main_keyboard(user_id, matches)
    if edit:
        try:
            await message.edit_text(text, reply_markup=markup, parse_mode="HTML")
            return
        except TelegramBadRequest as exc:
            if "message is not modified" in str(exc).lower():
                return
    await message.answer(text, reply_markup=markup, parse_mode="HTML")


async def render_callback(call: CallbackQuery):
    matches, waiting, paused = await get_snapshot()
    text = status_text(matches, waiting, paused, call.from_user.id)
    markup = main_keyboard(call.from_user.id, matches)
    try:
        await call.message.edit_text(text, reply_markup=markup, parse_mode="HTML")
    except TelegramBadRequest as exc:
        if "message is not modified" not in str(exc).lower():
            await call.message.answer(text, reply_markup=markup, parse_mode="HTML")


@dp.message(CommandStart())
async def start(message: Message):
    await render_for_message(message, message.from_user.id)
    await message.answer(
        "Здесь можно записаться в общую очередь. "
        "Как только на одном из двух столов освободится место, "
        "следующие два игрока автоматически начнут игру."
    )


@dp.message(Command("myid"))
async def myid(message: Message):
    await message.answer(f"Ваш Telegram ID: <code>{message.from_user.id}</code>", parse_mode="HTML")


@dp.message(Command("queue"))
async def queue_command(message: Message):
    await render_for_message(message, message.from_user.id)


@dp.callback_query(F.data == "join")
async def join(call: CallbackQuery):
    uid = call.from_user.id
    name = display_name(call.from_user)
    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute("SELECT value FROM settings WHERE key='paused'")
            row = await cur.fetchone()
            paused = bool(row and row[0] == "1")
            if paused:
                await call.answer("Запись временно приостановлена.", show_alert=True)
                return
            cur = await db.execute("SELECT value FROM settings WHERE key='completed_games'")
            games_row = await cur.fetchone()
            completed_games = int(games_row[0]) if games_row else 0
            cur = await db.execute(
                "SELECT cooldown_until_game FROM cooldowns WHERE user_id=?", (uid,)
            )
            cooldown_row = await cur.fetchone()
            if cooldown_row:
                remaining = int(cooldown_row[0]) - completed_games
                if remaining > 0:
                    await call.answer(
                        f"После вашей игры нужно завершить ещё {remaining} матча.",
                        show_alert=True
                    )
                    return
                await db.execute("DELETE FROM cooldowns WHERE user_id=?", (uid,))
            cur = await db.execute(
                "SELECT 1 FROM queue WHERE user_id=?", (uid,)
            )
            already_waiting = await cur.fetchone()
            cur = await db.execute(
                "SELECT 1 FROM matches WHERE player1_id=? OR player2_id=?",
                (uid, uid)
            )
            already_playing = await cur.fetchone()
            if already_waiting or already_playing:
                await call.answer("Вы уже в очереди или играете.", show_alert=True)
                return
            await db.execute(
                "INSERT INTO queue(user_id, name, joined_at) VALUES (?, ?, ?)",
                (uid, name, datetime.utcnow().isoformat(timespec="microseconds"))
            )
            await db.commit()
    await call.answer("Вы добавлены в очередь!")
    await assign_players()
    await render_callback(call)


@dp.callback_query(F.data == "leave")
async def leave(call: CallbackQuery):
    uid = call.from_user.id
    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute("DELETE FROM queue WHERE user_id=?", (uid,))
            removed = cur.rowcount > 0
            await db.commit()
    await call.answer("Вы вышли из очереди." if removed else "Вас нет в ожидающей очереди.")
    await render_callback(call)


@dp.callback_query(F.data == "refresh")
async def refresh(call: CallbackQuery):
    await call.answer("Очередь обновлена")
    await render_callback(call)


@dp.callback_query(F.data.startswith("finish:"))
async def finish(call: CallbackQuery):
    table_no = int(call.data.split(":", 1)[1])
    uid = call.from_user.id
    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM matches WHERE table_no=?", (table_no,))
            match = await cur.fetchone()
            if not match:
                await call.answer("За этим столом уже никто не играет.", show_alert=True)
                return
            if uid not in (match["player1_id"], match["player2_id"]) and not is_admin(uid):
                await call.answer("Завершить эту игру может только участник или администратор.", show_alert=True)
                return
            await db.execute("DELETE FROM matches WHERE table_no=?", (table_no,))
            cur = await db.execute(
                "SELECT value FROM settings WHERE key='completed_games'"
            )
            games_row = await cur.fetchone()
            completed_games = (int(games_row[0]) if games_row else 0) + 1
            await db.execute(
                "INSERT INTO settings(key, value) VALUES('completed_games', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(completed_games),)
            )
            cooldown_until = completed_games + 3
            for player_id in (match["player1_id"], match["player2_id"]):
                await db.execute(
                    "INSERT INTO cooldowns(user_id, cooldown_until_game) VALUES(?, ?) "
                    "ON CONFLICT(user_id) DO UPDATE SET cooldown_until_game=excluded.cooldown_until_game",
                    (player_id, cooldown_until)
                )
            await db.commit()

    for player_id in (match["player1_id"], match["player2_id"]):
        try:
            await bot.send_message(
                player_id,
                f"Игра за столом №{table_no} завершена. Спасибо!\n\n"
                f"По правилам очереди вы пропускаете следующие 3 завершённых матча. "
                f"После них сможете снова записаться."
            )
        except (TelegramForbiddenError, TelegramBadRequest):
            pass

    await assign_players()
    await call.answer(f"Стол №{table_no} освобождён!")
    await render_callback(call)


def admin_keyboard(paused: bool):
    rows = [
        [InlineKeyboardButton(text="🏁 Завершить игру за столом 1", callback_data="finish:1")],
        [InlineKeyboardButton(text="🏁 Завершить игру за столом 2", callback_data="finish:2")],
        [InlineKeyboardButton(
            text="▶️ Возобновить очередь" if paused else "⏸ Приостановить очередь",
            callback_data="toggle_pause"
        )],
        [InlineKeyboardButton(text="🗑 Очистить ожидающую очередь", callback_data="clear_queue")],
        [InlineKeyboardButton(text="↩️ Назад к очереди", callback_data="refresh")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def show_admin(message: Message, user_id: int):
    if not is_admin(user_id):
        await message.answer("Эта команда доступна только администраторам.")
        return
    matches, waiting, paused = await get_snapshot()
    text = (
        "⚙️ <b>ПАНЕЛЬ АДМИНИСТРАТОРА</b>\n\n"
        f"Статус: {'пауза' if paused else 'работает'}\n"
        f"Игр сейчас: {len(matches)}\n"
        f"В очереди: {len(waiting)}\n\n"
        "Завершение игры освобождает стол и автоматически запускает следующую пару."
    )
    await message.answer(text, reply_markup=admin_keyboard(paused), parse_mode="HTML")


@dp.message(Command("admin"))
async def admin_command(message: Message):
    await show_admin(message, message.from_user.id)


@dp.callback_query(F.data == "admin")
async def admin_callback(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа.", show_alert=True)
        return
    _, _, paused = await get_snapshot()
    await call.answer()
    await call.message.answer(
        "⚙️ <b>ПАНЕЛЬ АДМИНИСТРАТОРА</b>",
        reply_markup=admin_keyboard(paused),
        parse_mode="HTML"
    )


@dp.callback_query(F.data == "toggle_pause")
async def toggle_pause(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа.", show_alert=True)
        return
    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute("SELECT value FROM settings WHERE key='paused'")
            row = await cur.fetchone()
            new_value = "0" if row and row[0] == "1" else "1"
            await db.execute("UPDATE settings SET value=? WHERE key='paused'", (new_value,))
            await db.commit()
    if new_value == "0":
        await assign_players()
    await call.answer("Очередь возобновлена." if new_value == "0" else "Очередь приостановлена.")
    await render_callback(call)
    await show_admin(call.message, call.from_user.id)


@dp.callback_query(F.data == "clear_queue")
async def clear_queue(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа.", show_alert=True)
        return
    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("DELETE FROM queue")
            await db.commit()
    await call.answer("Ожидающая очередь очищена.")
    await render_callback(call)


@dp.message(Command("remove"))
async def remove_user(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("Эта команда доступна только администраторам.")
        return
    parts = (message.text or "").split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.answer("Использование: <code>/remove TELEGRAM_ID</code>", parse_mode="HTML")
        return
    target = int(parts[1])
    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute("DELETE FROM queue WHERE user_id=?", (target,))
            await db.commit()
    await message.answer(
        "Игрок удалён из очереди." if cur.rowcount else "Игрок не найден в ожидающей очереди."
    )


async def main():
    global bot
    if not BOT_TOKEN:
        raise RuntimeError("Не задана переменная окружения BOT_TOKEN")
    await init_db()
    bot = Bot(BOT_TOKEN)
    me = await bot.get_me()
    log.info("Бот запущен: @%s; admins=%s; db=%s", me.username, sorted(ADMIN_IDS), DB_PATH)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
