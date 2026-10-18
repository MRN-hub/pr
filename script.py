"""
Telegram-бот криптообменника: USDT / GRAM -> RUB
Оплата: CryptoBot (Crypto Pay) или xRocket (xRocket Pay)
Курс клиента = рыночный курс - MARGIN (по умолчанию 1%)
Админы (по Telegram ID) получают оплаченные заявки, берут их в работу
и общаются с клиентом напрямую через бота.
"""
import asyncio
import html
import logging
import os
import time
from decimal import ROUND_DOWN, ROUND_UP, Decimal, InvalidOperation

import aiohttp
import aiosqlite
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from dotenv import load_dotenv

load_dotenv("/Users/nikitaf/PycharmProjects/WelcomeScreen/WelcomeScreen.env")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("exchange")

# ───────────────────────── Конфигурация ─────────────────────────
BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").replace(" ", "").split(",") if x}
CRYPTOBOT_TOKEN = os.getenv("CRYPTOBOT_TOKEN", "")
CRYPTOBOT_API = os.getenv("CRYPTOBOT_API", "https://pay.crypt.bot/api")  # testnet: https://testnet-pay.crypt.bot/api
XROCKET_TOKEN = os.getenv("XROCKET_TOKEN", "")
XROCKET_API = os.getenv("XROCKET_API", "https://pay.xrocket.tg")
MARGIN = Decimal(os.getenv("MARGIN", "0.01"))  # 0.01 = 1%
MIN_RUB = Decimal(os.getenv("MIN_RUB", "100"))
MAX_RUB = Decimal(os.getenv("MAX_RUB", "300000"))
INVOICE_TTL = int(os.getenv("INVOICE_TTL", "900"))  # секунд на оплату
DB_PATH = os.getenv("DB_PATH", "exchange.db")

# dp — знаков после запятой для суммы в крипте, rate_dp — для показа курса
ASSETS = {
    "USDT": {"emoji": "💵", "dp": 2, "rate_dp": 2, "cryptobot": "USDT", "xrocket": "USDT"},
    "GRAM": {"emoji": "💎", "dp": 4, "rate_dp": 6, "cryptobot": "GRAM", "xrocket": "GRAM"},
}
PROVIDERS = {"cryptobot": "CryptoBot", "xrocket": "xRocket"}

STATUS = {
    "creating": "⏳ Создаётся",
    "awaiting_payment": "💳 Ожидает оплаты",
    "paid": "🆕 Оплачена, ждёт оператора",
    "in_work": "🔧 В работе",
    "done": "✅ Выполнена",
    "cancelled": "❌ Отменена",
    "expired": "⌛ Время оплаты истекло",
    "error": "⚠️ Ошибка",
}

http: aiohttp.ClientSession = None  # type: ignore


# ───────────────────────── Утилиты ─────────────────────────
def fmt(d: Decimal) -> str:
    s = f"{d:f}"
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s


def esc(s) -> str:
    return html.escape(str(s or ""))


def btn(text, data=None, url=None):
    return InlineKeyboardButton(text=text, callback_data=data, url=url)


def kb(rows):
    return InlineKeyboardMarkup(inline_keyboard=rows)


def is_admin(uid: int) -> bool:
    return uid in ADMIN_IDS


def main_menu(uid: int):
    rows = [[btn("💱 Создать заявку", "new")], [btn("📋 Мои заявки", "my")]]
    if is_admin(uid):
        rows.append([btn("🛠 Админ-панель", "a_menu")])
    return kb(rows)


async def safe_edit(c: CallbackQuery, text: str, markup=None):
    try:
        await c.message.edit_text(text, reply_markup=markup)
    except Exception:
        await c.message.answer(text, reply_markup=markup)


# ───────────────────────── База данных ─────────────────────────
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(
            """
            CREATE TABLE IF NOT EXISTS orders(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER, username TEXT, asset TEXT,
                crypto_amount TEXT, rub_amount TEXT, rate TEXT, market_rate TEXT,
                payout_details TEXT, provider TEXT, invoice_id TEXT, pay_url TEXT,
                status TEXT, admin_id INTEGER,
                created_at INTEGER, paid_at INTEGER, expires_at INTEGER
            );
            CREATE TABLE IF NOT EXISTS messages(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id INTEGER, sender TEXT, text TEXT, created_at INTEGER
            );
            CREATE TABLE IF NOT EXISTS admin_sessions(
                admin_id INTEGER PRIMARY KEY, order_id INTEGER
            );
            """
        )
        await db.commit()


async def db_exec(sql, params=()):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(sql, params)
        await db.commit()
        return cur.lastrowid, cur.rowcount


async def db_all(sql, params=()):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(sql, params)
        return [dict(r) for r in await cur.fetchall()]


async def db_one(sql, params=()):
    rows = await db_all(sql, params)
    return rows[0] if rows else None


async def get_order(oid: int):
    return await db_one("SELECT * FROM orders WHERE id=?", (oid,))


async def log_msg(oid: int, sender: str, text: str):
    await db_exec(
        "INSERT INTO messages(order_id,sender,text,created_at) VALUES(?,?,?,?)",
        (oid, sender, text, int(time.time())),
    )


# ───────────────────────── Курс ─────────────────────────
_rate_cache: dict = {}


async def get_market_rate(asset: str) -> Decimal:
    """Рыночный курс ASSET -> RUB через Crypto Pay API (getExchangeRates)."""
    cached = _rate_cache.get(asset)
    if cached and time.time() - cached[0] < 30:
        return cached[1]
    async with http.get(
        f"{CRYPTOBOT_API}/getExchangeRates", headers={"Crypto-Pay-API-Token": CRYPTOBOT_TOKEN}
    ) as r:
        data = await r.json()
    if not data.get("ok"):
        raise RuntimeError(f"getExchangeRates: {data}")
    for item in data["result"]:
        if item["source"] == ASSETS[asset]["cryptobot"] and item["target"] == "RUB" and item.get("is_valid", True):
            rate = Decimal(str(item["rate"]))
            _rate_cache[asset] = (time.time(), rate)
            return rate
    raise RuntimeError(f"Нет курса {asset}/RUB")


def our_rate(asset: str, market: Decimal) -> Decimal:
    """Курс для клиента: рыночный минус MARGIN."""
    q = Decimal(1).scaleb(-ASSETS[asset]["rate_dp"])
    return (market * (Decimal(1) - MARGIN)).quantize(q, ROUND_DOWN)


# ───────────────────────── Платёжные провайдеры ─────────────────────────
async def create_invoice(provider: str, order_id: int, asset: str, amount: Decimal):
    """Возвращает (invoice_id, pay_url)."""
    if provider == "cryptobot":
        body = {
            "asset": ASSETS[asset]["cryptobot"],
            "amount": fmt(amount),
            "description": f"Обмен #{order_id}",
            "payload": str(order_id),
            "expires_in": INVOICE_TTL,
            "allow_comments": False,
            "allow_anonymous": False,
        }
        async with http.post(
            f"{CRYPTOBOT_API}/createInvoice", json=body, headers={"Crypto-Pay-API-Token": CRYPTOBOT_TOKEN}
        ) as r:
            data = await r.json()
        if not data.get("ok"):
            raise RuntimeError(f"CryptoBot: {data}")
        res = data["result"]
        return str(res["invoice_id"]), res.get("bot_invoice_url") or res.get("pay_url")

    body = {
        "amount": float(amount),
        "numPayments": 1,
        "currency": ASSETS[asset]["xrocket"],
        "description": f"Обмен #{order_id}",
        "hiddenMessage": "Спасибо! Заявка передана оператору.",
        "commentsEnabled": False,
        "payload": str(order_id),
        "expiredIn": INVOICE_TTL,
    }
    async with http.post(
        f"{XROCKET_API}/tg-invoices", json=body, headers={"Rocket-Pay-Key": XROCKET_TOKEN}
    ) as r:
        data = await r.json()
    if not data.get("success"):
        raise RuntimeError(f"xRocket: {data}")
    d = data["data"]
    return str(d["id"]), d["link"]


async def check_invoice(provider: str, invoice_id: str) -> str:
    """Возвращает 'paid' | 'active' | 'expired'."""
    if provider == "cryptobot":
        async with http.get(
            f"{CRYPTOBOT_API}/getInvoices",
            params={"invoice_ids": invoice_id},
            headers={"Crypto-Pay-API-Token": CRYPTOBOT_TOKEN},
        ) as r:
            data = await r.json()
        items = (data.get("result") or {}).get("items") or []
        return items[0]["status"] if items else "active"
    async with http.get(f"{XROCKET_API}/tg-invoices/{invoice_id}", headers={"Rocket-Pay-Key": XROCKET_TOKEN}) as r:
        data = await r.json()
    return (data.get("data") or {}).get("status", "active")


def order_text(o: dict, for_admin: bool = False) -> str:
    a = ASSETS[o["asset"]]
    lines = [
        f"<b>Заявка #{o['id']}</b> — {STATUS.get(o['status'], o['status'])}",
        f"{a['emoji']} Отдаёте: <b>{fmt(Decimal(o['crypto_amount']))} {o['asset']}</b>",
        f"💰 Получаете: <b>{fmt(Decimal(o['rub_amount']))} ₽</b>",
        f"📈 Курс: 1 {o['asset']} = {fmt(Decimal(o['rate']))} ₽",
        f"🏦 Реквизиты: <code>{esc(o['payout_details'])}</code>",
    ]
    if o.get("provider"):
        lines.append(f"💳 Оплата: {PROVIDERS.get(o['provider'], o['provider'])}")
    if for_admin:
        uname = f"@{esc(o['username'])}" if o.get("username") else "—"
        lines.append(f"👤 Клиент: {uname} (<code>{o['user_id']}</code>)")
        lines.append(f"🧑‍💼 Оператор: {o['admin_id'] or '—'}")
        lines.append(f"📊 Рыночный курс при создании: {fmt(Decimal(o['market_rate']))} ₽")
    lines.append(f"🕒 Создана: {time.strftime('%d.%m.%Y %H:%M', time.localtime(o['created_at']))}")
    return "\n".join(lines)


# ───────────────────────── Обработка оплаты ─────────────────────────
async def handle_paid(bot: Bot, oid: int):
    _, n = await db_exec(
        "UPDATE orders SET status='paid', paid_at=? WHERE id=? AND status='awaiting_payment'",
        (int(time.time()), oid),
    )
    if not n:
        return  # уже обработана
    o = await get_order(oid)
    try:
        await bot.send_message(
            o["user_id"],
            f"✅ Оплата по заявке <b>#{oid}</b> получена!\n"
            f"Заявка передана оператору, он скоро возьмёт её в работу и свяжется с вами здесь.",
        )
    except Exception:
        log.exception("notify user")
    for admin in ADMIN_IDS:
        try:
            await bot.send_message(
                admin,
                "🆕 <b>Новая оплаченная заявка</b>\n\n" + order_text(o, for_admin=True),
                reply_markup=kb([[btn("✅ Взять в работу", f"a_take:{oid}")], [btn("📄 Открыть", f"a_view:{oid}")]]),
            )
        except Exception:
            log.exception("notify admin %s", admin)


async def payment_watcher(bot: Bot):
    while True:
        try:
            for o in await db_all("SELECT * FROM orders WHERE status='awaiting_payment'"):
                try:
                    st = await check_invoice(o["provider"], o["invoice_id"])
                except Exception:
                    log.exception("check_invoice #%s", o["id"])
                    continue
                if st == "paid":
                    await handle_paid(bot, o["id"])
                elif st == "expired":
                    _, n = await db_exec(
                        "UPDATE orders SET status='expired' WHERE id=? AND status='awaiting_payment'", (o["id"],)
                    )
                    if n:
                        try:
                            await bot.send_message(
                                o["user_id"],
                                f"⌛ Время оплаты заявки #{o['id']} истекло. Создайте новую заявку.",
                                reply_markup=main_menu(o["user_id"]),
                            )
                        except Exception:
                            pass
        except Exception:
            log.exception("watcher")
        await asyncio.sleep(10)


# ═════════════════════════ ПОЛЬЗОВАТЕЛЬ ═════════════════════════
user_router = Router()


class Ex(StatesGroup):
    amount = State()
    details = State()


@user_router.message(CommandStart())
async def cmd_start(m: Message, state: FSMContext):
    await state.clear()
    await m.answer(
        "👋 <b>Криптообменник</b>\n\nОбмен USDT и GRAM на рубли.",
        reply_markup=main_menu(m.from_user.id),
    )


@user_router.callback_query(F.data == "menu")
async def cb_menu(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await safe_edit(c, "Главное меню:", main_menu(c.from_user.id))
    await c.answer()


@user_router.callback_query(F.data == "new")
async def cb_new(c: CallbackQuery, state: FSMContext):
    await state.clear()
    rows = [[btn(f"{v['emoji']} {k}", f"asset:{k}") for k, v in ASSETS.items()], [btn("⬅️ Меню", "menu")]]
    await safe_edit(c, "Что хотите продать?", kb(rows))
    await c.answer()


@user_router.callback_query(F.data.startswith("asset:"))
async def cb_asset(c: CallbackQuery, state: FSMContext):
    asset = c.data.split(":")[1]
    if asset not in ASSETS:
        return await c.answer()
    try:
        rate = our_rate(asset, await get_market_rate(asset))
    except Exception:
        log.exception("rate")
        return await c.answer("Не удалось получить курс, попробуйте позже", show_alert=True)
    await state.update_data(asset=asset)
    await safe_edit(
        c,
        f"{ASSETS[asset]['emoji']} <b>{asset} → RUB</b>\nКурс: 1 {asset} = <b>{fmt(rate)} ₽</b>\n\nВ чём указать сумму?",
        kb(
            [
                [btn(f"В {asset}", "mode:crypto"), btn("В рублях", "mode:rub")],
                [btn("⬅️ Назад", "new")],
            ]
        ),
    )
    await c.answer()


@user_router.callback_query(F.data.startswith("mode:"))
async def cb_mode(c: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if "asset" not in data:
        return await c.answer("Сессия устарела, начните заново", show_alert=True)
    mode = c.data.split(":")[1]
    await state.update_data(mode=mode)
    await state.set_state(Ex.amount)
    unit = data["asset"] if mode == "crypto" else "₽"
    await safe_edit(
        c,
        f"Введите сумму в <b>{unit}</b>:\n(лимиты: {fmt(MIN_RUB)} – {fmt(MAX_RUB)} ₽)",
        kb([[btn("❌ Отмена", "menu")]]),
    )
    await c.answer()


@user_router.message(Ex.amount, F.text)
async def st_amount(m: Message, state: FSMContext):
    data = await state.get_data()
    asset, mode = data["asset"], data["mode"]
    try:
        value = Decimal(m.text.replace(",", ".").replace(" ", ""))
        if value <= 0:
            raise InvalidOperation
    except InvalidOperation:
        return await m.answer("Введите число, например <code>100</code> или <code>12.5</code>")
    try:
        market = await get_market_rate(asset)
    except Exception:
        log.exception("rate")
        return await m.answer("Не удалось получить курс, попробуйте ещё раз чуть позже.")
    rate = our_rate(asset, market)
    q = Decimal(1).scaleb(-ASSETS[asset]["dp"])
    if mode == "crypto":
        crypto = value.quantize(q, ROUND_DOWN)
        rub = (crypto * rate).quantize(Decimal("0.01"), ROUND_DOWN)
    else:
        rub = value.quantize(Decimal("0.01"), ROUND_DOWN)
        crypto = (rub / rate).quantize(q, ROUND_UP)
    if crypto <= 0 or rub < MIN_RUB or rub > MAX_RUB:
        return await m.answer(f"Сумма должна быть от {fmt(MIN_RUB)} до {fmt(MAX_RUB)} ₽. Введите другую.")
    await state.update_data(crypto=str(crypto), rub=str(rub), rate=str(rate), market=str(market))
    await state.set_state(Ex.details)
    await m.answer(
        f"Вы отдаёте <b>{fmt(crypto)} {asset}</b>, получаете <b>{fmt(rub)} ₽</b>.\n\n"
        "Введите реквизиты для выплаты (карта / СБП-телефон + банк, ФИО получателя):",
        reply_markup=kb([[btn("❌ Отмена", "menu")]]),
    )


@user_router.message(Ex.details, F.text)
async def st_details(m: Message, state: FSMContext):
    text = m.text.strip()
    if not 5 <= len(text) <= 300:
        return await m.answer("Реквизиты должны быть от 5 до 300 символов. Попробуйте снова.")
    await state.update_data(details=text)
    d = await state.get_data()
    rows = []
    if CRYPTOBOT_TOKEN:
        rows.append([btn("💳 Оплатить через CryptoBot", "pay:cryptobot")])
    if XROCKET_TOKEN:
        rows.append([btn("🚀 Оплатить через xRocket", "pay:xrocket")])
    rows.append([btn("❌ Отмена", "menu")])
    await m.answer(
        "<b>Проверьте заявку</b>\n\n"
        f"{ASSETS[d['asset']]['emoji']} Отдаёте: <b>{fmt(Decimal(d['crypto']))} {d['asset']}</b>\n"
        f"💰 Получаете: <b>{fmt(Decimal(d['rub']))} ₽</b>\n"
        f"📈 Курс: 1 {d['asset']} = {fmt(Decimal(d['rate']))} ₽\n"
        f"🏦 Реквизиты: <code>{esc(text)}</code>\n\n"
        f"Выберите способ оплаты. На оплату даётся {INVOICE_TTL // 60} мин.",
        reply_markup=kb(rows),
    )


@user_router.callback_query(F.data.startswith("pay:"))
async def cb_pay(c: CallbackQuery, state: FSMContext):
    provider = c.data.split(":")[1]
    d = await state.get_data()
    if provider not in PROVIDERS or "details" not in d:
        return await c.answer("Сессия устарела, создайте заявку заново", show_alert=True)
    now = int(time.time())
    oid, _ = await db_exec(
        """INSERT INTO orders(user_id,username,asset,crypto_amount,rub_amount,rate,market_rate,
           payout_details,provider,status,created_at,expires_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
        (c.from_user.id, c.from_user.username, d["asset"], d["crypto"], d["rub"], d["rate"], d["market"],
         d["details"], provider, "creating", now, now + INVOICE_TTL),
    )
    try:
        inv_id, url = await create_invoice(provider, oid, d["asset"], Decimal(d["crypto"]))
    except Exception:
        log.exception("create_invoice")
        await db_exec("UPDATE orders SET status='error' WHERE id=?", (oid,))
        return await c.answer("Не удалось создать счёт, попробуйте другой способ или позже", show_alert=True)
    await db_exec(
        "UPDATE orders SET invoice_id=?, pay_url=?, status='awaiting_payment' WHERE id=?", (inv_id, url, oid)
    )
    await state.clear()
    await safe_edit(
        c,
        f"💳 <b>Заявка #{oid}</b>\nК оплате: <b>{fmt(Decimal(d['crypto']))} {d['asset']}</b> "
        f"через {PROVIDERS[provider]}.\nСчёт действует {INVOICE_TTL // 60} мин.\n\n"
        "После оплаты заявка автоматически уйдёт оператору.",
        kb([[btn("💳 Оплатить", url=url)], [btn("🔄 Проверить оплату", f"check:{oid}")]]),
    )
    await c.answer()


@user_router.callback_query(F.data.startswith("check:"))
async def cb_check(c: CallbackQuery, bot: Bot):
    o = await get_order(int(c.data.split(":")[1]))
    if not o or o["user_id"] != c.from_user.id:
        return await c.answer()
    if o["status"] != "awaiting_payment":
        return await c.answer(STATUS.get(o["status"], o["status"]), show_alert=True)
    try:
        st = await check_invoice(o["provider"], o["invoice_id"])
    except Exception:
        return await c.answer("Ошибка проверки, попробуйте позже", show_alert=True)
    if st == "paid":
        await handle_paid(bot, o["id"])
        await c.answer("Оплата получена ✅")
    else:
        await c.answer("Оплата пока не найдена", show_alert=True)


@user_router.callback_query(F.data == "my")
async def cb_my(c: CallbackQuery):
    rows = await db_all(
        "SELECT * FROM orders WHERE user_id=? AND status NOT IN ('creating','error') ORDER BY id DESC LIMIT 10",
        (c.from_user.id,),
    )
    if not rows:
        await safe_edit(c, "У вас пока нет заявок.", kb([[btn("⬅️ Меню", "menu")]]))
    else:
        buttons = [
            [btn(f"#{o['id']} · {fmt(Decimal(o['crypto_amount']))} {o['asset']} · {STATUS[o['status']]}", f"my_view:{o['id']}")]
            for o in rows
        ]
        buttons.append([btn("⬅️ Меню", "menu")])
        await safe_edit(c, "<b>Ваши заявки:</b>", kb(buttons))
    await c.answer()


@user_router.callback_query(F.data.startswith("my_view:"))
async def cb_my_view(c: CallbackQuery):
    o = await get_order(int(c.data.split(":")[1]))
    if not o or o["user_id"] != c.from_user.id:
        return await c.answer()
    rows = []
    if o["status"] == "awaiting_payment":
        rows.append([btn("💳 Оплатить", url=o["pay_url"])])
        rows.append([btn("🔄 Проверить оплату", f"check:{o['id']}")])
    rows.append([btn("⬅️ К списку", "my")])
    await safe_edit(c, order_text(o), kb(rows))
    await c.answer()


# ───────── Чат: сообщения вне сценариев (клиент <-> оператор) ─────────
async def relay_to_user(bot: Bot, m: Message, o: dict):
    if m.text:
        await bot.send_message(o["user_id"], f"💬 <b>Оператор (заявка #{o['id']}):</b>\n{esc(m.text)}")
    else:
        await bot.send_message(o["user_id"], f"💬 <b>Оператор (заявка #{o['id']}):</b>")
        await bot.copy_message(o["user_id"], m.chat.id, m.message_id)
    await log_msg(o["id"], "admin", m.text or f"[{m.content_type}]")


@user_router.message(StateFilter(None))
async def generic_message(m: Message, bot: Bot):
    if m.text and m.text.startswith("/"):
        return
    uid = m.from_user.id

    # админ в режиме чата -> пишем клиенту
    if is_admin(uid):
        sess = await db_one("SELECT * FROM admin_sessions WHERE admin_id=?", (uid,))
        if sess:
            o = await get_order(sess["order_id"])
            if o and o["status"] == "in_work":
                try:
                    await relay_to_user(bot, m, o)
                    return await m.reply(f"✉️ Отправлено клиенту (заявка #{o['id']})")
                except Exception:
                    return await m.reply("⚠️ Не удалось отправить (клиент мог заблокировать бота).")
            await db_exec("DELETE FROM admin_sessions WHERE admin_id=?", (uid,))
            return await m.reply("Чат закрыт: заявка больше не в работе.")

    # клиент с заявкой в работе -> пишем оператору
    o = await db_one("SELECT * FROM orders WHERE user_id=? AND status='in_work' ORDER BY id DESC LIMIT 1", (uid,))
    if o and o["admin_id"]:
        uname = f"@{esc(m.from_user.username)}" if m.from_user.username else esc(m.from_user.full_name)
        try:
            head = f"💬 <b>Заявка #{o['id']}</b> · {uname}:"
            markup = kb([[btn("💬 Ответить", f"a_chat:{o['id']}"), btn("📄 Заявка", f"a_view:{o['id']}")]])
            if m.text:
                await bot.send_message(o["admin_id"], f"{head}\n{esc(m.text)}", reply_markup=markup)
            else:
                await bot.send_message(o["admin_id"], head, reply_markup=markup)
                await bot.copy_message(o["admin_id"], m.chat.id, m.message_id)
            await log_msg(o["id"], "user", m.text or f"[{m.content_type}]")
            return await m.reply("✉️ Передано оператору")
        except Exception:
            log.exception("relay to admin")
            return await m.reply("Не удалось передать сообщение, попробуйте позже.")

    paid = await db_one("SELECT id FROM orders WHERE user_id=? AND status='paid' LIMIT 1", (uid,))
    if paid:
        return await m.answer(f"Заявка #{paid['id']} ждёт оператора, он скоро свяжется с вами.")
    await m.answer("Выберите действие:", reply_markup=main_menu(uid))


# ═════════════════════════ АДМИНИСТРАТОР ═════════════════════════
admin_router = Router()
admin_router.message.filter(F.from_user.id.in_(ADMIN_IDS))
admin_router.callback_query.filter(F.from_user.id.in_(ADMIN_IDS))

PAGE = 8
LIST_TITLES = {
    "paid": "🆕 Оплаченные (ждут оператора)",
    "in_work": "🔧 В работе",
    "done": "✅ Выполненные",
    "all": "📋 Все заявки",
}


async def panel_text_kb():
    cnt = {r["status"]: r["c"] for r in await db_all("SELECT status, COUNT(*) c FROM orders GROUP BY status")}
    text = (
        "🛠 <b>Админ-панель</b>\n\n"
        f"🆕 Новые оплаченные: <b>{cnt.get('paid', 0)}</b>\n"
        f"🔧 В работе: <b>{cnt.get('in_work', 0)}</b>\n"
        f"✅ Выполнено: <b>{cnt.get('done', 0)}</b>\n\n"
        "Найти заявку по номеру: <code>/order 12</code>"
    )
    markup = kb(
        [
            [btn("🆕 Оплаченные", "a_list:paid:0"), btn("🔧 В работе", "a_list:in_work:0")],
            [btn("✅ Выполненные", "a_list:done:0"), btn("📋 Все", "a_list:all:0")],
            [btn("🚪 Выйти из чата", "a_leave")],
            [btn("⬅️ Меню", "menu")],
        ]
    )
    return text, markup


@admin_router.message(Command("admin"))
async def cmd_admin(m: Message, state: FSMContext):
    await state.clear()
    text, markup = await panel_text_kb()
    await m.answer(text, reply_markup=markup)


@admin_router.callback_query(F.data == "a_menu")
async def cb_a_menu(c: CallbackQuery):
    text, markup = await panel_text_kb()
    await safe_edit(c, text, markup)
    await c.answer()


@admin_router.callback_query(F.data.startswith("a_list:"))
async def cb_a_list(c: CallbackQuery):
    _, status, page = c.data.split(":")
    page = int(page)
    where, params = ("", ()) if status == "all" else ("WHERE status=?", (status,))
    total = (await db_one(f"SELECT COUNT(*) c FROM orders {where}", params))["c"]
    rows = await db_all(
        f"SELECT * FROM orders {where} ORDER BY id DESC LIMIT ? OFFSET ?", params + (PAGE, page * PAGE)
    )
    buttons = [
        [btn(f"#{o['id']} · {fmt(Decimal(o['crypto_amount']))} {o['asset']} → {fmt(Decimal(o['rub_amount']))}₽ · "
             f"{STATUS[o['status']].split(' ')[0]}", f"a_view:{o['id']}")]
        for o in rows
    ]
    nav = []
    if page > 0:
        nav.append(btn("◀️", f"a_list:{status}:{page - 1}"))
    if (page + 1) * PAGE < total:
        nav.append(btn("▶️", f"a_list:{status}:{page + 1}"))
    if nav:
        buttons.append(nav)
    buttons.append([btn("⬅️ Админ-панель", "a_menu")])
    await safe_edit(c, f"<b>{LIST_TITLES[status]}</b> — всего {total}", kb(buttons))
    await c.answer()


def order_kb(o: dict, uid: int):
    oid = o["id"]
    rows = []
    if o["status"] == "paid":
        rows.append([btn("✅ Взять в работу", f"a_take:{oid}")])
    if o["status"] == "in_work" and o["admin_id"] == uid:
        rows.append([btn("💬 Чат с клиентом", f"a_chat:{oid}")])
        rows.append([btn("✅ Выполнено", f"a_done:{oid}"), btn("❌ Отменить", f"a_cancel:{oid}")])
    if o["status"] in ("in_work", "done", "cancelled"):
        rows.append([btn("📜 История переписки", f"a_hist:{oid}")])
    rows.append([btn("⬅️ Админ-панель", "a_menu")])
    return kb(rows)


async def show_order(c: CallbackQuery, oid: int):
    o = await get_order(oid)
    if not o:
        return await c.answer("Заявка не найдена", show_alert=True)
    await safe_edit(c, order_text(o, for_admin=True), order_kb(o, c.from_user.id))


@admin_router.callback_query(F.data.startswith("a_view:"))
async def cb_a_view(c: CallbackQuery):
    await show_order(c, int(c.data.split(":")[1]))
    await c.answer()


@admin_router.message(Command("order"))
async def cmd_order(m: Message):
    parts = (m.text or "").split()
    if len(parts) < 2 or not parts[1].isdigit():
        return await m.answer("Использование: <code>/order 12</code>")
    o = await get_order(int(parts[1]))
    if not o:
        return await m.answer("Заявка не найдена")
    await m.answer(order_text(o, for_admin=True), reply_markup=order_kb(o, m.from_user.id))


@admin_router.callback_query(F.data.startswith("a_take:"))
async def cb_a_take(c: CallbackQuery, bot: Bot):
    oid = int(c.data.split(":")[1])
    _, n = await db_exec(
        "UPDATE orders SET status='in_work', admin_id=? WHERE id=? AND status='paid'", (c.from_user.id, oid)
    )
    if not n:
        await c.answer("Заявка уже взята другим оператором или недоступна", show_alert=True)
        return await show_order(c, oid)
    o = await get_order(oid)
    await db_exec("INSERT OR REPLACE INTO admin_sessions(admin_id, order_id) VALUES(?,?)", (c.from_user.id, oid))
    try:
        await bot.send_message(
            o["user_id"],
            f"🔧 Заявка <b>#{oid}</b> взята в работу.\nОператор на связи — можете писать сюда сообщения, "
            "они будут переданы напрямую.",
        )
    except Exception:
        pass
    await safe_edit(
        c,
        order_text(o, for_admin=True) + "\n\n💬 <i>Чат с клиентом включён: всё, что вы пишете боту, "
        "уходит клиенту. Выйти: кнопка «Выйти из чата».</i>",
        order_kb(o, c.from_user.id),
    )
    await c.answer("Заявка в работе")


@admin_router.callback_query(F.data.startswith("a_chat:"))
async def cb_a_chat(c: CallbackQuery):
    oid = int(c.data.split(":")[1])
    o = await get_order(oid)
    if not o or o["status"] != "in_work" or o["admin_id"] != c.from_user.id:
        return await c.answer("Чат доступен только по вашим заявкам в работе", show_alert=True)
    await db_exec("INSERT OR REPLACE INTO admin_sessions(admin_id, order_id) VALUES(?,?)", (c.from_user.id, oid))
    await c.message.answer(
        f"💬 Чат по заявке <b>#{oid}</b> включён. Пишите — сообщения уйдут клиенту.",
        reply_markup=kb([[btn("🚪 Выйти из чата", "a_leave")]]),
    )
    await c.answer()


@admin_router.callback_query(F.data == "a_leave")
async def cb_a_leave(c: CallbackQuery):
    await db_exec("DELETE FROM admin_sessions WHERE admin_id=?", (c.from_user.id,))
    await c.answer("Вы вышли из чата")


@admin_router.callback_query(F.data.startswith("a_hist:"))
async def cb_a_hist(c: CallbackQuery):
    oid = int(c.data.split(":")[1])
    rows = await db_all("SELECT * FROM messages WHERE order_id=? ORDER BY id DESC LIMIT 30", (oid,))
    if not rows:
        return await c.answer("Переписки пока нет", show_alert=True)
    lines = [
        f"{time.strftime('%d.%m %H:%M', time.localtime(r['created_at']))} "
        f"{'🧑‍💼' if r['sender'] == 'admin' else '👤'} {esc(r['text'])}"
        for r in reversed(rows)
    ]
    await c.message.answer(f"📜 <b>Переписка по заявке #{oid}</b>\n\n" + "\n".join(lines)[:3900])
    await c.answer()


async def finish_order(c: CallbackQuery, bot: Bot, new_status: str, user_text: str):
    oid = int(c.data.split(":")[1])
    _, n = await db_exec(
        "UPDATE orders SET status=? WHERE id=? AND status='in_work' AND admin_id=?",
        (new_status, oid, c.from_user.id),
    )
    if not n:
        return await c.answer("Недоступно", show_alert=True)
    await db_exec("DELETE FROM admin_sessions WHERE order_id=?", (oid,))
    o = await get_order(oid)
    try:
        await bot.send_message(o["user_id"], user_text.format(id=oid), reply_markup=main_menu(o["user_id"]))
    except Exception:
        pass
    await show_order(c, oid)
    await c.answer("Готово")


@admin_router.callback_query(F.data.startswith("a_done:"))
async def cb_a_done(c: CallbackQuery, bot: Bot):
    await finish_order(c, bot, "done", "✅ Заявка <b>#{id}</b> выполнена. Средства отправлены на ваши реквизиты. Спасибо!")


@admin_router.callback_query(F.data.startswith("a_cancel:"))
async def cb_a_cancel(c: CallbackQuery, bot: Bot):
    await finish_order(
        c, bot, "cancelled",
        "❌ Заявка <b>#{id}</b> отменена оператором. Если вопрос с возвратом средств не решён — напишите в поддержку.",
    )


# ───────────────────────── Запуск ─────────────────────────
async def main():
    global http
    await init_db()
    http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(admin_router)  # админ-хэндлеры раньше пользовательских
    dp.include_router(user_router)
    asyncio.create_task(payment_watcher(bot))
    try:
        await dp.start_polling(bot)
    finally:
        await http.close()


if __name__ == "__main__":
    asyncio.run(main())
