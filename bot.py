"""
Discord-бот для тестирования стратегий zapret2 (VOICE_UDP, профиль 6).

Стратегии НЕ хранятся в статическом yaml-файле — при каждом старте (и по
команде /refresh_strategies) бот спрашивает актуальное число стратегий
напрямую у z2r через set_strategy_cli.sh (та же config_profile_max_strategy(),
которой пользуются rank_strategies.sh/rank_quic.sh). Это исключает
рассинхронизацию после обновления стратегий в самом z2r (пункт 5 меню и т.п.)
— не нужно ничего перегенерировать вручную.

С 2026-08-07 бот работает от имени ОТДЕЛЬНОГО юзера zenith-voice-bot
(см. README) и тестирует ЧЕРЕЗ ПЕСОЧНИЦУ Zenith (изолированный nfqws2),
а не напрямую через боевой locked.tsv, как раньше -- песочница нужна,
чтобы Zenith мог гонять через ЭТОТ ЖЕ бот свои непроверенные
сгенерированные геномы, не трогая прод. Номер стратегии по-прежнему
читается из /opt/zapret2/config (extract_strategy_lines), просто
применяется в конфиг песочницы вместо set_strategy_cli.sh set.

НЕ тот же юзер, что zenith-sandbox (который использует Zenith для своих
curl-проверок TCP-профилей) -- живой инцидент 2026-08-07: у бота есть
свой обычный TCP-трафик (Discord gateway/API login), не связанный с
тестированием, и он ЗАВИСАЛ, будучи пойман тем же широким TCP-правилом
zenith-sandbox -- nfqws2 пытается реассемблировать TLS ClientHello даже
без единого --filter-tcp= в конфиге ("no lua functions in this
profile"), и в этом no-op режиме реассемблинг с replay иногда дропает
исходный пакет вместо чистого пропуска. У zenith-voice-bot -- узкое
правило ТОЛЬКО на UDP-порты голосового профиля (см.
sandbox/setup_sandbox.sh), обычный HTTPS-трафик бота его не касается.

Сценарий:
  1. /voice_test <strategy>     — применить стратегию, зайти в тестовый
                                   голосовой канал, продержать соединение
                                   N секунд, результат — в ЛС.
  2. /voice_test_all            — прогнать все стратегии по одному разу.
  3. /voice_rank <passes>       — прогнать все стратегии N раз подряд и
                                   прислать рейтинг (надёжность + скорость
                                   подключения), аналог rank_strategies.sh
                                   --passes, но для реального Discord-голоса.
  4. /strategy_list             — текущий список стратегий (актуальный на
                                   момент последней загрузки/refresh).
  5. /strategy_status           — какая стратегия применена последней.
  6. /refresh_strategies        — перечитать число стратегий у z2r заново
                                   (нужно после ручных изменений в z2r,
                                   пока бот уже запущен).
  7. /voice_leave                — аварийный выход бота из войса руками.

Плюс автоматический фоновый цикл auto_voice_check (см.
AUTO_CHECK_INTERVAL_HOURS/AUTO_CHECK_PASSES) -- та же логика, что
/voice_rank, только сам по расписанию, без ручной команды; отчёт уходит
владельцу в ЛС тем же notify(). Прямого способа доставить результат
куда-то ещё, кроме Discord (например, в чат с Claude), у бота нет --
отчёт нужно переслать вручную, если требуется его куда-то передать.

Плюс локальный HTTP (127.0.0.1 по умолчанию) -- POST /probe с
{"lua_desync_lines": [...]} -- для Zenith orchestrator/voice_tester.py.
Отдельного Discord-токена/бота Zenith'у для этого не нужно, дёргает уже
залогиненный этот процесс.
"""

import asyncio
import logging
import os
import time
from dataclasses import dataclass, field

import discord
from aiohttp import web
from discord import app_commands
from discord.ext import commands, tasks
from dotenv import load_dotenv

load_dotenv()

DISCORD_TOKEN = os.environ["DISCORD_TOKEN"]
GUILD_ID = int(os.environ["GUILD_ID"])
TEST_VOICE_CHANNEL_ID = int(os.environ["TEST_VOICE_CHANNEL_ID"])
OWNER_USER_ID = int(os.environ["OWNER_USER_ID"])

SET_STRATEGY_CLI = os.environ.get(
    "SET_STRATEGY_CLI", "/opt/z2r_autobench/set_strategy_cli.sh"
)
VOICE_PROFILE = os.environ.get("VOICE_PROFILE", "6")
VOICE_PROTO = os.environ.get("VOICE_PROTO", "udp")

HOLD_SECONDS = int(os.environ.get("HOLD_SECONDS", "5"))            # сколько держим соединение перед тем как считать успехом
CONNECT_TIMEOUT = int(os.environ.get("CONNECT_TIMEOUT", "15"))     # таймаут на сам connect()
STRATEGY_SWITCH_DELAY = int(os.environ.get("STRATEGY_SWITCH_DELAY", "5"))  # пауза между стратегиями

# Автоматический периодический прогон ВСЕХ стратегий без ручной команды --
# та же логика, что /voice_rank (run_check_sweep/build_check_summary ниже),
# просто по расписанию вместо ручного вызова. Отчёт уходит тем же notify()
# в ЛС владельцу, что и у ручных команд -- отдельного публичного канала/
# вебхука не заводим, у этого бота и так нет способа доставить результат
# куда-то ещё, кроме Discord. AUTO_CHECK_INTERVAL_HOURS<=0 отключает цикл
# полностью (полезно на серверах, где раньше бота ставили без этого).
AUTO_CHECK_INTERVAL_HOURS = float(os.environ.get("AUTO_CHECK_INTERVAL_HOURS", "6"))
AUTO_CHECK_PASSES = int(os.environ.get("AUTO_CHECK_PASSES", "1"))

# --- песочница Zenith (замена прямого apply_cmd в locked.tsv, см. докстринг) ---
ZAPRET_CONFIG_PATH = os.environ.get("ZAPRET_CONFIG_PATH", "/opt/zapret2/config")
ZENITH_SANDBOX_DIR = os.environ.get("ZENITH_SANDBOX_DIR", "/opt/z2r_autobench/Zenith/sandbox")
ZENITH_SANDBOX_CONF = os.path.join(ZENITH_SANDBOX_DIR, "nfqws2_sandbox.conf")
ZENITH_START_SCRIPT = os.path.join(ZENITH_SANDBOX_DIR, "start_sandbox.sh")
ZENITH_WRITE_CONF_SCRIPT = os.path.join(ZENITH_SANDBOX_DIR, "write_conf.sh")

# Тот же фильтр, что genome.PROFILE_FILTERS["VOICE_UDP"] в Zenith -- сверено
# построчно с /opt/zapret2/config, см. Zenith/orchestrator/genome.py. Держим
# копией, а не общим импортом -- два независимо разворачиваемых репозитория.
VOICE_FILTER_LINES = [
    "--filter-udp=443,2053,2083,2087,2096,8443,50000-50099,1400,3478-3481,5349,19294-19344",
    "--filter-l7=discord,stun",
    "--payload=discord_ip_discovery,stun",
]
_SANDBOX_REWRITE_PREFIXES = (
    "--filter-tcp=", "--filter-udp=", "--filter-l7=",
    "--hostlist=", "--hostlist-exclude=", "--hostlist-domains=",
    "--payload=", "--lua-desync=",
)

ZENITH_PROBE_HOST = os.environ.get("ZENITH_PROBE_HOST", "127.0.0.1")
ZENITH_PROBE_PORT = int(os.environ.get("ZENITH_PROBE_PORT", "8765"))

# Лог результатов в TSV, аналогичный по духу rank_strategies.sh/rank_quic.sh
# (pass, strategy, attempt, success, метрика), но метрика тут — время
# подключения в мс (меньше = лучше, в отличие от bytes у остальных тестов,
# где больше = лучше — учитываем это при сортировке рейтинга).
VOICE_LOG_DIR = os.environ.get("VOICE_LOG_DIR", "/opt/z2r_autobench/logs")
VOICE_RAW_FILE = os.path.join(VOICE_LOG_DIR, "voice_bot_raw.tsv")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("zapret-voice-bot")


@dataclass
class Strategy:
    name: str
    description: str
    strategy_n: int


@dataclass
class BotState:
    strategies: dict[str, Strategy] = field(default_factory=dict)
    strategies_error: str = ""
    startup_alert_sent: bool = False
    current_strategy: str | None = None
    last_apply_ok: bool | None = None
    last_apply_output: str = ""
    last_apply_ts: float | None = None


state = BotState()

# Сериализует ЛЮБОЙ прогон (ручные /voice_test, /voice_test_all,
# /voice_rank и автоматический цикл auto_voice_check) -- все они по
# очереди двигают ОДНУ и ту же песочницу Zenith и подключаются в ОДИН и
# тот же тестовый голосовой канал; параллельный запуск двух прогонов
# испортил бы оба одновременно выполняющихся теста, а не просто дал
# неверный результат одному из них.
_check_lock = asyncio.Lock()


def log_voice_result(pass_num: int, strategy: str, attempt: int, result: "VoiceTestResult") -> None:
    """Пишет результат в TSV. Формат: ts, pass, strategy, attempt, success, connect_ms, note."""
    os.makedirs(VOICE_LOG_DIR, exist_ok=True)
    new_file = not os.path.exists(VOICE_RAW_FILE)
    with open(VOICE_RAW_FILE, "a", encoding="utf-8") as f:
        if new_file:
            f.write("ts\tpass\tstrategy\tattempt\tsuccess\tconnect_ms\tnote\n")
        connect_ms = int(result.connect_time_s * 1000)
        note = (result.connect_error or result.hold_note or "").replace("\t", " ").replace("\n", " ")
        f.write(
            f"{time.strftime('%Y-%m-%dT%H:%M:%S')}\t{pass_num}\t{strategy}\t{attempt}\t"
            f"{1 if result.success else 0}\t{connect_ms}\t{note}\n"
        )


async def query_max_strategy(profile: str) -> tuple[int | None, str]:
    """Спрашивает у z2r (через set_strategy_cli.sh max) актуальное число
    стратегий профиля — ту же config_profile_max_strategy(), которой
    пользуется остальная автоматика (rank_strategies.sh и т.п.). Так список
    стратегий бота никогда не рассинхронизируется со статическим файлом,
    потому что статического файла со списком стратегий просто нет.

    Возвращает (число, "") при успехе или (None, причина) при провале —
    причина пробрасывается наружу вместо того, чтобы теряться в локальном
    логе, см. build_strategies()."""
    proc = await asyncio.create_subprocess_exec(
        "sudo", "bash", SET_STRATEGY_CLI, "max", profile,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    stderr_text = stderr.decode(errors="replace").strip()
    if proc.returncode != 0:
        err = f"{SET_STRATEGY_CLI} max {profile} завершился с кодом {proc.returncode}: {stderr_text or '(stderr пуст)'}"
        log.error(err)
        return None, err
    text = stdout.decode(errors="replace").strip()
    if not text.isdigit():
        err = f"неожиданный вывод max_strategy для profile={profile}: {text!r} (stderr: {stderr_text or 'пуст'})"
        log.error(err)
        return None, err
    return int(text), ""


async def build_strategies() -> tuple[dict[str, Strategy], str]:
    """Строит список стратегий на лету по актуальному числу от z2r.
    query_max_strategy -- только чтение (config_profile_max_strategy),
    locked.tsv не трогает. Возвращает (словарь, "") при успехе или
    ({}, причина) при провале."""
    max_voice, err = await query_max_strategy(VOICE_PROFILE)
    if max_voice is None:
        log.warning("Не удалось определить число стратегий VOICE_UDP, список будет пуст: %s", err)
        return {}, err
    result: dict[str, Strategy] = {}
    for s in range(1, max_voice + 1):
        name = f"voice_{s}"
        result[name] = Strategy(name=name, description=f"VOICE_UDP={s}", strategy_n=s)
    return result, ""


async def ensure_strategies_loaded() -> bool:
    """Если список стратегий пуст (например из-за сбоя при старте),
    перед тем как отказывать пользователю пробует перечитать его у z2r
    ещё раз -- тот же build_strategies(), что и /refresh_strategies,
    просто без ручного вызова команды. Возвращает True, если список
    не пуст после попытки."""
    if state.strategies:
        return True
    log.info("Список стратегий пуст, пробую автоматически перечитать перед выполнением команды...")
    state.strategies, state.strategies_error = await build_strategies()
    return bool(state.strategies)


def extract_strategy_lines(config_path: str, key: str, strategy_n: int) -> list[str]:
    """Вытаскивает все --lua-desync=...:strategy=N инстансы (может быть
    несколько на одну физическую строку конфига, разделены пробелом перед
    следующим --lua-desync=) из блока, привязанного к
    circular_locked:key={key}:proto=udp -- до ближайшей пустой строки/--new.
    Та же ручная логика, которой в этой сессии вытаскивали control-геномы
    для Zenith (grep -n -B/-A по /opt/zapret2/config), просто автоматом."""
    try:
        with open(config_path) as f:
            lines = f.read().splitlines()
    except FileNotFoundError:
        return []

    marker = f"circular_locked:key={key}:proto=udp"
    start = None
    for i, line in enumerate(lines):
        if marker in line:
            start = i
            break
    if start is None:
        return []

    tag = f":strategy={strategy_n}"
    result: list[str] = []
    for line in lines[start + 1:]:
        stripped = line.strip()
        if stripped == "" or stripped == "--new":
            break
        for token in stripped.split(" --lua-desync="):
            token = token if token.startswith("--lua-desync=") else "--lua-desync=" + token
            if tag in token and (token.endswith(tag) or f"{tag}:" in token):
                result.append(token)
    return result


def apply_to_sandbox(lua_lines: list[str]) -> tuple[bool, str]:
    """То же самое, что sandbox_apply.apply_raw() в Zenith -- переписывает
    только строки фильтра/--lua-desync= в конфиге песочницы, перезапускает
    её через start_sandbox.sh. Не трогает /opt/zapret2 вообще (в отличие от
    старого apply_cmd=set_strategy_cli.sh set, который менял боевой
    locked.tsv)."""
    try:
        with open(ZENITH_SANDBOX_CONF) as f:
            existing = f.readlines()
    except FileNotFoundError:
        return False, f"{ZENITH_SANDBOX_CONF} не найден -- запусти Zenith/sandbox/start_sandbox.sh хотя бы раз вручную"

    kept = [ln for ln in existing if not ln.strip().startswith(_SANDBOX_REWRITE_PREFIXES)]
    for line in VOICE_FILTER_LINES:
        kept.append(line + "\n")
    for line in lua_lines:
        kept.append(line + "\n")

    import subprocess

    # Пишем через root-скрипт, не open() напрямую -- конфиг обычно
    # root-owned (пересоздаётся Zenith'овским main.py через sudo), у
    # zenith-voice-bot нет прав писать в него иначе. Тот же паттерн, что
    # уже есть для start_sandbox.sh (тоже требует root).
    write_result = subprocess.run(
        ["sudo", ZENITH_WRITE_CONF_SCRIPT],
        input="".join(kept), capture_output=True, text=True, timeout=10,
    )
    if write_result.returncode != 0:
        return False, f"не удалось записать конфиг: {(write_result.stdout + write_result.stderr).strip()}"

    result = subprocess.run(
        ["sudo", ZENITH_START_SCRIPT],
        capture_output=True, text=True, timeout=15,
    )
    return result.returncode == 0, (result.stdout + result.stderr).strip()


@dataclass
class VoiceTestResult:
    strategy: str
    apply_ok: bool
    apply_output: str
    connect_ok: bool
    connect_error: str = ""
    connect_time_s: float = 0.0
    hold_ok: bool = False
    hold_note: str = ""

    @property
    def success(self) -> bool:
        return self.apply_ok and self.connect_ok and self.hold_ok


async def test_voice_connection(guild: discord.Guild, channel: discord.VoiceChannel) -> tuple[bool, str, float]:
    """Подключается к голосовому каналу, держит соединение HOLD_SECONDS и
    проверяет что оно не отвалилось. Возвращает (ok, note, connect_time_s)."""
    t0 = time.time()
    try:
        vc = await channel.connect(timeout=CONNECT_TIMEOUT, reconnect=False)
    except Exception as e:
        return False, f"connect() не удался: {e}", time.time() - t0

    connect_time = time.time() - t0

    hold_ok = True
    hold_note = f"соединение стабильно {HOLD_SECONDS}s"
    start = time.time()
    try:
        while time.time() - start < HOLD_SECONDS:
            if not vc.is_connected():
                hold_ok = False
                hold_note = f"соединение разорвалось через {time.time() - start:.1f}s"
                break
            await asyncio.sleep(1)
    finally:
        try:
            await vc.disconnect(force=True)
        except Exception:
            pass

    return hold_ok, hold_note, connect_time


async def run_voice_test(strategy_name: str) -> VoiceTestResult:
    strat = state.strategies[strategy_name]

    lua_lines = extract_strategy_lines(ZAPRET_CONFIG_PATH, VOICE_PROFILE, strat.strategy_n)
    if not lua_lines:
        apply_ok, apply_output = False, f"strategy={strat.strategy_n} не найдена в {ZAPRET_CONFIG_PATH} (блок key={VOICE_PROFILE})"
    else:
        loop = asyncio.get_running_loop()
        apply_ok, apply_output = await loop.run_in_executor(None, apply_to_sandbox, lua_lines)

    state.current_strategy = strat.name
    state.last_apply_ok = apply_ok
    state.last_apply_output = apply_output
    state.last_apply_ts = time.time()

    if not apply_ok:
        return VoiceTestResult(
            strategy=strat.name,
            apply_ok=False,
            apply_output=apply_output,
            connect_ok=False,
        )

    guild = bot.get_guild(GUILD_ID)
    channel = guild.get_channel(TEST_VOICE_CHANNEL_ID)
    if channel is None:
        return VoiceTestResult(
            strategy=strat.name,
            apply_ok=True,
            apply_output=apply_output,
            connect_ok=False,
            connect_error=f"Канал с id={TEST_VOICE_CHANNEL_ID} не найден на сервере",
        )

    hold_ok, hold_note, connect_time = await test_voice_connection(guild, channel)

    # если connect() упал — hold_note содержит "connect() не удался"
    connect_failed = hold_note.startswith("connect() не удался")

    return VoiceTestResult(
        strategy=strat.name,
        apply_ok=True,
        apply_output=apply_output,
        connect_ok=not connect_failed,
        connect_error=hold_note if connect_failed else "",
        connect_time_s=connect_time,
        hold_ok=hold_ok and not connect_failed,
        hold_note="" if connect_failed else hold_note,
    )


def result_embed(r: VoiceTestResult) -> discord.Embed:
    color = discord.Color.green() if r.success else discord.Color.red()
    embed = discord.Embed(title=f"Тест стратегии: {r.strategy}", color=color)
    embed.add_field(name="Применение стратегии", value="✅ ok" if r.apply_ok else "❌ fail", inline=True)
    if not r.apply_ok:
        embed.add_field(name="Вывод скрипта", value=f"```{r.apply_output[:900]}```", inline=False)
        return embed

    embed.add_field(name="UDP-подключение", value="✅ ok" if r.connect_ok else "❌ fail", inline=True)
    if r.connect_ok:
        embed.add_field(name="Время подключения", value=f"{r.connect_time_s:.2f}s", inline=True)
        embed.add_field(name="Удержание", value="✅ " + r.hold_note if r.hold_ok else "❌ " + r.hold_note, inline=False)
    else:
        embed.add_field(name="Ошибка", value=r.connect_error or "неизвестно", inline=False)

    embed.add_field(name="ИТОГ", value="✅ РАБОТАЕТ" if r.success else "❌ НЕ РАБОТАЕТ", inline=False)
    return embed


async def notify(result_embed_obj: discord.Embed, invoker: discord.abc.User | None):
    recipient_ids = {OWNER_USER_ID}
    if invoker is not None:
        recipient_ids.add(invoker.id)
    for uid in recipient_ids:
        try:
            user = await bot.fetch_user(uid)
            await user.send(embed=result_embed_obj)
        except discord.Forbidden:
            log.warning("Не могу написать в ЛС пользователю %s (закрыты личные сообщения)", uid)
        except Exception as e:
            log.warning("Ошибка отправки ЛС %s: %s", uid, e)


async def handle_probe(request: web.Request) -> web.Response:
    """POST /probe {"lua_desync_lines": ["--lua-desync=..."]} -- для
    Zenith orchestrator/voice_tester.py. Тот же путь, что и слэш-команды
    (apply_to_sandbox + test_voice_connection), только геном приходит
    готовым от вызывающего, а не берётся из /opt/zapret2/config по
    номеру -- Zenith тестирует ещё не существующие там геномы.

    Также принимает {"strategy_n": N} (добавлено для autotune_daemon.sh
    в z2r_autobench, health-check/ретюн профиля 6 -- см. rank_voice.sh /
    check_profile_voice()) -- то же самое, что вводит номер в слэш-команде
    /voice_test, просто без Discord-взаимодействия: сам достаёт lua-строки
    ИМЕННО ЭТОЙ стратегии из живого /opt/zapret2/config через
    extract_strategy_lines(), тем же путём, что /voice_test. lua_desync_lines
    и strategy_n взаимоисключающие -- ровно один из двух должен быть задан."""
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"success": False, "connect_ms": 0, "note": "invalid JSON body"}, status=400)

    lua_lines = body.get("lua_desync_lines")
    strategy_n = body.get("strategy_n")

    if strategy_n is not None:
        if lua_lines:
            return web.json_response({"success": False, "connect_ms": 0, "note": "lua_desync_lines и strategy_n взаимоисключающие"}, status=400)
        try:
            strategy_n = int(strategy_n)
        except (TypeError, ValueError):
            return web.json_response({"success": False, "connect_ms": 0, "note": "strategy_n должен быть целым числом"}, status=400)
        lua_lines = extract_strategy_lines(ZAPRET_CONFIG_PATH, VOICE_PROFILE, strategy_n)
        if not lua_lines:
            return web.json_response({"success": False, "connect_ms": 0, "note": f"strategy={strategy_n} не найдена в {ZAPRET_CONFIG_PATH} (блок key={VOICE_PROFILE})"})

    if not lua_lines or not isinstance(lua_lines, list):
        return web.json_response({"success": False, "connect_ms": 0, "note": "missing lua_desync_lines (list) or strategy_n"}, status=400)

    loop = asyncio.get_running_loop()
    apply_ok, apply_output = await loop.run_in_executor(None, apply_to_sandbox, lua_lines)
    if not apply_ok:
        return web.json_response({"success": False, "connect_ms": 0, "note": f"apply failed: {apply_output}"})

    guild = bot.get_guild(GUILD_ID)
    if guild is None:
        return web.json_response({"success": False, "connect_ms": 0, "note": "guild not cached yet, бот ещё стартует"})
    channel = guild.get_channel(TEST_VOICE_CHANNEL_ID)
    if channel is None:
        return web.json_response({"success": False, "connect_ms": 0, "note": f"канал {TEST_VOICE_CHANNEL_ID} не найден"})

    hold_ok, hold_note, connect_time = await test_voice_connection(guild, channel)
    connect_failed = hold_note.startswith("connect() не удался")
    return web.json_response({
        "success": hold_ok and not connect_failed,
        "connect_ms": int(connect_time * 1000),
        "note": hold_note,
    })


async def start_probe_server():
    app = web.Application()
    app.router.add_post("/probe", handle_probe)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, ZENITH_PROBE_HOST, ZENITH_PROBE_PORT)
    await site.start()
    log.info("Zenith probe HTTP слушает %s:%d (POST /probe)", ZENITH_PROBE_HOST, ZENITH_PROBE_PORT)


# hours= требует положительное число даже когда цикл выключен
# (AUTO_CHECK_INTERVAL_HOURS<=0) -- в этом случае подставляем заглушку и
# просто никогда не вызываем .start() в setup_hook(), см. ниже.
_AUTO_CHECK_LOOP_HOURS = AUTO_CHECK_INTERVAL_HOURS if AUTO_CHECK_INTERVAL_HOURS > 0 else 1.0


@tasks.loop(hours=_AUTO_CHECK_LOOP_HOURS)
async def auto_voice_check():
    """Тот же прогон, что /voice_rank, только по расписанию -- см.
    AUTO_CHECK_INTERVAL_HOURS/AUTO_CHECK_PASSES. Отчёт уходит владельцу в
    ЛС тем же notify(), что и у ручных команд (invoker=None -- слать
    больше некому, команду никто не вызывал)."""
    if not await ensure_strategies_loaded():
        log.warning("Автопроверка VOICE_UDP пропущена: список стратегий пуст (%s)", state.strategies_error)
        return
    log.info("Автопроверка VOICE_UDP: запускаю %d проход(ов)...", AUTO_CHECK_PASSES)
    async with _check_lock:
        await run_check_sweep(AUTO_CHECK_PASSES)
    summary = build_check_summary(
        AUTO_CHECK_PASSES,
        title=f"🔁 Автопроверка VOICE_UDP — {AUTO_CHECK_PASSES} проход(ов), раз в {AUTO_CHECK_INTERVAL_HOURS:g}ч",
    )
    await notify(summary, None)
    log.info("Автопроверка VOICE_UDP завершена")


@auto_voice_check.before_loop
async def before_auto_voice_check():
    await bot.wait_until_ready()


class ZapretBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.voice_states = True
        intents.guilds = True
        super().__init__(command_prefix="!", intents=intents)

    async def setup_hook(self):
        state.strategies, state.strategies_error = await build_strategies()
        log.info("Загружено стратегий: %d (динамически, из z2r config)", len(state.strategies))
        if not state.strategies:
            log.error("Бот стартует с ПУСТЫМ списком стратегий VOICE_UDP: %s", state.strategies_error)
        guild = discord.Object(id=GUILD_ID)
        self.tree.copy_global_to(guild=guild)
        try:
            await self.tree.sync(guild=guild)
            log.info("Slash-команды синхронизированы для guild %s", GUILD_ID)
        except discord.HTTPException as e:
            # Не роняем весь процесс из-за slash-команд -- HTTP /probe для
            # Zenith и voice-тесты им не пользуются вообще. Частая причина
            # 403 (Missing Access) -- бот приглашён без scope
            # applications.commands, нужен новый инвайт с обоими scope.
            log.error("Не удалось синхронизировать slash-команды (не критично для Zenith): %s", e)
        await start_probe_server()
        if AUTO_CHECK_INTERVAL_HOURS > 0:
            auto_voice_check.start()
            log.info(
                "Автопроверка VOICE_UDP включена: раз в %gч, %d проход(ов)",
                AUTO_CHECK_INTERVAL_HOURS, AUTO_CHECK_PASSES,
            )
        else:
            log.info("Автопроверка VOICE_UDP отключена (AUTO_CHECK_INTERVAL_HOURS<=0)")


bot = ZapretBot()


def strategy_choices() -> list[app_commands.Choice[str]]:
    return [
        app_commands.Choice(name=f"{s.name} — {s.description}"[:100], value=s.name)
        for s in state.strategies.values()
    ][:25]


@bot.tree.command(description="Показать текущий список стратегий VOICE_UDP")
async def strategy_list(interaction: discord.Interaction):
    if not await ensure_strategies_loaded():
        await interaction.response.send_message(
            f"Стратегии не загружены: {state.strategies_error or 'см. логи'}\n"
            f"Попытка автоматически перечитать тоже не удалась -- проверь sudo-права "
            f"{SET_STRATEGY_CLI} и доступность zapret2, либо запусти /refresh_strategies вручную.",
            ephemeral=True,
        )
        return
    lines = []
    for s in state.strategies.values():
        marker = " ⬅️ текущая" if s.name == state.current_strategy else ""
        lines.append(f"**{s.name}**{marker} — {s.description}")
    embed = discord.Embed(title="Стратегии VOICE_UDP (z2r, актуально)", description="\n".join(lines), color=discord.Color.blurple())
    await interaction.response.send_message(embed=embed)


@bot.tree.command(description="Перечитать число стратегий у z2r заново (если менял конфиг, пока бот работает)")
async def refresh_strategies(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    old_count = len(state.strategies)
    state.strategies, state.strategies_error = await build_strategies()
    new_count = len(state.strategies)
    msg = f"Обновлено: было {old_count} стратегий, стало {new_count}."
    if new_count == 0:
        msg += f"\nПричина: {state.strategies_error or 'см. логи бота'}"
    await interaction.followup.send(msg, ephemeral=True)


@bot.tree.command(description="Статус последней применённой стратегии")
async def strategy_status(interaction: discord.Interaction):
    if state.current_strategy is None:
        await interaction.response.send_message("Стратегия ещё ни разу не применялась.", ephemeral=True)
        return
    ago = int(time.time() - state.last_apply_ts) if state.last_apply_ts else None
    embed = discord.Embed(
        title="Текущий статус",
        color=discord.Color.green() if state.last_apply_ok else discord.Color.red(),
    )
    embed.add_field(name="Стратегия", value=state.current_strategy, inline=True)
    embed.add_field(name="Результат применения", value="✅ ok" if state.last_apply_ok else "❌ fail", inline=True)
    if ago is not None:
        embed.add_field(name="Применена", value=f"{ago} сек назад", inline=True)
    await interaction.response.send_message(embed=embed)


@bot.tree.command(description="Применить стратегию, зайти в тестовый канал и написать результат в ЛС")
@app_commands.describe(name="Название стратегии из strategy_list")
async def voice_test(interaction: discord.Interaction, name: str):
    if not state.strategies:
        await ensure_strategies_loaded()
    if name not in state.strategies:
        await interaction.response.send_message(f"Нет такой стратегии: `{name}`. Смотри /strategy_list", ephemeral=True)
        return

    await interaction.response.send_message(
        f"Тестирую стратегию **{name}**: применяю конфиг, подключаюсь к голосовому каналу, "
        f"держу соединение {HOLD_SECONDS}s...", ephemeral=True
    )
    async with _check_lock:
        result = await run_voice_test(name)
    log_voice_result(pass_num=1, strategy=name, attempt=1, result=result)
    embed = result_embed(result)
    await notify(embed, interaction.user)
    await interaction.followup.send("Готово, результат отправлен в ЛС.", ephemeral=True)


@voice_test.autocomplete("name")
async def voice_test_autocomplete(interaction: discord.Interaction, current: str):
    return [c for c in strategy_choices() if current.lower() in c.value.lower()]


@bot.tree.command(description="Прогнать ВСЕ стратегии по очереди (1 раз) и прислать сводку в ЛС")
async def voice_test_all(interaction: discord.Interaction):
    if not await ensure_strategies_loaded():
        await interaction.response.send_message(
            f"Стратегии не загружены: {state.strategies_error or 'см. логи'}\n"
            f"Попытка автоматически перечитать тоже не удалась -- проверь sudo-права "
            f"{SET_STRATEGY_CLI} и доступность zapret2, либо запусти /refresh_strategies вручную.",
            ephemeral=True,
        )
        return

    await interaction.response.send_message(
        f"Запускаю прогон всех стратегий ({len(state.strategies)} шт.), "
        f"пауза между ними {STRATEGY_SWITCH_DELAY}s. Отпишусь в ЛС по каждой и общей сводкой в конце.",
        ephemeral=True,
    )

    results: list[VoiceTestResult] = []
    names = list(state.strategies.keys())
    async with _check_lock:
        for i, name in enumerate(names):
            result = await run_voice_test(name)
            log_voice_result(pass_num=1, strategy=name, attempt=1, result=result)
            results.append(result)
            await notify(result_embed(result), interaction.user)
            if i < len(names) - 1:
                await asyncio.sleep(STRATEGY_SWITCH_DELAY)

    summary_lines = []
    for r in results:
        mark = "✅" if r.success else "❌"
        summary_lines.append(f"{mark} **{r.strategy}**" + (f" — {r.connect_time_s:.2f}s" if r.success else ""))

    working = [r.strategy for r in results if r.success]
    summary = discord.Embed(
        title="Сводка по всем стратегиям",
        description="\n".join(summary_lines),
        color=discord.Color.green() if working else discord.Color.red(),
    )
    summary.add_field(
        name="Рабочие стратегии",
        value=", ".join(working) if working else "ни одна не сработала",
        inline=False,
    )
    await notify(summary, interaction.user)
    await interaction.followup.send("Прогон завершён, сводка в ЛС.", ephemeral=True)


async def run_check_sweep(passes: int, names: list[str] | None = None) -> None:
    """Гоняет все стратегии VOICE_UDP passes раз подряд, логируя каждый
    результат через log_voice_result() -- общее тело для /voice_rank и
    автоматического периодического цикла (auto_voice_check). Вызывающий
    обязан держать _check_lock снаружи (обе точки вызова это делают)."""
    strategy_names = names if names is not None else list(state.strategies.keys())
    for p in range(1, passes + 1):
        for i, name in enumerate(strategy_names):
            result = await run_voice_test(name)
            log_voice_result(pass_num=p, strategy=name, attempt=1, result=result)
            if not (i == len(strategy_names) - 1 and p == passes):
                await asyncio.sleep(STRATEGY_SWITCH_DELAY)
        log.info("check sweep: проход %d/%d завершён", p, passes)


def build_check_summary(passes: int, title: str) -> discord.Embed:
    """Агрегирует VOICE_RAW_FILE за последние `passes` проходов в
    рейтинг-эмбед -- общее тело для /voice_rank и auto_voice_check.
    Не полагаемся на внешний awk, чтобы результат можно было сразу
    прислать в Discord."""
    stats: dict[str, dict] = {}
    try:
        with open(VOICE_RAW_FILE, "r", encoding="utf-8") as f:
            next(f)  # header
            for line in f:
                parts = line.rstrip("\n").split("\t")
                if len(parts) < 7:
                    continue
                _ts, ln_pass, strat, _attempt, success, connect_ms, _note = parts[:7]
                if int(ln_pass) > passes:
                    continue  # старые записи с прошлых запусков бота, не считаем
                st = stats.setdefault(strat, {"total": 0, "success": 0, "sum_ms": 0})
                st["total"] += 1
                if success == "1":
                    st["success"] += 1
                    st["sum_ms"] += int(connect_ms)
    except FileNotFoundError:
        pass

    ranked = []
    for strat, st in stats.items():
        if st["success"] > 0:
            rate = st["success"] / st["total"]
            avg_ms = st["sum_ms"] / st["success"]
            ranked.append((strat, rate, avg_ms, st["success"], st["total"]))
    # сортировка: надёжность убыв., затем время подключения возр. (быстрее = лучше)
    ranked.sort(key=lambda x: (-x[1], x[2]))

    lines = []
    for strat, rate, avg_ms, succ, tot in ranked[:25]:
        lines.append(f"**{strat}**: {succ}/{tot} ({rate*100:.0f}%), ~{avg_ms:.0f}ms")
    failed = [s for s, st in stats.items() if st["success"] == 0]

    summary = discord.Embed(
        title=title,
        description="\n".join(lines) if lines else "Ни одна стратегия не сработала ни разу.",
        color=discord.Color.green() if lines else discord.Color.red(),
    )
    if failed:
        summary.add_field(name="Провалились полностью", value=", ".join(failed)[:1000], inline=False)
    summary.add_field(name="Полный лог", value=f"`{VOICE_RAW_FILE}`", inline=False)
    return summary


@bot.tree.command(description="Многопроходный прогон ВСЕХ стратегий (аналог rank_strategies.sh --passes)")
@app_commands.describe(passes="Сколько раз прогнать полный набор стратегий (по умолчанию 3)")
async def voice_rank(interaction: discord.Interaction, passes: int = 3):
    if not await ensure_strategies_loaded():
        await interaction.response.send_message(
            f"Стратегии не загружены: {state.strategies_error or 'см. логи'}\n"
            f"Попытка автоматически перечитать тоже не удалась -- проверь sudo-права "
            f"{SET_STRATEGY_CLI} и доступность zapret2, либо запусти /refresh_strategies вручную.",
            ephemeral=True,
        )
        return
    if passes < 1 or passes > 10:
        await interaction.response.send_message("passes должен быть от 1 до 10 (иначе тест займёт слишком много времени).", ephemeral=True)
        return

    names = list(state.strategies.keys())
    total = len(names) * passes
    await interaction.response.send_message(
        f"Запускаю {passes} проход(ов) по {len(names)} стратегиям ({total} тестов, "
        f"по ~{HOLD_SECONDS + STRATEGY_SWITCH_DELAY}s каждый — это надолго). "
        f"Промежуточные результаты НЕ шлю в ЛС (иначе будет спам), только финальную сводку "
        f"и полный TSV-лог: {VOICE_RAW_FILE}",
        ephemeral=True,
    )

    async with _check_lock:
        await run_check_sweep(passes, names)

    summary = build_check_summary(passes, title=f"Рейтинг VOICE_UDP — {passes} проход(ов)")
    await notify(summary, interaction.user)
    await interaction.followup.send("Прогон завершён, рейтинг отправлен в ЛС.", ephemeral=True)


@bot.tree.command(description="Аварийно вывести бота из голосового канала")
async def voice_leave(interaction: discord.Interaction):
    guild = interaction.guild
    if guild.voice_client is None:
        await interaction.response.send_message("Бот сейчас не в войсе.", ephemeral=True)
        return
    await guild.voice_client.disconnect(force=True)
    await interaction.response.send_message("Бот вышел из войса.")


@bot.event
async def on_ready():
    log.info("Бот запущен как %s (id=%s)", bot.user, bot.user.id)
    if not state.strategies and not state.startup_alert_sent:
        state.startup_alert_sent = True
        embed = discord.Embed(
            title="⚠️ Стратегии VOICE_UDP не загружены при старте",
            description=state.strategies_error or "неизвестная причина, см. логи бота",
            color=discord.Color.red(),
        )
        await notify(embed, None)


if __name__ == "__main__":
    bot.run(DISCORD_TOKEN)
