"""
Discord-бот для тестирования стратегий zapret2 (VOICE_UDP, профиль 6).

Стратегии НЕ хранятся в статическом yaml-файле — при каждом старте (и по
команде /refresh_strategies) бот спрашивает актуальное число стратегий
напрямую у z2r через set_strategy_cli.sh (та же config_profile_max_strategy(),
которой пользуются rank_strategies.sh/rank_quic.sh). Это исключает
рассинхронизацию после обновления стратегий в самом z2r (пункт 5 меню и т.п.)
— не нужно ничего перегенерировать вручную.

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
"""

import asyncio
import logging
import os
import time
from dataclasses import dataclass, field

import discord
from discord import app_commands
from discord.ext import commands
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
    apply_cmd: str


@dataclass
class BotState:
    strategies: dict[str, Strategy] = field(default_factory=dict)
    current_strategy: str | None = None
    last_apply_ok: bool | None = None
    last_apply_output: str = ""
    last_apply_ts: float | None = None


state = BotState()


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


async def query_max_strategy(profile: str) -> int | None:
    """Спрашивает у z2r (через set_strategy_cli.sh max) актуальное число
    стратегий профиля — ту же config_profile_max_strategy(), которой
    пользуется остальная автоматика (rank_strategies.sh и т.п.). Так список
    стратегий бота никогда не рассинхронизируется со статическим файлом,
    потому что статического файла со списком стратегий просто нет."""
    proc = await asyncio.create_subprocess_exec(
        "sudo", "bash", SET_STRATEGY_CLI, "max", profile,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        log.error("Не удалось получить max_strategy для profile=%s: %s", profile, stderr.decode(errors="replace"))
        return None
    text = stdout.decode(errors="replace").strip()
    if not text.isdigit():
        log.error("Неожиданный вывод max_strategy для profile=%s: %r", profile, text)
        return None
    return int(text)


async def build_strategies() -> dict[str, Strategy]:
    """Строит список стратегий на лету по актуальному числу от z2r."""
    max_voice = await query_max_strategy(VOICE_PROFILE)
    if max_voice is None:
        log.warning("Не удалось определить число стратегий VOICE_UDP, список будет пуст.")
        return {}
    result: dict[str, Strategy] = {}
    for s in range(1, max_voice + 1):
        name = f"voice_{s}"
        result[name] = Strategy(
            name=name,
            description=f"VOICE_UDP={s}",
            apply_cmd=f"sudo bash {SET_STRATEGY_CLI} set {VOICE_PROFILE} {VOICE_PROTO} {s}",
        )
    return result


async def run_apply_cmd(cmd: str) -> tuple[bool, str]:
    """Асинхронно выполняет команду переключения стратегии, не блокируя event loop."""
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    stdout, _ = await proc.communicate()
    output = stdout.decode(errors="replace").strip()
    ok = proc.returncode == 0
    return ok, output


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

    apply_ok, apply_output = await run_apply_cmd(strat.apply_cmd)
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


class ZapretBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.voice_states = True
        intents.guilds = True
        super().__init__(command_prefix="!", intents=intents)

    async def setup_hook(self):
        state.strategies = await build_strategies()
        log.info("Загружено стратегий: %d (динамически, из z2r config)", len(state.strategies))
        guild = discord.Object(id=GUILD_ID)
        self.tree.copy_global_to(guild=guild)
        await self.tree.sync(guild=guild)
        log.info("Slash-команды синхронизированы для guild %s", GUILD_ID)


bot = ZapretBot()


def strategy_choices() -> list[app_commands.Choice[str]]:
    return [
        app_commands.Choice(name=f"{s.name} — {s.description}"[:100], value=s.name)
        for s in state.strategies.values()
    ][:25]


@bot.tree.command(description="Показать текущий список стратегий VOICE_UDP")
async def strategy_list(interaction: discord.Interaction):
    if not state.strategies:
        await interaction.response.send_message(
            "Стратегии не загружены — проверь sudo-права set_strategy_cli.sh и доступность zapret2.", ephemeral=True
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
    state.strategies = await build_strategies()
    new_count = len(state.strategies)
    await interaction.followup.send(
        f"Обновлено: было {old_count} стратегий, стало {new_count}.", ephemeral=True
    )


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
    if name not in state.strategies:
        await interaction.response.send_message(f"Нет такой стратегии: `{name}`. Смотри /strategy_list", ephemeral=True)
        return

    await interaction.response.send_message(
        f"Тестирую стратегию **{name}**: применяю конфиг, подключаюсь к голосовому каналу, "
        f"держу соединение {HOLD_SECONDS}s...", ephemeral=True
    )
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
    if not state.strategies:
        await interaction.response.send_message("Стратегии не загружены. Попробуй /refresh_strategies.", ephemeral=True)
        return

    await interaction.response.send_message(
        f"Запускаю прогон всех стратегий ({len(state.strategies)} шт.), "
        f"пауза между ними {STRATEGY_SWITCH_DELAY}s. Отпишусь в ЛС по каждой и общей сводкой в конце.",
        ephemeral=True,
    )

    results: list[VoiceTestResult] = []
    names = list(state.strategies.keys())
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


@bot.tree.command(description="Многопроходный прогон ВСЕХ стратегий (аналог rank_strategies.sh --passes)")
@app_commands.describe(passes="Сколько раз прогнать полный набор стратегий (по умолчанию 3)")
async def voice_rank(interaction: discord.Interaction, passes: int = 3):
    if not state.strategies:
        await interaction.response.send_message("Стратегии не загружены. Попробуй /refresh_strategies.", ephemeral=True)
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

    for p in range(1, passes + 1):
        for i, name in enumerate(names):
            result = await run_voice_test(name)
            log_voice_result(pass_num=p, strategy=name, attempt=1, result=result)
            if not (i == len(names) - 1 and p == passes):
                await asyncio.sleep(STRATEGY_SWITCH_DELAY)
        log.info("voice_rank: проход %d/%d завершён", p, passes)

    # Агрегация прямо в Python — не полагаемся на внешний awk, чтобы
    # результат можно было сразу прислать в Discord.
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
        title=f"Рейтинг VOICE_UDP — {passes} проход(ов)",
        description="\n".join(lines) if lines else "Ни одна стратегия не сработала ни разу.",
        color=discord.Color.green() if lines else discord.Color.red(),
    )
    if failed:
        summary.add_field(name="Провалились полностью", value=", ".join(failed)[:1000], inline=False)
    summary.add_field(name="Полный лог", value=f"`{VOICE_RAW_FILE}`", inline=False)
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


if __name__ == "__main__":
    bot.run(DISCORD_TOKEN)
