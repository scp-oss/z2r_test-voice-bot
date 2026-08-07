# z2r_test-voice-bot

Discord-бот для проверки стратегий zapret2 (`VOICE_UDP`, профиль 6) на
голосовом канале Discord — и тестовый клиент Zenith для того же профиля
(см. "Интеграция с Zenith" ниже), один бот на обе задачи.

## Как это работает

Список стратегий строится на лету — при каждом старте (и по
`/refresh_strategies`) бот спрашивает у z2r актуальное число стратегий
профиля 6 через `set_strategy_cli.sh max` (только чтение, `locked.tsv` не
трогает). Статического yaml-файла со списком нет — не рассинхронизируется
после правок конфига z2r.

`/voice_test <strategy>`:
1. Вытаскивает реальные `--lua-desync=...:strategy=N` строки нужного
   номера прямо из `/opt/zapret2/config` и применяет их в **песочнице
   Zenith** (изолированный `nfqws2`, не боевой) — см. "Песочница, не
   прод" ниже.
2. Бот заходит в фиксированный тестовый голосовой канал через эту
   песочницу — это реальное UDP-подключение к voice-серверу Discord
   (websocket handshake + IP discovery по UDP), не имитация.
3. Держит соединение `HOLD_SECONDS` секунд, проверяя что оно не отвалилось.
4. Отключается и присылает результат в ЛС — и владельцу (`OWNER_USER_ID`),
   и тому, кто запустил команду.

`/voice_test_all` — то же самое по очереди для всех стратегий, с паузой
`STRATEGY_SWITCH_DELAY` между ними, в конце — сводный отчёт в ЛС (какие
стратегии рабочие, время подключения). `/voice_rank <passes>` — то же
самое в несколько проходов, с рейтингом по надёжности+скорости.

## Песочница, не прод

Раньше `/voice_test` переключал боевой `locked.tsv`
(`set_strategy_cli.sh set`) — стратегия реально становилась активной для
всех пользователей на время теста. Теперь — только изолированная
песочница [Zenith](https://github.com/scp-oss/Zenith), боевой
`/opt/zapret2` бот вообще не трогает. Это же даёт Zenith возможность
гонять через этот бот СВОИ сгенерированные (ещё не существующие в
конфиге z2r) геномы, не заводя отдельного токена — см. ниже.

Отсюда требование: бот должен работать от системного юзера
`zenith-voice-bot`, СОЗДАВАЕМОГО `Zenith/sandbox/setup_sandbox.sh` — не
от `zenith-sandbox` (тот у Zenith занят curl-проверками TCP-профилей).
Живой инцидент 2026-08-07: у бота есть свой обычный TCP-трафик
(Discord gateway/API логин), не связанный с тестированием, и он ЗАВИСАЛ,
будучи пойман тем же широким TCP-правилом `zenith-sandbox` — `nfqws2`
пытается реассемблировать TLS ClientHello даже без единого
`--filter-tcp=` в конфиге, и в этом no-op режиме реассемблинг с replay
иногда дропает исходный пакет вместо чистого пропуска. У
`zenith-voice-bot` — узкое правило ТОЛЬКО на конкретные UDP-порты
голосового профиля, обычный HTTPS-трафик бота его вообще не касается.
И ему нужны права перезапускать `Zenith/sandbox/start_sandbox.sh` (сам
скрипт требует root):

```bash
# создаёт юзеров zenith-sandbox И zenith-voice-bot, ставит оба правила
sudo /opt/z2r_autobench/Zenith/sandbox/setup_sandbox.sh

# systemd-юнит бота — на юзера zenith-voice-bot
sudo systemctl edit z2r-test-voice-bot   # добавить в [Service]: User=zenith-voice-bot

# узкий sudoers, только на этот скрипт
echo 'zenith-voice-bot ALL=(root) NOPASSWD: /opt/z2r_autobench/Zenith/sandbox/start_sandbox.sh' \
  | sudo tee /etc/sudoers.d/zenith-voice-bot

sudo systemctl restart z2r-test-voice-bot
```

## Интеграция с Zenith

Zenith (`orchestrator/voice_tester.py`) дёргает этот же бот через
локальный HTTP (`POST http://127.0.0.1:8765/probe`,
`{"lua_desync_lines": ["--lua-desync=..."]}`) — не создаёт свой Discord-
токен/приложение. Порт настраивается `ZENITH_PROBE_HOST`/
`ZENITH_PROBE_PORT` в `.env`, слушает только `127.0.0.1` по умолчанию,
наружу не выставлять.

## Установка

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pip install "discord.py[voice]"   # для голосовых соединений
```

## Настройка

1. `cp .env.example .env` и заполни:
   - `DISCORD_TOKEN` — токен бота
   - `GUILD_ID` — ID твоего сервера
   - `TEST_VOICE_CHANNEL_ID` — ID уже существующего голосового канала
     (создай его сам один раз в Discord, канал не пересоздаётся ботом)
   - `OWNER_USER_ID` — твой Discord user ID, куда слать ЛС
   - `HOLD_SECONDS`, `CONNECT_TIMEOUT`, `STRATEGY_SWITCH_DELAY` — при
     желании подстрой под себя (дефолты разумные)
   - `ZAPRET_CONFIG_PATH`/`ZENITH_SANDBOX_DIR`/`ZENITH_PROBE_HOST`/
     `ZENITH_PROBE_PORT` — обычно дефолты подходят, менять только если
     Zenith установлен не рядом со стандартным путём.

2. Чтобы бот мог писать тебе в ЛС — у тебя должны быть открыты личные
   сообщения от участников этого сервера (Settings -> Privacy -> Allow
   direct messages from server members), иначе Discord это заблокирует.

## Права бота при инвайте (OAuth2 URL Generator)

Scopes: `bot`, `applications.commands`
Bot Permissions: `View Channels`, `Connect`, `Move Members` (на будущее)

Manage Channels **не нужен** — канал фиксированный, бот его не создаёт
и не удаляет.

## Запуск

```bash
python3 bot.py
```

## Команды

| Команда | Что делает |
|---|---|
| `/strategy_list` | Список стратегий из конфига |
| `/strategy_status` | Какая стратегия применена последней |
| `/voice_test name:<...>` | Полный цикл: apply -> connect -> hold -> ЛС |
| `/voice_test_all` | Прогон всех стратегий по очереди + сводка в ЛС |
| `/voice_leave` | Аварийно вывести бота из войса вручную |

## Про безопасность аккаунта

Бот работает через официальный Bot Token (не self-bot), поэтому банить
тут нечего — это штатный способ работы с Discord API. Единственное на
что стоит обратить внимание: не гонять `/voice_test_all` слишком часто
подряд без пауз (сейчас между стратегиями и так есть задержка
`STRATEGY_SWITCH_DELAY`) — это просто хорошая практика, а не защита от
бана.
