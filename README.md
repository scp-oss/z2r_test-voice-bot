# zapret-voice-bot

Discord-бот для проверки рабочих стратегий zapret2 (`DS_TLS` / `VOICE_UDP`)
на голосовом канале Discord.

## Как это работает

`/voice_test <strategy>`:
1. Запускает твой скрипт переключения стратегии (`apply_cmd` из конфига)
2. Бот заходит в фиксированный тестовый голосовой канал — это реальное
   UDP-подключение к voice-серверу Discord (websocket handshake + IP
   discovery по UDP), не имитация
3. Держит соединение `HOLD_SECONDS` секунд, проверяя что оно не отвалилось
4. Отключается и присылает результат в ЛС — и владельцу (`OWNER_USER_ID`),
   и тому, кто запустил команду

`/voice_test_all` — то же самое по очереди для всех стратегий из
`config/strategies.yaml`, с паузой `STRATEGY_SWITCH_DELAY` между ними,
в конце — сводный отчёт в ЛС (какие стратегии рабочие, время подключения).

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

2. `cp config/strategies.example.yaml config/strategies.yaml` и впиши
   реальные команды твоего скрипта переключения стратегий в `apply_cmd`.

3. Чтобы бот мог писать тебе в ЛС — у тебя должны быть открыты личные
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
