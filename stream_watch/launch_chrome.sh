#!/bin/bash
# Отдельный Chrome для наблюдения за стримом Discord (копия профиля тестового аккаунта), CDP :9333, экран :99
pkill -f 'discord-watch/chrome-prof' 2>/dev/null; sleep 1
DISPLAY=:99 nohup /opt/google/chrome/chrome --user-data-dir=/opt/z2r_autobench/z2r_test-voice-bot/stream_watch/state/chrome-prof \
  --no-first-run --no-default-browser-check --no-sandbox --use-gl=swiftshader --disable-gpu-sandbox \
  --remote-debugging-port=9333 --remote-debugging-address=127.0.0.1 --window-size=1600,1000 --window-position=200,40 \
  --use-fake-ui-for-media-stream --use-fake-device-for-media-stream --autoplay-policy=no-user-gesture-required \
  --disable-quic --lang=ru "https://discord.com/channels/${GUILD_ID:-@me}" > /opt/z2r_autobench/z2r_test-voice-bot/stream_watch/state/chrome.log 2>&1 &
