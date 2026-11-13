#!/usr/bin/env python3
"""HTTP-сервис наблюдения Discord Go Live через реальный браузер.

Боты Discord не могут смотреть стримы (STREAM_WATCH op 20 для бот-токена ->
close 4001, проверено 2026-11-13). Поэтому «зритель» -- обычный тестовый
аккаунт в Chrome (state/chrome-prof, экран :99, CDP 127.0.0.1:9333), а бот
только видит self_stream и дёргает этот сервис.

POST /watch {"channel_id": "...", "user_id": "...", "seconds": 60}
  -> {"verdict": "OK|LOADING|GRAY|FROZEN|NO_STREAM|ERROR", ...метрики}
Один просмотр одновременно (лок); запуск от root (systemd discord-stream-watch).
"""
import json, os, subprocess, threading, time, urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from playwright.sync_api import sync_playwright

HERE = os.path.dirname(os.path.abspath(__file__))
CDP = os.environ.get("STREAM_WATCH_CDP", "http://127.0.0.1:9333")
PORT = int(os.environ.get("STREAM_WATCH_PORT", "8770"))
GUILD_ID = os.environ.get("GUILD_ID", "")
LOCK = threading.Lock()

SAMPLE_JS = """() => {
  const vs = [...document.querySelectorAll('video')].filter(v => v.videoWidth > 0);
  vs.sort((a, b) => { const ra = a.getBoundingClientRect(), rb = b.getBoundingClientRect(); return rb.width*rb.height - ra.width*ra.height; });
  const v = vs[0]; if (!v) return null;
  const q = v.getVideoPlaybackQuality ? v.getVideoPlaybackQuality() : {};
  let sd = null, fp = null;
  try { const c = document.createElement('canvas'); c.width = 32; c.height = 18; const x = c.getContext('2d');
    x.drawImage(v, 0, 0, 32, 18); const d = x.getImageData(0, 0, 32, 18).data; let s = 0, s2 = 0, n = 0; fp = [];
    for (let i = 0; i < d.length; i += 4) { const y = 0.299*d[i] + 0.587*d[i+1] + 0.114*d[i+2]; s += y; s2 += y*y; n++; fp.push(Math.round(y)); }
    sd = Math.sqrt(Math.max(0, s2/n - (s/n)**2)); } catch (e) {}
  return {w: v.videoWidth, h: v.videoHeight, frames: q.totalVideoFrames || 0, dropped: q.droppedVideoFrames || 0, t: v.currentTime, sd, fp};
}"""

def cdp_alive():
    try: urllib.request.urlopen(CDP + "/json/version", timeout=2); return True
    except Exception: return False

def ensure_chrome():
    if cdp_alive(): return
    subprocess.run(["bash", os.path.join(HERE, "launch_chrome.sh")], env=dict(os.environ, GUILD_ID=GUILD_ID), timeout=30)
    for _ in range(30):
        if cdp_alive(): return
        time.sleep(1)
    raise RuntimeError("Chrome (CDP) не поднялся")

def watch(channel_id, user_id, seconds):
    ensure_chrome()
    with sync_playwright() as pw:
        b = pw.chromium.connect_over_cdp(CDP)
        pages = [p for c in b.contexts for p in c.pages if "discord.com" in p.url]
        p = pages[0] if pages else b.contexts[0].new_page()
        if channel_id and GUILD_ID:
            p.goto(f"https://discord.com/channels/{GUILD_ID}/{channel_id}", wait_until="domcontentloaded", timeout=30000)
        p.bring_to_front(); time.sleep(4)
        live = None
        for _ in range(10):
            loc = p.locator("text=/в эфире|live/i")
            if loc.count(): live = loc.first; break
            time.sleep(2)
        if not live:
            return {"verdict": "NO_STREAM", "note": "значок «В эфире» не найден"}
        live.click(); time.sleep(4)
        for label in ("Смотреть стрим", "Смотреть трансляцию", "Watch Stream", "Присоединиться к стриму"):
            btn = p.get_by_role("button", name=label)
            if btn.count():
                try: btn.first.click(timeout=3000); time.sleep(2); break
                except Exception: pass
        samples, t0 = [], time.time()
        while time.time() - t0 < seconds:
            samples.append(p.evaluate(SAMPLE_JS)); time.sleep(2)
        shot = os.path.join(HERE, "state", "last_watch.png")
        p.screenshot(path=shot)
    vs = [s for s in samples if s]
    res = {"samples": len(samples), "with_video": len(vs), "seconds": seconds, "screenshot": shot}
    if len(vs) < 2 or vs[-1]["frames"] <= vs[0]["frames"] + 5:
        res["verdict"] = "LOADING"; return res
    fps = (vs[-1]["frames"] - vs[0]["frames"]) / max(1, 2 * (len(vs) - 1))
    flat = sum(1 for s in vs if s["sd"] is not None and s["sd"] < 3)
    changes = sum(1 for a, b2 in zip(vs, vs[1:]) if a["fp"] and b2["fp"] and sum(abs(x - y) for x, y in zip(a["fp"], b2["fp"])) / len(a["fp"]) > 1.0)
    res.update(fps=round(fps, 1), resolution=f"{vs[-1]['w']}x{vs[-1]['h']}", dropped=vs[-1]["dropped"] - vs[0]["dropped"],
               flat_samples=flat, picture_changes=changes)
    if flat > len(vs) * 0.7: res["verdict"] = "GRAY"
    elif fps < 1: res["verdict"] = "FROZEN"
    else: res["verdict"] = "OK"
    if res["verdict"] == "OK" and changes == 0:
        res["note"] = "кадры идут, но картинка не менялась (статичный экран — зависание не отличить)"
    return res

class H(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path != "/watch": self.send_error(404); return
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0)) or 0) or b"{}")
        if not LOCK.acquire(blocking=False):
            out, code = {"verdict": "BUSY", "note": "уже идёт просмотр"}, 409
        else:
            try: out, code = watch(str(body.get("channel_id") or ""), str(body.get("user_id") or ""), int(body.get("seconds", 60))), 200
            except Exception as e: out, code = {"verdict": "ERROR", "note": str(e)[:300]}, 500
            finally: LOCK.release()
        data = json.dumps(out, ensure_ascii=False).encode()
        self.send_response(code); self.send_header("Content-Type", "application/json"); self.send_header("Content-Length", str(len(data))); self.end_headers(); self.wfile.write(data)
    def log_message(self, fmt, *a): print("watcher:", fmt % a, flush=True)

if __name__ == "__main__":
    os.makedirs(os.path.join(HERE, "state"), exist_ok=True)
    ThreadingHTTPServer(("127.0.0.1", PORT), H).serve_forever()
