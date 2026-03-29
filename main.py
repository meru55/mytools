import asyncio
import json
import shutil
import tempfile
import threading
import time
import uuid
from pathlib import Path

import yt_dlp
from fastapi import FastAPI, Request, UploadFile, File
from fastapi.responses import HTMLResponse, StreamingResponse, FileResponse
from fastapi.templating import Jinja2Templates

CONFIG_DIR = Path.home() / ".ytdownloader"
CONFIG_DIR.mkdir(parents=True, exist_ok=True)
HISTORY_PATH = CONFIG_DIR / "history.json"
COOKIES_PATH = CONFIG_DIR / "cookies.txt"

TEMP_DIR = Path(tempfile.gettempdir()) / "ytdownloader_web"
TEMP_DIR.mkdir(parents=True, exist_ok=True)

MAX_RETRIES = 3

app = FastAPI()
templates = Jinja2Templates(directory="templates")

# task_id -> {status, progress, logs, last_log_time, filepath, filename, error}
_tasks: dict[str, dict] = {}


# ─── Pages ─────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(request, "index.html")


@app.get("/youtube", response_class=HTMLResponse)
async def youtube_page(request: Request):
    return templates.TemplateResponse(request, "youtube.html")


# ─── YouTube API ────────────────────────────────────

@app.post("/api/youtube/info")
async def youtube_info(request: Request):
    data = await request.json()
    url = data.get("url", "").strip()
    if not url:
        return {"error": "URLが必要です"}
    try:
        ydl_base: dict = {
            "quiet": True,
            "no_warnings": True,
            "extractor_args": {"youtube": {"player_client": ["ios", "tv_embedded", "web"]}},
        }
        if COOKIES_PATH.exists():
            ydl_base["cookiefile"] = str(COOKIES_PATH)
        with yt_dlp.YoutubeDL(ydl_base) as ydl:
            info = ydl.extract_info(url, download=False)

        is_playlist = info.get("_type") == "playlist"
        entries = []
        if is_playlist:
            for e in (info.get("entries") or []):
                if not e:
                    continue
                entry_url = (
                    e.get("webpage_url")
                    or e.get("url")
                    or f"https://www.youtube.com/watch?v={e.get('id', '')}"
                )
                entries.append({"url": entry_url, "title": e.get("title", entry_url)})

        duration = info.get("duration") or 0
        h, m, s = duration // 3600, (duration % 3600) // 60, duration % 60
        dur_str = f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"

        return {
            "title": info.get("title", "不明"),
            "uploader": info.get("uploader", ""),
            "duration_str": dur_str,
            "thumbnail": info.get("thumbnail", ""),
            "is_playlist": is_playlist,
            "playlist_count": len(entries),
            "entries": entries,
        }
    except Exception as e:
        return {"error": str(e)}


@app.post("/api/youtube/download")
async def youtube_download(request: Request):
    data = await request.json()
    url = data.get("url", "").strip()
    fmt = data.get("format", "動画 (mp4)")
    quality = data.get("quality", "1080p (FHD)")
    subtitles = data.get("subtitles", False)
    sub_lang = data.get("sub_lang", "")

    if not url:
        return {"error": "URLが必要です"}

    task_id = str(uuid.uuid4())
    _tasks[task_id] = {
        "status": "running",
        "progress": 0.0,
        "logs": [],
        "last_log_time": 0.0,
        "filepath": None,
        "filename": None,
        "error": None,
    }

    threading.Thread(
        target=_download_worker,
        args=(task_id, url, fmt, quality, subtitles, sub_lang),
        daemon=True,
    ).start()

    return {"task_id": task_id}


@app.get("/api/youtube/progress/{task_id}")
async def youtube_progress(task_id: str):
    async def generate():
        last_log_count = 0
        while True:
            task = _tasks.get(task_id)
            if not task:
                yield f"data: {json.dumps({'error': 'タスクが見つかりません'})}\n\n"
                return

            new_logs = task["logs"][last_log_count:]
            last_log_count = len(task["logs"])

            payload = {
                "status": task["status"],
                "progress": task["progress"],
                "new_logs": new_logs,
                "filename": task.get("filename"),
                "error": task.get("error"),
            }
            yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"

            if task["status"] in ("done", "error", "cancelled"):
                return

            await asyncio.sleep(0.4)

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/api/youtube/file/{task_id}")
async def youtube_file(task_id: str):
    task = _tasks.get(task_id)
    if not task or not task.get("filepath"):
        return HTMLResponse("ファイルが見つかりません", status_code=404)

    filepath = Path(task["filepath"])
    if not filepath.exists():
        return HTMLResponse("ファイルが見つかりません", status_code=404)

    filename = task.get("filename") or filepath.name

    def _cleanup():
        time.sleep(600)
        try:
            shutil.rmtree(filepath.parent, ignore_errors=True)
        except Exception:
            pass

    threading.Thread(target=_cleanup, daemon=True).start()

    return FileResponse(
        path=str(filepath),
        filename=filename,
        media_type="application/octet-stream",
    )


@app.get("/api/youtube/history")
async def youtube_history():
    return _load_history()


@app.delete("/api/youtube/history")
async def clear_youtube_history():
    try:
        HISTORY_PATH.unlink(missing_ok=True)
    except Exception:
        pass
    return {"ok": True}


@app.post("/api/youtube/cookies")
async def upload_cookies(file: UploadFile = File(...)):
    content = await file.read()
    # Validate it looks like a Netscape cookies file
    text = content.decode("utf-8", errors="replace")
    if not any(line.startswith("# Netscape") or "\t" in line for line in text.splitlines()[:5]):
        return {"error": "Netscape形式のcookies.txtファイルを選択してください。"}
    COOKIES_PATH.write_bytes(content)
    return {"ok": True, "message": "cookies.txtを保存しました。"}


@app.delete("/api/youtube/cookies")
async def delete_cookies():
    try:
        COOKIES_PATH.unlink(missing_ok=True)
    except Exception:
        pass
    return {"ok": True}


@app.get("/api/youtube/cookies/status")
async def cookies_status():
    return {"exists": COOKIES_PATH.exists()}


# ─── Worker ─────────────────────────────────────────

def _parse_quality(quality: str) -> int | None:
    if quality.startswith("自動"):
        return None
    try:
        return int(quality.split("p")[0])
    except Exception:
        return None


def _build_ydl_opts(task_id: str, fmt: str, quality: str) -> dict:
    task_dir = TEMP_DIR / task_id
    task_dir.mkdir(parents=True, exist_ok=True)

    has_ffmpeg = shutil.which("ffmpeg") is not None
    max_height = _parse_quality(quality)

    opts: dict = {
        "outtmpl": str(task_dir / "%(title)s.%(ext)s"),
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "progress_hooks": [lambda d: _progress_hook(task_id, d)],
        # Use iOS/TV client to bypass bot detection without cookies
        "extractor_args": {"youtube": {"player_client": ["ios", "tv_embedded", "web"]}},
    }
    if COOKIES_PATH.exists():
        opts["cookiefile"] = str(COOKIES_PATH)

    if fmt == "音声 (mp3)":
        if not has_ffmpeg:
            raise RuntimeError("音声(mp3)ダウンロードには ffmpeg が必要です。")
        opts["format"] = "bestaudio/best"
        opts["postprocessors"] = [
            {"key": "FFmpegExtractAudio", "preferredcodec": "mp3", "preferredquality": "192"}
        ]
    elif fmt == "動画 (mp4)":
        if max_height:
            if has_ffmpeg:
                opts["format"] = (
                    f"bestvideo[ext=mp4][height<={max_height}]+bestaudio[ext=m4a]/"
                    f"bestvideo[height<={max_height}]+bestaudio[ext=m4a]/"
                    f"bestvideo[height<={max_height}]+bestaudio/best[height<={max_height}]/best"
                )
            else:
                opts["format"] = (
                    f"best[ext=mp4][height<={max_height}]/best[height<={max_height}]/best[ext=mp4]/best"
                )
        else:
            if has_ffmpeg:
                opts["format"] = (
                    "bestvideo[ext=mp4]+bestaudio[ext=m4a]/"
                    "bestvideo+bestaudio[ext=m4a]/bestvideo+bestaudio/best"
                )
            else:
                opts["format"] = "best[ext=mp4]/best"
        if has_ffmpeg:
            opts["merge_output_format"] = "mp4"
    else:
        opts["format"] = "best"

    return opts


def _progress_hook(task_id: str, d: dict) -> None:
    task = _tasks.get(task_id)
    if not task:
        return

    status = d.get("status")
    if status == "downloading":
        total = d.get("total_bytes") or d.get("total_bytes_estimate", 0)
        downloaded = d.get("downloaded_bytes", 0)
        if total:
            task["progress"] = downloaded / total

        now = time.monotonic()
        if now - task["last_log_time"] >= 0.8:
            task["last_log_time"] = now
            speed = d.get("speed") or 0
            eta = d.get("eta")
            dl_str = f"{downloaded / 1_048_576:.1f} MB"
            tot_str = f"{total / 1_048_576:.1f} MB" if total else "?"
            spd_str = f"{speed / 1_048_576:.2f} MB/s" if speed else "-"
            eta_str = f"{eta}s" if eta is not None else "-"
            task["logs"].append(f"{dl_str} / {tot_str}  速度={spd_str}  残り={eta_str}")

    elif status == "finished":
        task["progress"] = 1.0
        task["logs"].append("ファイル取得完了 — 後処理中...")


def _download_worker(
    task_id: str, url: str, fmt: str, quality: str, subtitles: bool, sub_lang: str
) -> None:
    task = _tasks[task_id]
    has_ffmpeg = shutil.which("ffmpeg") is not None
    task["logs"].append(f"URL: {url}")
    task["logs"].append(f"形式: {fmt}  |  品質: {quality}  |  ffmpeg: {'✓' if has_ffmpeg else '✗'}")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            ydl_opts = _build_ydl_opts(task_id, fmt, quality)

            if subtitles:
                ydl_opts["writesubtitles"] = True
                ydl_opts["writeautomaticsub"] = True
                langs = [ln.strip() for ln in sub_lang.split(",") if ln.strip()]
                ydl_opts["subtitleslangs"] = langs or ["all"]
                ydl_opts["subtitlesformat"] = "srt/vtt/best"

            if attempt > 1:
                task["logs"].append(f"再試行 {attempt}/{MAX_RETRIES}")

            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=True)

            title = info.get("title", "不明")
            task_dir = TEMP_DIR / task_id
            all_files = [
                f for f in task_dir.iterdir()
                if f.is_file() and f.suffix not in (".part", ".ytdl", ".json")
            ]

            if all_files:
                filepath = max(all_files, key=lambda p: p.stat().st_mtime)
                size_mb = filepath.stat().st_size / 1_048_576
                task["filepath"] = str(filepath)
                task["filename"] = filepath.name
                task["progress"] = 1.0
                task["status"] = "done"
                task["logs"].append(f"完了: {title}  ({size_mb:.2f} MB)")
                _save_history(url, title, fmt)
            else:
                task["status"] = "error"
                task["error"] = "ダウンロードされたファイルが見つかりません"
                task["logs"].append("エラー: ファイルが見つかりません")
            break

        except yt_dlp.utils.DownloadError as e:
            if attempt < MAX_RETRIES:
                wait = 2 ** attempt
                task["logs"].append(f"エラー: {e}  →  {wait}秒後に再試行")
                time.sleep(wait)
            else:
                task["status"] = "error"
                task["error"] = str(e)
                task["logs"].append(f"ダウンロード失敗（{MAX_RETRIES}回）: {e}")
        except Exception as e:
            task["status"] = "error"
            task["error"] = str(e)
            task["logs"].append(f"エラー: {e}")
            break


def _save_history(url: str, title: str, fmt: str) -> None:
    history = _load_history()
    history.insert(0, {
        "url": url,
        "title": title,
        "format": fmt,
        "date": time.strftime("%Y-%m-%d %H:%M"),
    })
    try:
        with open(HISTORY_PATH, "w", encoding="utf-8") as f:
            json.dump(history[:100], f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def _load_history() -> list:
    try:
        with open(HISTORY_PATH, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []
