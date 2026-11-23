"""Public TikTok creator embed and public video pages. No session cookies."""
import json
import re
from dataclasses import dataclass
from urllib.request import Request, urlopen


def username(text):
    value = text.strip().removeprefix("@").lower()
    if not re.fullmatch(r"[a-z0-9_][a-z0-9_.]{0,23}", value) or value.endswith("."):
        raise ValueError("Укажите TikTok-юзернейм, например @tiktok.")
    return value


def state(path, script):
    req = Request("https://www.tiktok.com/" + path, headers={"User-Agent": "Mozilla/5.0", "Accept": "text/html"})
    with urlopen(req, timeout=12) as response:
        html = response.read(5_000_000).decode("utf-8", "replace")
    match = re.search(r'<script\b[^>]*\bid="' + script + r'"[^>]*>(.*?)</script>', html, re.S)
    if not match:
        raise ValueError("TikTok не вернул публичные данные.")
    return json.loads(match[1])


@dataclass
class Video:
    id: str
    title: str
    posted: int
    views: int
    likes: int | None = None
    comments: int | None = None
    shares: int | None = None
    saves: int | None = None


def profile(name):
    name = username(name)
    route = "/embed/@" + name
    items = state(route[1:], "__FRONTITY_CONNECT_STATE__")["source"]["data"]
    page = next((v for v in items.values() if isinstance(v, dict) and "userInfo" in v), {})
    info = page.get("userInfo") or {}
    if page.get("isError") or str(info.get("uniqueId", "")).lower() != name or info.get("privateAccount"):
        raise ValueError("Профиль закрыт, переименован или недоступен.")
    videos = {}
    for item in page["videoList"]:
        vid = str(item["id"])
        if not re.fullmatch(r"\d{15,21}", vid) or item.get("privateItem"):
            continue
        # TikTok snowflake IDs encode the upload timestamp in their high bits.
        videos[vid] = Video(vid, item.get("desc", ""), int(vid) >> 32, int(item["playCount"]))
    return int(info["followerCount"]), int(info["heartCount"]), videos


def detail(name, vid):
    scope = state(f"@{name}/video/{vid}", "__UNIVERSAL_DATA_FOR_REHYDRATION__")["__DEFAULT_SCOPE__"]
    v = scope["webapp.video-detail"]["itemInfo"]["itemStruct"]
    if str(v["id"]) != vid or v["author"]["uniqueId"].lower() != name:
        raise ValueError("Видео недоступно.")
    stats = v.get("statsV2") or v["stats"]
    def optional(k):
        return int(stats[k]) if stats.get(k) is not None else None
    return Video(vid, v.get("desc", ""), int(v["createTime"]), int(stats["playCount"]),
                 optional("diggCount"), optional("commentCount"), optional("shareCount"), optional("collectCount"))
