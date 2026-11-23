import asyncio
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from storage import METRICS, database, init, record, reminders
from tiktok import detail, profile, username

OWNER = int(os.getenv("OWNER_ID", "7325597184"))
TOKEN = os.getenv("BOT_TOKEN", "")
PERIODS = {1:"сутки", 3:"3 дня", 7:"неделю", 14:"2 недели", 30:"30 дней", 365:"365 дней"}
MENU = {"keyboard":[["📊 Статистика","🔄 Обновить"],["📋 Аккаунты","🔎 Чекер"],["➕ Добавить","💸 Выплаты"]],
        "resize_keyboard":True}
HELP = """Добавьте канал командой /add @имя. Кнопка «Обновить» запускает проверку ваших каналов.

/check @имя — разовый чекер
/list — аккаунты и время проверки
/last @имя — последние 5 обнаруженных видео
/stats 7 — прирост по всем вашим каналам
/stats 7 @имя — отчёт канала
/pay @имя 6 — напомнить через 6 суток после публикации (0 — отключить)
/due — ближайшие напоминания
/remove @имя — перестать следить"""
pending = set()
wake = asyncio.Event()
log = logging.getLogger("monitor")


def num(v):
    return "—" if v is None else f"{v:,}".replace(",", " ")


def stamp(t):
    return datetime.fromtimestamp(t,timezone.utc).strftime("%d.%m.%Y %H:%M UTC") if t else "ожидает проверки"


def api(method, params):
    r = Request(f"https://api.telegram.org/bot{TOKEN}/{method}",
                data=json.dumps(params).encode(),headers={"Content-Type":"application/json"})
    with urlopen(r,timeout=35) as res:
        body = json.load(res)
    if not body.get("ok"):
        raise RuntimeError("Ошибка Telegram API")
    return body["result"]


async def tg(method, **params):
    return await asyncio.to_thread(api,method,params)


async def say(uid,text,markup=None):
    # Split between report rows and respect Telegram's UTF-16 length limit.
    chunk = ""
    for line in text.splitlines():
        if len((chunk + line).encode("utf-16-le")) > 7600:
            await tg("sendMessage",chat_id=uid,text=chunk,link_preview_options={"is_disabled":True})
            chunk = ""
        chunk += line + "\n"
    await tg("sendMessage",chat_id=uid,text=chunk.strip() or "Пусто",
             link_preview_options={"is_disabled":True}, **({"reply_markup":markup} if markup else {}))


def read_video(pair):
    name, vid = pair
    try:
        return detail(name,vid)
    except Exception:
        return None


def lookup(name, old=(), checker=False):
    followers, hearts, videos = profile(name)
    newest = sorted(videos, key=lambda i: videos[i].posted, reverse=True)
    ids = newest[:5] if checker else newest + [i for i in old if i not in videos][:4]
    with ThreadPoolExecutor(max_workers=3) as pool:
        for v in pool.map(read_video, ((name,i) for i in ids)):
            if v:
                videos[v.id] = v
    return followers,hearts,videos,set(ids)


def scan(name):
    with database() as c:
        old = [r[0] for r in c.execute("select id from videos where name=? order by attempted,id", (name,))]
    try:
        followers,hearts,videos,tried = lookup(name,old)
        record(name,followers,hearts,videos,tried,int(time.time()))
    except Exception as e:
        log.warning("Проверка @%s: %s",name,type(e).__name__)
        with database() as c:
            c.execute("update accounts set error=? where name=?",("TikTok не вернул профиль",name))


async def worker():
    next_tick = 0
    while True:
        if time.monotonic() >= next_tick:
            with database() as c:
                pending.update(r[0] for r in c.execute("select distinct name from subs"))
            next_tick = time.monotonic()+300
        if pending:
            batch = sorted(pending)
            pending.clear()
            for name in batch:
                await asyncio.to_thread(scan,name)
            continue
        wake.clear()
        try:
            await asyncio.wait_for(wake.wait(),max(0,next_tick-time.monotonic()))
        except asyncio.TimeoutError:
            pass


def rows(uid):
    with database() as c:
        return list(c.execute("select a.*,s.since,s.days from accounts a join subs s using(name) where uid=? order by name",(uid,)))


def last_text(name, videos):
    out = [f"Последние 5 обнаруженных видео @{name}", "👁 просмотры · ♥ лайки · 💬 комментарии · ↗ репосты · ★ сохранения"]
    for v in sorted(videos,key=lambda r:r["posted"],reverse=True)[:5]:
        out += [f"\n{stamp(v['posted'])} · {v['title'][:90]}",
                " · ".join(num(v[k]) for k in METRICS),f"https://www.tiktok.com/@{name}/video/{v['id']}"]
    if not videos:
        out.append("Публичных роликов пока нет.")
    return "\n".join(out)


def report(uid,days,name=None):
    out = [f"📊 Прирост за {PERIODS[days]}"]
    cutoff = int(time.time())-days*86400
    totals = [0]*7
    count = 0
    with database() as c:
        for a in rows(uid):
            n = a["name"]
            if name and n!=name:
                continue
            if not a["last"]:
                out.append(f"@{n} — {a['error'] or 'ожидает проверки'}")
                continue
            start = max(cutoff,a["since"])
            base = c.execute("select * from history where name=? and ts<=? order by ts desc limit 1",(n,start)).fetchone()
            if not base or base["ts"] < a["since"]:
                base = c.execute("select * from history where name=? and ts>=? order by ts limit 1",(n,a["since"])).fetchone()
            if not base:
                out.append(f"@{n} — ждём первый замер")
                continue
            sums = c.execute("select sum(views),sum(likes),sum(comments),sum(shares),sum(saves) from history where name=? and ts>?",(n,base["ts"])).fetchone()
            vals = [v or 0 for v in sums]+[a["followers"]-base["followers"],a["hearts"]-base["hearts"]]
            count += 1
            totals = [x+y for x,y in zip(totals,vals)]
            out += [f"\n@{n} · {stamp(base['ts'])} → {stamp(a['last'])}",metrics_text(vals)]
            if a["error"]:
                out.append("⚠ Последняя попытка обновления не удалась.")
    if count>1:
        out += ["\nВсего по каналам",metrics_text(totals)]
    if len(out)==1:
        out.append("Добавьте аккаунт через /add @имя.")
    out.append("Прирост по доступным замерам. Первый снимок задаёт базу. Лайки профиля TikTok считает отдельно.")
    return "\n".join(out)


def metrics_text(v):
    labels = ("Просмотры","Лайки видео","Комментарии","Репосты","Сохранения","Подписчики","Лайки профиля")
    return " · ".join(f"{k} {num(i)}" for k,i in zip(labels,v))


def allowed(uid):
    with database() as c:
        return bool(c.execute("select 1 from users where uid=?", (uid,)).fetchone())


async def dispatch(update):
    cb = update.get("callback_query")
    msg = cb["message"] if cb else update.get("message",{})
    person = cb["from"] if cb else msg.get("from",{})
    uid = person.get("id")
    if msg.get("chat",{}).get("type")!="private" or msg["chat"]["id"]!=uid:
        return
    if not allowed(uid):
        if cb:
            await tg("answerCallbackQuery",callback_query_id=cb["id"],text="Доступ закрыт")
        else:
            await say(uid,f"Передайте владельцу @lapiduser ваш Telegram ID: {uid}")
        return
    if cb:
        await tg("answerCallbackQuery",callback_query_id=cb["id"])
    raw = cb.get("data","") if cb else msg.get("text","")
    aliases = {"📊 Статистика":"/stats","🔄 Обновить":"/refresh","📋 Аккаунты":"/list","🔎 Чекер":"/check",
               "➕ Добавить":"/add","💸 Выплаты":"/due"}
    parts = aliases.get(raw,raw).split()
    cmd = parts[0].split("@")[0] if parts else ""
    args = parts[1:]
    now = int(time.time())
    mine = {r["name"]:r for r in rows(uid)}
    if cmd in ("/start","/help"):
        await say(uid,HELP + ("\n/grant ID · /revoke ID — управление доступом" if uid==OWNER else ""),MENU)
    elif cmd in ("/grant","/revoke"):
        if uid != OWNER:
            raise ValueError("Доступ выдаёт владелец.")
        if len(args)!=1 or not args[0].isascii() or not args[0].isdigit() or not 0<int(args[0])<2**63:
            raise ValueError(f"Пример: {cmd} 123456789")
        target = int(args[0])
        if cmd=="/revoke" and target==OWNER:
            raise ValueError("Владелец сохраняет доступ.")
        with database() as c:
            if cmd=="/grant":
                c.execute("insert or ignore into users values(?)",(target,))
            else:
                c.execute("delete from users where uid=?",(target,))
                c.execute("delete from accounts where name not in (select name from subs)")
        await say(uid,("Доступ выдан: " if cmd=="/grant" else "Доступ отозван: ")+str(target))
    elif cmd in ("/check","/add","/remove","/last","/pay"):
        if not args:
            raise ValueError(f"Отправьте {cmd} @имя" + (" 6 (число суток)" if cmd=="/pay" else ""))
        name = username(args[0])
        if cmd=="/check":
            try:
                f,h,vs,_ = await asyncio.to_thread(lookup,name,(),True)
            except Exception:
                raise ValueError("Публичные данные недоступны. Проверьте юзернейм или повторите позднее.") from None
            await say(uid,f"@{name} · подписчики {num(f)} · лайки профиля {num(h)}\n"+last_text(name,[vars(v) for v in vs.values()]))
        elif cmd=="/add":
            with database() as c:
                c.execute("insert or ignore into accounts(name) values(?)",(name,))
                c.execute("insert or ignore into subs(uid,name,since) values(?,?,?)",(uid,name,now))
            pending.add(name)
            wake.set()
            await say(uid,f"@{name} в списке. Проверка поставлена в очередь. Результат — /list")
        elif name not in mine:
            raise ValueError("Сначала добавьте этот канал через /add @имя.")
        elif cmd=="/last":
            with database() as c:
                vs = list(c.execute("select * from videos where name=? order by posted desc limit 5",(name,)))
            await say(uid,last_text(name,vs))
        elif cmd=="/remove":
            with database() as c:
                c.execute("delete from subs where uid=? and name=?",(uid,name))
                c.execute("delete from accounts where name=? and name not in (select name from subs)",(name,))
            await say(uid,f"Отслеживание @{name} отключено.")
        else:
            if len(args)!=2 or not args[1].isascii() or not args[1].isdigit() or not 0<=int(args[1])<=365:
                raise ValueError("Пример: /pay @имя 6. Допустимо от 0 до 365 суток.")
            with database() as c:
                c.execute("update subs set days=? where uid=? and name=?",(int(args[1]),uid,name))
                c.execute("delete from alerts where uid=? and name=? and kind='pay' and sent=0",(uid,name))
            await say(uid,"Настройка выплат сохранена.")
    elif cmd=="/refresh":
        pending.update(mine)
        wake.set()
        await say(uid,f"В очереди на обновление: {len(mine)}. Время последней успешной проверки — /list.")
    elif cmd=="/list":
        await say(uid,"\n".join(f"@{n} · {stamp(a['last'])} · подписчики {num(a['followers'])} · лайки {num(a['hearts'])}"+
                                (f" · ⚠ {a['error']}" if a['error'] else "") for n,a in mine.items()) or "Добавьте аккаунт через /add @имя.")
    elif cmd=="/stats":
        if not args:
            await say(uid,"Выберите период. Для одного канала: /stats 7 @имя",
                      {"inline_keyboard":[[{"text":PERIODS[d],"callback_data":f"/stats {d}"} for d in group] for group in [(1,3,7),(14,30,365)]]})
        else:
            if args[0] not in {str(d) for d in PERIODS}:
                raise ValueError("Выберите период: 1, 3, 7, 14, 30 или 365 дней.")
            name = username(args[1]) if len(args)>1 else None
            if name and name not in mine:
                raise ValueError("Канал отсутствует в вашем списке.")
            await say(uid,report(uid,int(args[0]),name))
    elif cmd=="/due":
        with database() as c:
            due = list(c.execute("""select s.name,v.id,v.views,v.posted+s.days*86400 as due from subs s join videos v using(name)
              where uid=? and days>0 and due>=s.since and not exists
              (select 1 from settled t where (t.uid,t.name,t.vid)=(s.uid,s.name,v.id)) order by due limit 30""",(uid,)))
        await say(uid,"Ближайшие выплаты (до 30)\n"+"\n".join(f"{stamp(r['due'])} · @{r['name']} · {num(r['views'])} 👁\nhttps://www.tiktok.com/@{r['name']}/video/{r['id']}" for r in due) if due else "Предстоящих выплат по обнаруженным роликам пока нет.")
    elif cmd=="/done" and len(args)==2 and args[0] in mine:
        with database() as c:
            c.execute("insert or ignore into settled select ?,name,id from videos where name=? and id=?",(uid,*args))
        await say(uid,"Заявка отмечена как поданная.")
    else:
        await say(uid,HELP,MENU)


async def deliver():
    while True:
        reminders(int(time.time()))
        with database() as c:
            queue = list(c.execute("select * from alerts where sent=0 order by rowid limit 100"))
        for a in queue:
            key = (a["uid"],a["name"],a["vid"],a["kind"])
            with database() as c:
                if not c.execute("select 1 from alerts where (uid,name,vid,kind)=(?,?,?,?) and sent=0",key).fetchone():
                    continue
            markup = None
            if a["kind"]=="pay":
                markup = {"inline_keyboard":[[{"text":"Заявка подана","callback_data":f"/done {a['name']} {a['vid']}"}]]}
            try:
                await say(a["uid"],a["body"],markup)
            except HTTPError as e:
                if e.code==429:
                    await asyncio.sleep(retry_delay(e))
                    break
                if e.code!=403:
                    continue
                # A blocked bot cannot deliver. Future alerts are still independent.
                status = -1
            except OSError:
                continue
            else:
                status = 1
            with database() as c:
                c.execute("update alerts set sent=? where (uid,name,vid,kind)=(?,?,?,?)",(status,*key))
            await asyncio.sleep(0.1)
        await asyncio.sleep(15)


def retry_delay(err):
    try:
        return max(1,int(json.load(err)["parameters"]["retry_after"]))
    except (ValueError,KeyError,OSError):
        return 10


async def poll():
    with database() as c:
        row = c.execute("select value from meta where key='offset'").fetchone()
    offset = row[0] if row else 0
    while True:
        try:
            updates = await tg("getUpdates",offset=offset,timeout=20,allowed_updates=["message","callback_query"])
            for update in updates:
                try:
                    await dispatch(update)
                except ValueError as e:
                    source = update.get("callback_query",{}).get("message") or update.get("message",{})
                    await say(source["chat"]["id"],str(e))
                except HTTPError as e:
                    if e.code not in (400,403):
                        raise
                    log.warning("Ответ Telegram: HTTP %d",e.code)
                offset = update["update_id"]+1
                with database() as c:
                    c.execute("insert or replace into meta values('offset',?)",(offset,))
        except HTTPError as e:
            log.warning("Telegram: HTTP %d",e.code)
            await asyncio.sleep(retry_delay(e) if e.code==429 else 8)
        except OSError as e:
            log.warning("Соединение Telegram: %s",type(e).__name__)
            await asyncio.sleep(5)


async def run():
    if not TOKEN:
        raise SystemExit("Задайте BOT_TOKEN в переменных окружения хостинга.")
    init(OWNER)
    # Long polling requires that no webhook is registered for this bot.
    await tg("deleteWebhook",drop_pending_updates=False)
    await asyncio.gather(worker(),deliver(),poll())


if __name__=="__main__":
    logging.basicConfig(level=logging.INFO,format="%(asctime)s %(levelname)s %(message)s")
    try:
        asyncio.run(run())
    except HTTPError as exc:
        raise SystemExit(f"Telegram вернул HTTP {exc.code}. Проверьте BOT_TOKEN и соединение.") from None
