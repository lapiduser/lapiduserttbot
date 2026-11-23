import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path

DB = Path(os.getenv("DATABASE_PATH", "data/monitor.db"))
METRICS = ("views", "likes", "comments", "shares", "saves")


@contextmanager
def database():
    DB.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB, timeout=15)
    con.row_factory = sqlite3.Row
    try:
        con.execute("pragma foreign_keys=on")
        with con:
            yield con
    finally:
        con.close()


def init(owner):
    with database() as c:
        c.execute("pragma journal_mode=wal")
        c.executescript("""
        CREATE TABLE IF NOT EXISTS users(uid INTEGER PRIMARY KEY);
        CREATE TABLE IF NOT EXISTS accounts(name TEXT PRIMARY KEY, first INTEGER, last INTEGER,
            followers INTEGER, hearts INTEGER, error TEXT);
        CREATE TABLE IF NOT EXISTS subs(uid INTEGER REFERENCES users ON DELETE CASCADE,
            name TEXT REFERENCES accounts ON DELETE CASCADE, since INTEGER, days INTEGER DEFAULT 6,
            PRIMARY KEY(uid,name));
        CREATE TABLE IF NOT EXISTS videos(name TEXT REFERENCES accounts ON DELETE CASCADE,id TEXT,
            title TEXT,posted INTEGER,views INTEGER,likes INTEGER,comments INTEGER,shares INTEGER,saves INTEGER,
            attempted INTEGER DEFAULT 0, seen INTEGER, PRIMARY KEY(name,id));
        CREATE TABLE IF NOT EXISTS history(name TEXT REFERENCES accounts ON DELETE CASCADE,ts INTEGER,
            views INTEGER,likes INTEGER,comments INTEGER,shares INTEGER,saves INTEGER,
            followers INTEGER,hearts INTEGER,PRIMARY KEY(name,ts));
        CREATE TABLE IF NOT EXISTS alerts(uid INTEGER,name TEXT,vid TEXT,kind TEXT,body TEXT,
            sent INTEGER DEFAULT 0, PRIMARY KEY(uid,name,vid,kind),
            FOREIGN KEY(uid,name) REFERENCES subs ON DELETE CASCADE);
        CREATE TABLE IF NOT EXISTS settled(uid INTEGER,name TEXT,vid TEXT,PRIMARY KEY(uid,name,vid),
            FOREIGN KEY(uid,name) REFERENCES subs ON DELETE CASCADE);
        CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value INTEGER);
        """)
        c.execute("insert or ignore into users values(?)", (owner,))


def enqueue(c, uid, name, v, kind, heading):
    body = f"{heading}\n@{name} · {v['views']:,} просмотров\n{v['title'][:160]}\nhttps://www.tiktok.com/@{name}/video/{v['id']}"
    c.execute("insert or ignore into alerts(uid,name,vid,kind,body) values(?,?,?,?,?)",
              (uid, name, v["id"], kind, body))


def record(name, followers, hearts, videos, attempted, now):
    with database() as c:
        c.execute("begin immediate")
        account = c.execute("select * from accounts where name=?", (name,)).fetchone()
        if account is None:
            return
        totals = [0] * len(METRICS)
        members = list(c.execute("select * from subs where name=?", (name,)))
        for v in videos.values():
            old = c.execute("select * from videos where name=? and id=?", (name,v.id)).fetchone()
            values = []
            for i,k in enumerate(METRICS):
                value = getattr(v,k)
                previous = old[k] if old else None
                if value is not None:
                    if previous is not None:
                        totals[i] += value - previous
                    elif account["last"] is not None and v.posted >= account["last"]:
                        totals[i] += value
                values.append(value if value is not None else previous)
            tried = now if v.id in attempted else (old["attempted"] if old else 0)
            c.execute("insert or replace into videos values(?,?,?,?,?,?,?,?,?,?,?)",
                      (name,v.id,v.title,v.posted,*values,tried,now))
            for m in members:
                crossed = old is not None and old["views"] <= 5000 and old["seen"] >= m["since"]
                if v.views > 5000 and (crossed or v.posted >= m["since"]):
                    enqueue(c,m["uid"],name,vars(v),"viral","🔥 Видео превысило 5 000 просмотров")
        for vid in attempted - videos.keys():
            c.execute("update videos set attempted=? where name=? and id=?", (now,name,vid))
        c.execute("""insert into history values(?,?,?,?,?,?,?,?,?)
                  on conflict(name,ts) do update set views=views+excluded.views,
                  likes=likes+excluded.likes,comments=comments+excluded.comments,
                  shares=shares+excluded.shares,saves=saves+excluded.saves,
                  followers=excluded.followers,hearts=excluded.hearts""",
                  (name,now,*totals,followers,hearts))
        c.execute("update accounts set first=coalesce(first,?),last=?,followers=?,hearts=?,error=null where name=?",
                  (now,now,followers,hearts,name))


def reminders(now):
    with database() as c:
        for r in c.execute("""select s.uid,v.* from subs s join videos v using(name)
            where s.days>0 and v.posted+s.days*86400 between s.since and ?
            and not exists(select 1 from settled t where (t.uid,t.name,t.vid)=(s.uid,s.name,v.id))
            and not exists(select 1 from alerts a where (a.uid,a.name,a.vid,a.kind)=(s.uid,s.name,v.id,'pay'))""", (now,)):
            enqueue(c,r["uid"],r["name"],r,"pay","💸 Пора подать заявку на выплату")
