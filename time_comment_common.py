"""time_comment_data(Turso, 対象時刻×日付で1行)の生成ロジック共通部。

analytics_aggregates_updater/time_comment_common.py のコピー(2026-07-29、turso_client.py等と
同じ「複数コピーを手動同期」パターン)。karotter_bot/temporary/backfill_time_comment_data.py
（初回一括バックフィル）とanalytics_aggregates_updater/main.py（日次差分更新、翌日0:01 JST確定）
の判定・書き込みロジックはそちらのコピーを参照。このコピーは comment_sync/sync.py 専用で、
対象時刻発生直後の数分間だけ即時反映するための upsert_achieved_only() を追加で持つ
(ファイル末尾、日次バッチ側のコピーには存在しない)。クエリの対象範囲
（全期間 or 直近N日）は呼び出し側が決める（heatmap_common.py/ranking_common.pyと同型）。

判定アルゴリズムは `analytics_graph/集計/時刻コメント未達成日_知見と実装.md` の
擬似コードに準拠: 対象時刻ちょうどの分に投稿されたコメント本文が、NFKC正規化+
「時」→":"/「分」→削除した上で、12時間制/24時間制どちらの表記でも(ゼロ埋め有無を
問わず)対象時刻を含めば「達成」。

time_comment_data の各行:
    time_key           TEXT: '00:00' '07:21' '08:10' '11:45' '19:19' '19:21' '20:10' '23:45'
    date               TEXT: JST暦日 'YYYY-MM-DD'
    achieved           INTEGER: 0 or 1
    winner_channel_id  TEXT: 一番乗り(published_at最小)コメントの投稿者channel_id。achieved=0ならNULL
    winner_handle      TEXT: 同、投稿時点のhandle(スナップショット、リネーム追従なし)
PRIMARY KEY (time_key, date)。8時刻 × 全履歴日数(現在約800日)で数千行程度。

INCLUDE_DELETED = True: 削除済みコメント(is_deleted=1)も判定対象に含める。
  「その日に達成コメントが投稿されたという歴史的事実」を問う機能であり、後から削除
  されても事実は変わらないため(削除済みアカウントも対象に含める、という参照ドキュメント
  の方針の延長)。heatmap_data等の他集計がis_deleted=0でフィルタするのとは意図的に異なる。

THREADS_ONLY = False: スレッド(親コメント)・返信の両方を判定対象に含める。

一番乗り(winner)の決定方法:
  同一(time_key, date)に複数コメントがヒットした場合、published_at(UNIX秒)が最小の
  ものを勝者とする。published_atが同一秒で複数アカウントがタイした場合(YouTube APIは
  秒未満を公開しないため区別不能)は、Turso comments テーブルの暗黙rowidが大きい方
  (=後から書き込まれた方)を勝者とする。理由: comment_sync/sync.pyの新着取得は
  commentThreads.list(order="time")(新しい順)のレスポンスをそのままの順でバッチ
  INSERTするため、同一バッチ内の新規行はrowidが受信順=新しい順に単調増加する。
  つまり同着タイの中でrowidが大きい(=バッチの後方=新しい順ストリームの後方=真の
  時系列ではより古い)方が実際には先に投稿されている。ただしこれは「同じ同期run内で
  一緒に新規発見された」場合のみ正確で、別runやバックフィル経由で見つかった場合は
  rowidに意味のある前後関係が無い(ベストエフォート、詳細はCLAUDE.mdの設計議論を参照)。
"""
import re
import time
import unicodedata
from datetime import date, datetime
from zoneinfo import ZoneInfo

from turso_client import TursoClient

JST = ZoneInfo("Asia/Tokyo")

TARGET_TIMES = ["00:00", "07:21", "08:10", "11:45", "19:19", "19:21", "20:10", "23:45"]
INCLUDE_DELETED = True
THREADS_ONLY = False

TABLE_SQL = """
CREATE TABLE IF NOT EXISTS time_comment_data (
  time_key           TEXT NOT NULL,
  date               TEXT NOT NULL,
  achieved           INTEGER NOT NULL,
  winner_channel_id  TEXT,
  winner_handle      TEXT,
  updated_at         INTEGER NOT NULL,
  PRIMARY KEY (time_key, date)
)
"""

# 既存デプロイ(winner列追加前)向けの一回限りの追加マイグレーション。
# 列が既に存在する場合はTurso側が "duplicate column name" エラーを返すので無視する
# (CREATE TABLE IF NOT EXISTSは既存テーブルに新列を反映しないため、この追加が必要)。
_MIGRATE_WINNER_COLUMNS_SQL = [
    "ALTER TABLE time_comment_data ADD COLUMN winner_channel_id TEXT",
    "ALTER TABLE time_comment_data ADD COLUMN winner_handle TEXT",
]

_UPSERT_SQL = """
INSERT INTO time_comment_data (time_key, date, achieved, winner_channel_id, winner_handle, updated_at)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(time_key, date) DO UPDATE SET
  achieved = excluded.achieved,
  winner_channel_id = excluded.winner_channel_id,
  winner_handle = excluded.winner_handle,
  updated_at = excluded.updated_at
"""

# /ranking の日別「時報コメント順番ランキング」用。time_comment_data は一番乗り
# だけを保持するが、こちらは該当分に投稿された全員を順位付きで保持する。
_ORDER_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS time_comment_order (
  time_key           TEXT NOT NULL,
  date               TEXT NOT NULL,
  rank               INTEGER NOT NULL,
  comment_id         TEXT NOT NULL,
  author_channel_id  TEXT,
  handle             TEXT,
  published_at       INTEGER NOT NULL,
  kind               TEXT NOT NULL,
  resolved           INTEGER NOT NULL,
  updated_at         INTEGER NOT NULL,
  PRIMARY KEY (time_key, date, rank)
)
"""

_ORDER_UPSERT_SQL = """
INSERT INTO time_comment_order
  (time_key, date, rank, comment_id, author_channel_id, handle, published_at, kind, resolved, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(time_key, date, rank) DO UPDATE SET
  comment_id = excluded.comment_id,
  author_channel_id = excluded.author_channel_id,
  handle = excluded.handle,
  published_at = excluded.published_at,
  kind = excluded.kind,
  resolved = excluded.resolved,
  updated_at = excluded.updated_at
"""

_WRITE_CHUNK = 200


def ensure_schema(turso: TursoClient) -> None:
    """テーブル作成 + winner列マイグレーションを冪等に行う。upsert_range()の先頭で呼ぶ。"""
    turso.execute(TABLE_SQL)
    for stmt in _MIGRATE_WINNER_COLUMNS_SQL:
        try:
            turso.execute(stmt)
        except Exception as e:
            if "duplicate column name" not in str(e).lower():
                raise


def _parse_time_key(time_key: str) -> tuple[int, int]:
    h, m = time_key.split(":")
    return int(h), int(m)


def _build_pattern(h24: int, m: int) -> re.Pattern:
    h12 = ((h24 + 11) % 12) + 1
    hours_alt = f"{h24}" if h24 == h12 else f"(?:{h24}|{h12})"
    return re.compile(rf"(?<![0-9])0*{hours_alt}:0*{m}(?![0-9])")


def _build_lookup_tables() -> tuple[dict[int, str], dict[str, re.Pattern]]:
    minute_to_key: dict[int, str] = {}
    patterns: dict[str, re.Pattern] = {}
    for tk in TARGET_TIMES:
        h24, m = _parse_time_key(tk)
        minute_to_key[h24 * 60 + m] = tk
        patterns[tk] = _build_pattern(h24, m)
    return minute_to_key, patterns


# 8つの対象時刻ぶんの (通算分 -> time_key) 逆引きと、time_key -> コンパイル済み正規表現。
# 公開名(先頭アンダースコアなし)なのは、backfill側の素朴パターンとの比較検証でも参照するため。
MINUTE_TO_KEY, PATTERNS = _build_lookup_tables()


def normalize_text(s: str) -> str:
    s = unicodedata.normalize("NFKC", s)
    return s.replace("時", ":").replace("分", "")


def classify_row(row: dict) -> tuple[str, str] | None:
    """1コメント行が対象時刻を「達成」しているか判定する。

    row: {"published_at": int, "text": str, "parent_id": str|None, "is_deleted": int(省略可)}
    達成なら (time_key, date_str) を返す。非達成/対象外(THREADS_ONLY等)なら None。
    """
    if THREADS_ONLY and row.get("parent_id") is not None:
        return None
    if not INCLUDE_DELETED and row.get("is_deleted"):
        return None
    text = row.get("text")
    published_at = row.get("published_at")
    if not text or published_at is None:
        return None

    jst_dt = datetime.fromtimestamp(published_at, tz=JST)
    time_key = MINUTE_TO_KEY.get(jst_dt.hour * 60 + jst_dt.minute)
    if time_key is None:
        return None

    if PATTERNS[time_key].search(normalize_text(str(text))):
        return (time_key, jst_dt.date().isoformat())
    return None


def classify_specific_time(row: dict, time_key: str) -> tuple[str, str] | None:
    """Apply the normal time-comment rules to an achievement-only time.

    Unlike adding a value to TARGET_TIMES, this does not change the public
    eight-time calendar/ranking feature. It is used for 07:20 by account
    achievements while preserving the exact-minute, text-pattern,
    INCLUDE_DELETED and THREADS_ONLY semantics of classify_row().
    """
    if THREADS_ONLY and row.get("parent_id") is not None:
        return None
    if not INCLUDE_DELETED and row.get("is_deleted"):
        return None
    text = row.get("text")
    published_at = row.get("published_at")
    if not text or published_at is None:
        return None
    h24, minute = _parse_time_key(time_key)
    jst_dt = datetime.fromtimestamp(published_at, tz=JST)
    if (jst_dt.hour, jst_dt.minute) != (h24, minute):
        return None
    if _build_pattern(h24, minute).search(normalize_text(str(text))):
        return (time_key, jst_dt.date().isoformat())
    return None


def classify_rows(rows: list[dict]) -> set[tuple[str, str]]:
    """classify_rowをまとめて呼ぶ薄いラッパ(日次更新の少量データ向け)。"""
    result: set[tuple[str, str]] = set()
    for row in rows:
        hit = classify_row(row)
        if hit is not None:
            result.add(hit)
    return result


def group_matches(rows: list[dict]) -> dict[tuple[str, str], list[dict]]:
    """達成行を(time_key, date)ごとにグルーピングし、勝者決定に必要な情報を集める。

    row は少なくとも comment_id/author_channel_id/handle/published_at/parent_id を
    持つこと。reply_order があれば返信同士の同着解決に使う(無ければ解決不能扱い)。
    """
    groups: dict[tuple[str, str], list[dict]] = {}
    for row in rows:
        hit = classify_row(row)
        if hit is None:
            continue
        groups.setdefault(hit, []).append({
            "comment_id": row.get("comment_id"),
            "author_channel_id": row.get("author_channel_id"),
            "handle": row.get("handle"),
            "published_at": row.get("published_at"),
            "parent_id": row.get("parent_id"),
            "reply_order": row.get("reply_order"),
        })
    return groups


def _resolve_tie(tied: list[dict], thread_order_evidence: dict[str, tuple[int, int]]):
    """同着候補(同一published_at)の中から真の投稿順で一番乗りを決める。決定できなければNone。

    真の順序が引けない候補(主に削除済みでthread_order_evidence/reply_orderに存在しない
    コメント)は比較対象から除外して、残りの候補だけで解決する——「順序不明」と
    「削除済みだから無条件で除外」は別物であり、削除済みでも取得当時の順序記録が
    残っていれば通常の候補として扱い、勝者にもなり得る(バックフィル版
    build_time_comment_order.pyのrank_group()と同じ方針、2026-08-12)。

    スレッド同士: thread_order_evidence({comment_id: (run_started_at, run_seq)}、
      sync_new_comments()がその場で発見した順を書き込む)を使う。記録がある候補のうち、
      全員が同一run_started_at(=同一runで一緒に発見された)を持つ場合のみ、
      run_seq最小(真の時系列で最も古い)を勝者とする。記録が1件も無い、または
      記録がある候補同士でrun_started_atが割れている場合は解決不能。
    返信同士(同一スレッド内): reply_orderが分かる候補の中で最小(=最古)を勝者とする
      (reply_orderがpublishedAt昇順でソート済みであることが前提——
      comments_db/refresh_replies_local.py・comment_sync/sync.pyの各書き込み経路が
      この前提を満たすよう2026-08-12に統一済み)。
    別スレッドをまたぐ返信同士、スレッドと返信の混在は常に解決不能
    (比較する軸そのものが存在しない)。
    """
    threads = [c for c in tied if c.get("parent_id") is None]
    replies = [c for c in tied if c.get("parent_id") is not None]

    if len(threads) == len(tied):
        with_evidence = [c for c in threads if thread_order_evidence.get(c["comment_id"]) is not None]
        if not with_evidence:
            return None
        run_ids = {thread_order_evidence[c["comment_id"]][0] for c in with_evidence}
        if len(run_ids) != 1:
            return None
        return min(with_evidence, key=lambda c: thread_order_evidence[c["comment_id"]][1])

    if len(replies) == len(tied):
        parents = {c.get("parent_id") for c in replies}
        if len(parents) != 1:
            return None
        with_order = [c for c in replies if c.get("reply_order") is not None]
        if not with_order:
            return None
        return min(with_order, key=lambda c: c["reply_order"])

    return None


def resolve_winners(
    groups: dict[tuple[str, str], list[dict]],
    thread_order_evidence: dict[str, tuple[int, int]] | None = None,
) -> dict[tuple[str, str], dict]:
    """グループごとに一番乗り(published_at最小、同着は真の投稿順)を決定する。

    thread_order_evidence: fetch_thread_order_evidence() の戻り値をそのまま渡す。
    省略時(None)はスレッド同士の同着が全て解決不能になる。

    戻り値: {(time_key, date): {"channel_id":..., "handle":...}}(決定できたぶんのみ。
    決定できない同着はキー自体が存在しない——呼び出し側は該当日を
    winner_channel_id=NULL・tie扱いにすること)。
    """
    evidence = thread_order_evidence or {}
    winners: dict[tuple[str, str], dict] = {}
    for key, candidates in groups.items():
        min_pub = min(c["published_at"] for c in candidates)
        tied = [c for c in candidates if c["published_at"] == min_pub]
        if len(tied) == 1:
            winner = tied[0]
        else:
            winner = _resolve_tie(tied, evidence)
            if winner is None:
                continue
        winners[key] = {"channel_id": winner["author_channel_id"], "handle": winner["handle"]}
    return winners


def fetch_thread_order_evidence(turso, comment_ids: list[str]) -> dict[str, tuple[int, int]]:
    """thread_order_evidence から指定comment_idぶんだけ引く(PK探索、有界クエリ)。"""
    ids = sorted({cid for cid in comment_ids if cid})
    if not ids:
        return {}
    evidence: dict[str, tuple[int, int]] = {}
    for i in range(0, len(ids), 200):
        chunk = ids[i:i + 200]
        placeholders = ",".join("?" for _ in chunk)
        rows = turso.query(
            f"SELECT comment_id, run_started_at, run_seq FROM thread_order_evidence "
            f"WHERE comment_id IN ({placeholders})",
            chunk,
        )
        for r in rows:
            evidence[r["comment_id"]] = (r["run_started_at"], r["run_seq"])
    return evidence


def rank_order_group(
    candidates: list[dict],
    thread_order_evidence: dict[str, tuple[int, int]] | None = None,
) -> list[dict]:
    """Return every candidate in display order, preserving unresolved ties.

    ``published_at`` is only second-resolution. Within a tied second, use the
    same evidence as ``resolve_winners`` when the comments are comparable;
    otherwise retain a deterministic order and mark that slice unresolved.
    """
    evidence = thread_order_evidence or {}
    buckets: dict[int, list[dict]] = {}
    for candidate in candidates:
        buckets.setdefault(candidate["published_at"], []).append(candidate)

    ordered: list[dict] = []
    for _published_at, bucket in sorted(buckets.items()):
        if len(bucket) == 1:
            row = dict(bucket[0])
            row["resolved"] = True
            ordered.append(row)
            continue

        threads = [c for c in bucket if c.get("parent_id") is None]
        replies = [c for c in bucket if c.get("parent_id") is not None]
        known: list[tuple[int, dict]] = []
        comparable = False

        if len(threads) == len(bucket):
            with_evidence = [c for c in threads if c.get("comment_id") in evidence]
            run_ids = {evidence[c["comment_id"]][0] for c in with_evidence}
            if with_evidence and len(run_ids) == 1:
                comparable = True
                known = [(evidence[c["comment_id"]][1], c) for c in with_evidence]
        elif len(replies) == len(bucket) and len({c.get("parent_id") for c in replies}) == 1:
            with_order = [c for c in replies if c.get("reply_order") is not None]
            if with_order:
                comparable = True
                known = [(c["reply_order"], c) for c in with_order]

        if comparable:
            known_ids = {c["comment_id"] for _, c in known}
            for _sort_key, candidate in sorted(known, key=lambda item: item[0]):
                row = dict(candidate)
                row["resolved"] = True
                ordered.append(row)
            for candidate in sorted(
                (c for c in bucket if c.get("comment_id") not in known_ids),
                key=lambda c: c.get("comment_id") or "",
            ):
                row = dict(candidate)
                row["resolved"] = False
                ordered.append(row)
            continue

        for candidate in sorted(bucket, key=lambda c: c.get("comment_id") or ""):
            row = dict(candidate)
            row["resolved"] = False
            ordered.append(row)

    for rank, row in enumerate(ordered, start=1):
        row["rank"] = rank
    return ordered


def upsert_daily_order(
    turso: TursoClient,
    groups: dict[tuple[str, str], list[dict]],
    thread_order_evidence: dict[str, tuple[int, int]] | None = None,
) -> set[tuple[str, str]]:
    """Persist the current candidates for each achieved minute in display order.

    The source window is re-read repeatedly just after a target minute and by
    the next day's aggregate job. Re-upserting all ranks makes late YouTube
    visibility and earlier-arriving comments self-correcting without scanning
    the whole comments table during a page request.
    """
    if not groups:
        return set()
    turso.execute(_ORDER_TABLE_SQL)
    now = int(time.time())
    evidence = thread_order_evidence or {}
    for (time_key, date_str), candidates in sorted(groups.items()):
        ranked = rank_order_group(candidates, evidence)
        statements = [
            {
                "sql": _ORDER_UPSERT_SQL,
                "args": [
                    time_key, date_str, row["rank"], row["comment_id"],
                    row.get("author_channel_id"), row.get("handle"), row["published_at"],
                    "thread" if row.get("parent_id") is None else "reply",
                    1 if row["resolved"] else 0, now,
                ],
            }
            for row in ranked
        ]
        for i in range(0, len(statements), _WRITE_CHUNK):
            turso.batch(statements[i:i + _WRITE_CHUNK])
    return set(groups)


def upsert_range(
    turso: TursoClient,
    dates: list[date],
    achieved: set[tuple[str, str]],
    winners: dict[tuple[str, str], dict] | None = None,
) -> None:
    """dates × TARGET_TIMES の全組み合わせをUPSERTする(achievedに無ければ0)。

    winners は {(time_key, date): {"channel_id":..., "handle":...}} 形式(省略時は勝者情報なし)。
    ranking_common.write_full()と同様、batch()をchunk単位で呼ぶ(per-statementエラーは
    検知されないため、呼び出し側でCOUNT(*)等の書き込み検証を行うこと)。
    """
    ensure_schema(turso)
    now = int(time.time())
    winners = winners or {}

    items = []
    for d in dates:
        d_str = d.isoformat()
        for time_key in TARGET_TIMES:
            key = (time_key, d_str)
            achieved_flag = 1 if key in achieved else 0
            w = winners.get(key) if achieved_flag else None
            winner_channel_id = w["channel_id"] if w else None
            winner_handle = w["handle"] if w else None
            items.append((time_key, d_str, achieved_flag, winner_channel_id, winner_handle, now))

    for i in range(0, len(items), _WRITE_CHUNK):
        chunk = items[i:i + _WRITE_CHUNK]
        turso.batch([{"sql": _UPSERT_SQL, "args": list(item)} for item in chunk])


def upsert_achieved_only(
    turso: TursoClient,
    achieved: set[tuple[str, str]],
    winners: dict[tuple[str, str], dict] | None = None,
) -> None:
    """achieved に含まれる (time_key, date) だけを achieved=1 でUPSERTする。

    upsert_range() は dates × TARGET_TIMES の全組み合わせを書き、achieved に無ければ
    0 にする設計のため、comment_sync/sync.py の毎分リアルタイムチェック(対象時刻発生
    直後の1分幅だけを見る狭いクエリ)からそのまま呼ぶと、その日の残り7つのtime_keyを
    「まだ判定していないだけ」なのに誤って achieved=0 で上書きしてしまう。

    この関数は achieved=0 を書く経路を一切持たない — time_comment_data の「今日は
    まだ行を持たない」設計(未達成のまま暫定行を書くと、対象時刻がまだ来ていない
    ユーザーに誤って「未達成」と表示してしまう)を壊さないため。渡された
    (time_key, date) の組み合わせ以外には一切触れない。
    """
    if not achieved:
        return
    ensure_schema(turso)
    now = int(time.time())
    winners = winners or {}

    items = []
    for time_key, date_str in achieved:
        w = winners.get((time_key, date_str))
        winner_channel_id = w["channel_id"] if w else None
        winner_handle = w["handle"] if w else None
        items.append((time_key, date_str, 1, winner_channel_id, winner_handle, now))

    for i in range(0, len(items), _WRITE_CHUNK):
        chunk = items[i:i + _WRITE_CHUNK]
        turso.batch([{"sql": _UPSERT_SQL, "args": list(item)} for item in chunk])
