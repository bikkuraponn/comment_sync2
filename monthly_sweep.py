"""
月次スイープ。直近1ヶ月ぶんのスレッドを一気に Pass1/Pass2 で確認し、整合性を取る。

sync.py の巡回チェックには「同じスレッドで同じチェック間隔内に1件削除＋1件追加が
起きると totalReplyCount が一致して見逃す」という原理的な弱点がある
(件数比較でしか差分を検知していないため)。コールド層が全履歴を約20日で
一周するのでいずれ拾い直せるが、そのタイミングでも同じ数だった場合は
すり抜け続ける。ここで月1回まとめて見直すことでその取りこぼしを回収する。

sync.py の Pass1/Pass2 をそのまま再利用するので、コストは
  スレッド数 ÷ RECHECK_ID_CHUNK + 食い違い分
で済む。直近1ヶ月 ≒ 57,600スレッドなら Pass1 は約1,152 units。
1スレッド=1 unit でフル再取得すると57,600 units(1プロジェクトの5.7日分)に
なって成立しないので、この方式であることが前提。

毎分動く sync.py とは別ワークフロー・別 event_type にしてある。
同じジョブに相乗りさせると、その分の実行時間ぶん毎分同期が止まるため。

実行:
  python monthly_sweep.py                # 直近31日、保存済み状態があれば自動再開
  python monthly_sweep.py --days 60      # 範囲を変える(新規開始時のみ意味を持つ)
  python monthly_sweep.py --dry-run      # Pass1 の対象件数と概算コストだけ出す
  python monthly_sweep.py --restart      # 保存済み状態を破棄して新しい固定窓で開始

2026-09-04: 状態管理を「対象期間の総件数を事前に数えて位置No.で追跡する」方式から
「cursor(published_at, comment_id)だけで前に進む」方式に作り替えた。旧方式は
新規開始・再開のたびに対象期間の全件リスト(--restartのたびに数十万行)を読む
必要があり、resumeの安全チェックも「前回保存した残り件数と今の実測件数が
完全一致するか」を要求していたため、毎分動く本体同期が後から検知する
新着スレッド(YouTube側の審査保留等で数分〜数日遅れて表示されるケースがある)
だけで簡単に破綻していた。cursor方式ならその手の変動を気にする必要がそもそも
無く、1回の読み取りコストも常にCHUNK_THREADS件に固定される。

環境変数: sync.py と同じ
"""

import argparse
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from turso_client import TursoClient
import repair_queue
import sync as sync_module
from sync import (
    RECHECK_ID_CHUNK,
    _recheck_threads,
)

MONTHLY_API_KEY = os.getenv("API_KEY_FOR_MONTHLY_SWEEP", "").strip()
DEFAULT_DAYS = 31

# GitHub Actions の timeout-minutes より短く切り上げ、途中で強制終了されて
# ログや進捗を失うのを避ける。打ち切っても状態テーブル(window/cursor)に
# 進捗を保存してから終了するので、次回はそこから再開できる。
# 2026-09-03: 実測で全120,621件の完走に約209分かかることが判明し、旧値110分では
# 単発で完走しきれなかったため引き上げた。2026-09-04のcursor方式化により、
# 単発で完走できなくても次回resumeのコストがほぼゼロになったので、この値自体の
# 重要性は下がっている(完走できなければ複数回に分けて進めればよいだけ)。
TIME_BUDGET_SEC = 300 * 60

# 1回の fetch_threads / _recheck_threads に渡すスレッド数。全件を一度に渡すと
# Pass1 が全部終わるまで1件も書き込まれず、タイムアウト時に成果がゼロになる。
# 2026-09-04: fetch_threads() 側のLIMITにもそのまま使う(後述)。
CHUNK_THREADS = 5000
SNAPSHOT_IN_CHUNK = 200
JST = ZoneInfo("Asia/Tokyo")
STATE_KEY = "monthly"
INCOMPLETE_EXIT_CODE = 2

# processed_count/total_count は2026-09-04以降どちらも同じ値(累計処理件数)を
# 書き込むだけの表示用フィールドで、正しさの判定には使わない
# (total_count列はNOT NULL制約があるためスキーマ変更を避けて残してある)。
STATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS monthly_sweep_state (
  state_key TEXT PRIMARY KEY,
  run_month TEXT NOT NULL,
  window_start INTEGER NOT NULL,
  window_end INTEGER NOT NULL,
  cursor_published_at INTEGER,
  cursor_comment_id TEXT,
  processed_count INTEGER NOT NULL,
  total_count INTEGER NOT NULL,
  status TEXT NOT NULL,
  updated_at INTEGER NOT NULL
)
"""


def _snapshot_thread_rows(client: TursoClient, thread_ids: list[str]) -> dict[str, dict]:
    """Read the rows whose active membership can change during Pass 2."""
    snapshot: dict[str, dict] = {}
    for offset in range(0, len(thread_ids), SNAPSHOT_IN_CHUNK):
        chunk = thread_ids[offset:offset + SNAPSHOT_IN_CHUNK]
        marks = ",".join("?" for _ in chunk)
        # 2026-08-04, EXPLAIN QUERY PLAN確認済み(200件chunk): SQLiteのOR最適化により
        # comment_id側はsqlite_autoindex_comments_1、parent_id側はidx_parent_publishedの
        # 独立したSEARCHに分解される(MULTI-INDEX OR)。フルSCANにはならない。
        rows = client.query(
            "SELECT comment_id,parent_id,author_channel_id,handle,text,published_at,is_deleted "
            f"FROM comments WHERE comment_id IN ({marks}) OR parent_id IN ({marks})",
            [*chunk, *chunk],
        )
        for row in rows:
            snapshot[row["comment_id"]] = row
    return snapshot


def _repair_work(rows: list[dict]) -> dict[str, set[str]]:
    work: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        stamp = row.get("published_at")
        if stamp is None:
            continue
        dt = datetime.fromtimestamp(int(stamp), JST)
        day = dt.date().isoformat()
        hour_start = (int(stamp) // 3600) * 3600
        for consumer in (
            "ranking_date", "daily_stats_date", "analytics_date",
            "wordcloud_recent7d_date", "wordcloud_recent30d_date",
        ):
            work[consumer].add(day)
        work["hourly_bucket"].add(str(hour_start))
        author_id = row.get("author_channel_id")
        if author_id:
            work["account_profile"].add(str(author_id))
    if rows:
        work["network_full"].add("required")
        work["calendar_wordcloud_full"].add("required")
    return work


def _persist_chunk_repairs(
    client: TursoClient, touched_ids: list[str],
) -> tuple[int, int]:
    """touched_ids(消滅/食い違いと判定されたスレッドID)に絞って現在の行を読み、
    そのまま repair_queue へ登録する。

    2026-09-04: 以前はチャンク全件(最大5000スレッド+その返信全部)を処理の前後で
    2回スナップショットして差分を取っていたが、実際に行が変化しうるのは
    touched_ids(実測でチャンクの約1割)に限られる。_recheck_threads() が
    Pass1の時点で判明済みのdead_ids/mismatchedをそのままtouched_idsへ詰めて
    返す(追加のTurso読み取り無し)ので、このID集合だけを1回読めば済み、
    以前の"before"スナップショットや差分計算(_changed_rows)自体が不要になる
    (このIDに紐づく行はこのチャンクの処理で変化した/変化し得るものだけなので、
    読めた行=変化した行とみなしてよい)。
    """
    if not touched_ids:
        return 0, 0
    changed = list(_snapshot_thread_rows(client, touched_ids).values())
    work = _repair_work(changed)
    queued = repair_queue.enqueue(client, work) if work else 0
    return len(changed), queued


def ensure_state_table(client: TursoClient) -> None:
    client.execute(STATE_TABLE_SQL)


def load_state(client: TursoClient) -> dict | None:
    ensure_state_table(client)
    rows = client.query(
        "SELECT state_key,run_month,window_start,window_end,cursor_published_at,"
        "cursor_comment_id,processed_count,total_count,status,updated_at "
        "FROM monthly_sweep_state WHERE state_key=?",
        [STATE_KEY],
    )
    return rows[0] if rows else None


def fetch_threads(
    client: TursoClient,
    window_start: int,
    window_end: int,
    cursor_published_at: int | None = None,
    cursor_comment_id: str | None = None,
    limit: int | None = None,
) -> list[dict]:
    """固定窓内のスレッドを、cursorより後ろから古い順に返す。

    limit: 渡された場合はSQL側のLIMITで打ち切る(2026-09-04追加)。メインループは
    常にCHUNK_THREADSを渡し、1回の読み取りコストをチャンクサイズに固定する。
    以前はlimit無しで「cursorから窓の終端まで全部」を毎回読んでおり、
    --restart/resumeのたびに数十万行を読み直す原因になっていた。
    --dry-runのプレビューだけは、正確な全体件数を出すため引き続きlimit無しで呼ぶ
    (人間が明示的に1回だけ実行する操作であり、自動リトライの対象ではないため)。
    """
    sql = (
        "SELECT comment_id, published_at FROM comments "
        "WHERE parent_id IS NULL AND published_at >= ? AND published_at <= ?"
    )
    args: list = [window_start, window_end]
    if cursor_published_at is not None and cursor_comment_id is not None:
        sql += " AND (published_at > ? OR (published_at = ? AND comment_id > ?))"
        args.extend([cursor_published_at, cursor_published_at, cursor_comment_id])
    sql += " ORDER BY published_at ASC, comment_id ASC"
    if limit is not None:
        sql += " LIMIT ?"
        args.append(limit)
    return client.query(sql, args, timeout=120)


def start_fresh_state(
    client: TursoClient,
    days: int,
    window_end: int,
    resume_from_published_at: int | None,
    resume_from_comment_id: str | None,
) -> dict:
    """新しい固定窓で状態を作る。対象期間の全件リストは事前に読まない。

    2026-09-04: 以前はここでfetch_threads()を無条件に呼び、対象期間の全件を
    総件数計算のために毎回読んでいた。cursor方式では総件数を事前に知る必要が
    無いため、この読み取り自体を廃止した。resume_from_*を指定しない限り
    cursor=Noneで開始し、最初のチャンク取得(fetch_threads内のLIMIT付きクエリ)が
    自然に窓の先頭から始まる。

    resume_from_published_at/resume_from_comment_idは、状態テーブル自体が
    書き換わってしまった場合の手動復旧専用(通常運用では使わない — 保存済み
    状態からの自動再開で十分)。過去の進捗ログに出力されたcursor値をそのまま
    渡す想定で、値の妥当性はここでは検証しない。
    """
    window_start = window_end - days * 86400
    now_epoch = int(datetime.now(timezone.utc).timestamp())
    run_month = datetime.fromtimestamp(window_end, JST).strftime("%Y-%m")
    state = {
        "state_key": STATE_KEY,
        "run_month": run_month,
        "window_start": window_start,
        "window_end": window_end,
        "cursor_published_at": resume_from_published_at,
        "cursor_comment_id": resume_from_comment_id,
        "processed_count": 0,
        "status": "in_progress",
        "updated_at": now_epoch,
    }
    ensure_state_table(client)
    client.execute(
        "INSERT INTO monthly_sweep_state "
        "(state_key,run_month,window_start,window_end,cursor_published_at,"
        "cursor_comment_id,processed_count,total_count,status,updated_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?) "
        "ON CONFLICT(state_key) DO UPDATE SET "
        "run_month=excluded.run_month,window_start=excluded.window_start,"
        "window_end=excluded.window_end,"
        "cursor_published_at=excluded.cursor_published_at,"
        "cursor_comment_id=excluded.cursor_comment_id,"
        "processed_count=excluded.processed_count,total_count=excluded.total_count,"
        "status=excluded.status,updated_at=excluded.updated_at",
        [
            STATE_KEY, run_month, window_start, window_end,
            resume_from_published_at, resume_from_comment_id,
            0, 0, "in_progress", now_epoch,
        ],
    )
    return state


def checkpoint_state(client: TursoClient, state: dict) -> None:
    """state辞書に既に反映済みのcursor/processed_countをTursoへ書き戻す。

    呼び出し側が state["cursor_published_at"]/["cursor_comment_id"]/
    ["processed_count"] を更新してから呼ぶこと。total_countは
    processed_countと同じ値を書く(表示用の名残、正しさの判定には使わない)。
    """
    state["updated_at"] = int(datetime.now(timezone.utc).timestamp())
    client.execute(
        "UPDATE monthly_sweep_state SET cursor_published_at=?,cursor_comment_id=?,"
        "processed_count=?,total_count=?,updated_at=? WHERE state_key=? AND status='in_progress'",
        [
            state["cursor_published_at"], state["cursor_comment_id"],
            state["processed_count"], state["processed_count"],
            state["updated_at"], STATE_KEY,
        ],
    )


def complete_state(client: TursoClient, state: dict) -> None:
    state["status"] = "completed"
    state["updated_at"] = int(datetime.now(timezone.utc).timestamp())
    client.execute(
        "UPDATE monthly_sweep_state SET total_count=processed_count,status='completed',"
        "updated_at=? WHERE state_key=?",
        [state["updated_at"], STATE_KEY],
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS,
                        help=f"遡る日数 (既定: {DEFAULT_DAYS}、新規開始時のみ意味を持つ)")
    parser.add_argument("--window-end", type=int,
                        help="固定スキャン窓の終了epoch秒(新規開始時のみ意味を持つ)")
    parser.add_argument("--resume-from-published-at", type=int,
                        help="手動復旧用: このpublished_at/comment_idの行より後ろから"
                             "開始する(--resume-from-comment-idと併用必須、--restart時のみ)")
    parser.add_argument("--resume-from-comment-id", type=str,
                        help="手動復旧用: --resume-from-published-atと同じ行のcomment_id"
                             "(同着タイの向きを決める複合カーソル)")
    parser.add_argument("--restart", action="store_true",
                        help="既存状態を破棄して新しい固定窓を開始する")
    parser.add_argument("--dry-run", action="store_true",
                        help="対象件数と概算コストだけ出して終了する")
    args = parser.parse_args()

    if (args.resume_from_published_at is None) != (args.resume_from_comment_id is None):
        print("ERROR: --resume-from-published-at と --resume-from-comment-id は両方指定してください")
        sys.exit(1)

    if not MONTHLY_API_KEY:
        print("ERROR: API_KEY_FOR_MONTHLY_SWEEP を設定してください")
        sys.exit(1)
    sync_module.configure_api_keys([MONTHLY_API_KEY], allow_rotation=False)

    url = os.getenv("TURSO_URL")
    token = os.getenv("TURSO_AUTH_TOKEN")
    if not url or not token:
        print("ERROR: TURSO_URL と TURSO_AUTH_TOKEN を設定してください")
        sys.exit(1)

    client = TursoClient(url, token)
    started = time.monotonic()
    requested_end = args.window_end or int(datetime.now(timezone.utc).timestamp())
    requested_start = requested_end - args.days * 86400
    requested_month = datetime.fromtimestamp(requested_end, JST).strftime("%Y-%m")

    print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC] "
          f"月次スイープ開始（直近{args.days}日）", flush=True)

    if args.dry_run:
        preview = fetch_threads(client, requested_start, requested_end)
        pass1_units = -(-len(preview) // RECHECK_ID_CHUNK)
        print(
            f"  固定窓: {requested_start}..{requested_end} / "
            f"全{len(preview)}件 / Pass1概算{pass1_units} units",
            flush=True,
        )
        print("  --dry-run のため API は叩かずに終了", flush=True)
        return

    state = load_state(client)
    if (
        state and state["status"] == "completed"
        and state["run_month"] == requested_month and not args.restart
    ):
        print(
            f"  {requested_month} の月次スイープは既に完了済み "
            f"({state['processed_count']}件処理)",
            flush=True,
        )
        return

    if state and state["status"] == "in_progress" and not args.restart:
        window_start = int(state["window_start"])
        window_end = int(state["window_end"])
        print(
            f"  再開: {state['processed_count']}件処理済み / "
            f"固定窓 {window_start}..{window_end} / "
            f"カーソル {state.get('cursor_comment_id') or '(先頭)'}",
            flush=True,
        )
    else:
        window_start = requested_start
        window_end = requested_end
        state = start_fresh_state(
            client, args.days, requested_end,
            args.resume_from_published_at, args.resume_from_comment_id,
        )
        print(
            f"  新規状態: 固定窓 {window_start}..{window_end}"
            + (f" / カーソル{state['cursor_comment_id']}から開始"
               if state["cursor_comment_id"] else ""),
            flush=True,
        )

    youtube = sync_module.get_youtube()
    total_written = total_dead = total_mismatch = 0
    changed_count = queued_count = 0
    incomplete_reason = None
    exhausted_window = False

    while True:
        elapsed = time.monotonic() - started
        if elapsed > TIME_BUDGET_SEC:
            incomplete_reason = f"時間予算 {TIME_BUDGET_SEC}s 超過"
            break

        chunk = fetch_threads(
            client, window_start, window_end,
            state.get("cursor_published_at"), state.get("cursor_comment_id"),
            limit=CHUNK_THREADS,
        )
        if not chunk:
            exhausted_window = True
            break

        # pass2_cap は指定しない(無制限) — 月次スイープは TIME_BUDGET_SEC +
        # チャンク分割で実行時間を管理しており、Pass2 を人為的に絞ると
        # 「毎月一括で確実に整合性を取る」という役割そのものが弱まるため。
        #
        # touched_ids は _recheck_threads が Pass1 完了時点(Pass2の書き込み・
        # 例外より前)で追記するため、途中で例外が飛んでも「それより前に
        # 判明した分」は失われない(_recheck_threadsのdocstring参照)。
        touched_ids: list[str] = []
        try:
            written, dead, mismatch, aborted, _deferred = _recheck_threads(
                client, youtube, chunk, "月次スイープ", touched_ids=touched_ids,
            )
        except Exception:
            # Writes may already have succeeded. Persist their repair work
            # before propagating the failure so aggregates cannot drift.
            changed, queued = _persist_chunk_repairs(client, touched_ids)
            changed_count += changed
            queued_count += queued
            raise
        total_written += written
        total_dead += dead
        total_mismatch += mismatch
        changed, queued = _persist_chunk_repairs(client, touched_ids)
        changed_count += changed
        queued_count += queued

        if aborted:
            incomplete_reason = "クォータ枯渇またはAPIレスポンス切り詰め"
            print(
                f"  チャンク未完了のためカーソル維持: {state['processed_count']}件処理済み",
                flush=True,
            )
            break

        state["cursor_published_at"] = int(chunk[-1]["published_at"])
        state["cursor_comment_id"] = str(chunk[-1]["comment_id"])
        state["processed_count"] += len(chunk)
        checkpoint_state(client, state)
        print(
            f"  進捗 {state['processed_count']}件処理: "
            f"食い違い{mismatch}件、返信{written}件書込み、消滅{dead}件",
            flush=True,
        )

        if len(chunk) < CHUNK_THREADS:
            exhausted_window = True
            break

    print(f"  集計修復キュー: 変更行{changed_count}件 / work {queued_count}件", flush=True)
    if not exhausted_window:
        print(
            f"月次スイープ未完了: {state['processed_count']}件処理済み / "
            f"理由={incomplete_reason} / 次回は保存カーソルから自動再開 / "
            f"所要 {time.monotonic() - started:.0f}s",
            flush=True,
        )
        raise SystemExit(INCOMPLETE_EXIT_CODE)

    complete_state(client, state)
    print(
        f"月次スイープ完了: 累計{state['processed_count']}件確認、"
        f"食い違い{total_mismatch}件、返信{total_written}件書込み、"
        f"消滅{total_dead}件、所要 {time.monotonic() - started:.0f}s",
        flush=True,
    )


if __name__ == "__main__":
    main()
