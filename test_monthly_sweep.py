import unittest
import sqlite3
from datetime import datetime

import monthly_sweep


class SqliteTurso:
    def __init__(self):
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row

    def query(self, sql, args=None, timeout=30):
        return [dict(row) for row in self.db.execute(sql, args or []).fetchall()]

    def execute(self, sql, args=None, timeout=30):
        self.db.execute(sql, args or [])
        self.db.commit()
        return {}

    def batch(self, statements, timeout=30):
        for stmt in statements:
            self.db.execute(stmt["sql"], stmt.get("args") or [])
        self.db.commit()
        return {}


class MonthlySweepRepairTests(unittest.TestCase):
    def test_repair_work_routes_exact_date_hour_and_author(self):
        stamp = int(datetime(2026, 7, 10, 12, 34, tzinfo=monthly_sweep.JST).timestamp())
        work = monthly_sweep._repair_work([{
            "comment_id": "r1", "published_at": stamp,
            "author_channel_id": "UC123", "is_deleted": 0,
        }])
        for consumer in (
            "ranking_date", "daily_stats_date", "analytics_date",
            "wordcloud_recent7d_date", "wordcloud_recent30d_date",
        ):
            self.assertEqual(work[consumer], {"2026-07-10"})
        self.assertEqual(work["hourly_bucket"], {str((stamp // 3600) * 3600)})
        self.assertEqual(work["account_profile"], {"UC123"})
        self.assertEqual(work["network_full"], {"required"})
        self.assertEqual(work["calendar_wordcloud_full"], {"required"})


class SqlShapeTest(unittest.TestCase):
    """索引選択を左右する書き方が壊れていないことを固定する(test_ksk_common.SqlShapeTestと同じ狙い)。

    2026-08-04, 本番TursoでのEXPLAIN QUERY PLAN確認結果(MULTI-INDEX OR、
    comment_id側はsqlite_autoindex_comments_1、parent_id側はidx_parent_publishedの
    独立SEARCH)は monthly_sweep._snapshot_thread_rows のコメントに記録済み。ここでは
    その形(OR結合、author_channel_idを足さない)が将来の編集で崩れないことだけを守る。
    """

    def _captured_sql(self, thread_ids):
        captured = {}

        class _CapturingTurso:
            def query(self, sql, args=None, timeout=30):
                captured["sql"] = sql
                captured["args"] = args
                return []

        monthly_sweep._snapshot_thread_rows(_CapturingTurso(), thread_ids)
        return captured["sql"], captured["args"]

    def test_snapshot_query_ors_comment_id_and_parent_id_only(self):
        sql, args = self._captured_sql(["t1", "t2"])
        self.assertIn("comment_id IN (", sql)
        self.assertIn("OR parent_id IN (", sql)
        where_clause = sql.split("WHERE", 1)[1]
        self.assertNotIn("author_channel_id", where_clause)
        # comment_id側・parent_id側それぞれにthread_idsぶんのプレースホルダ(2件→4個)
        self.assertEqual(sql.count("?"), len(args))
        self.assertEqual(len(args), 4)


class PersistChunkRepairsTests(unittest.TestCase):
    """2026-09-04: 前後2回のチャンク全件スナップショット+差分計算を廃止し、
    touched_ids(_recheck_threadsがPass1時点で判明済みの消滅/食い違いスレッド
    IDを詰めて返す)だけを読む設計に変更した際の回帰テスト。"""

    def setUp(self):
        self.client = SqliteTurso()
        self.addCleanup(self.client.db.close)
        self.client.execute(
            "CREATE TABLE comments (comment_id TEXT PRIMARY KEY, parent_id TEXT, "
            "author_channel_id TEXT, handle TEXT, text TEXT, published_at INTEGER, "
            "is_deleted INTEGER)"
        )
        for comment_id, parent_id, author, published_at, is_deleted in (
            ("thread_a", None, "UC_a", 1000, 1),   # 消滅したスレッド
            ("reply_a1", "thread_a", "UC_x", 1005, 1),
            ("thread_b", None, "UC_b", 2000, 0),   # 食い違いで再取得されたスレッド
            ("reply_b1", "thread_b", "UC_y", 2005, 0),
            ("thread_c", None, "UC_c", 3000, 0),   # 変化していない(touched対象外)
        ):
            self.client.execute(
                "INSERT INTO comments"
                "(comment_id,parent_id,author_channel_id,published_at,is_deleted) "
                "VALUES(?,?,?,?,?)",
                [comment_id, parent_id, author, published_at, is_deleted],
            )

    def test_empty_touched_ids_skips_turso_entirely(self):
        class _ExplodingTurso:
            def query(self, *a, **k):
                raise AssertionError("touched_idsが空ならqueryを呼んではいけない")

            def batch(self, *a, **k):
                raise AssertionError("touched_idsが空ならbatchを呼んではいけない")

            def execute(self, *a, **k):
                raise AssertionError("touched_idsが空ならexecuteを呼んではいけない")

        changed, queued = monthly_sweep._persist_chunk_repairs(_ExplodingTurso(), [])

        self.assertEqual((changed, queued), (0, 0))

    def test_persist_chunk_repairs_reads_only_touched_ids_and_enqueues_work(self):
        changed, queued = monthly_sweep._persist_chunk_repairs(
            self.client, ["thread_a", "thread_b"],
        )

        # thread_c(未変化)は対象外。thread_a/bとその返信(計4行)だけが対象になる。
        self.assertEqual(changed, 4)
        self.assertGreater(queued, 0)
        rows = self.client.query(
            "SELECT DISTINCT consumer FROM aggregate_repair_queue ORDER BY consumer"
        )
        consumers = {r["consumer"] for r in rows}
        self.assertIn("daily_stats_date", consumers)
        self.assertIn("account_profile", consumers)
        accounts = {
            r["work_key"] for r in self.client.query(
                "SELECT work_key FROM aggregate_repair_queue WHERE consumer='account_profile'"
            )
        }
        self.assertEqual(accounts, {"UC_a", "UC_x", "UC_b", "UC_y"})
        self.assertNotIn("UC_c", accounts, "変化していないthread_cの著者を含めてはいけない")


class MonthlySweepResumeTests(unittest.TestCase):
    def setUp(self):
        self.client = SqliteTurso()
        self.client.execute(
            "CREATE TABLE comments (comment_id TEXT PRIMARY KEY, parent_id TEXT, published_at INTEGER)"
        )
        for comment_id, parent_id, published_at in (
            ("t2", None, 100),
            ("t1", None, 100),
            ("t3", None, 200),
            ("t4", None, 300),
            ("t5", None, 400),
            ("reply", "t1", 150),
            ("outside", None, -100000),
        ):
            self.client.execute(
                "INSERT INTO comments(comment_id,parent_id,published_at) VALUES(?,?,?)",
                [comment_id, parent_id, published_at],
            )

    def test_initial_offset_and_checkpoints_use_stable_composite_cursor(self):
        state, remaining = monthly_sweep.initialize_state(
            self.client, days=1, window_end=500, resume_offset=2,
        )
        self.assertEqual(state["processed_count"], 2)
        self.assertEqual(state["total_count"], 5)
        self.assertEqual(state["cursor_comment_id"], "t2")
        self.assertEqual([row["comment_id"] for row in remaining], ["t3", "t4", "t5"])

        monthly_sweep.checkpoint_state(self.client, state, remaining[:2])
        loaded = monthly_sweep.load_state(self.client)
        self.assertEqual(loaded["processed_count"], 4)
        self.assertEqual(loaded["cursor_comment_id"], "t4")

        resumed = monthly_sweep.fetch_threads(
            self.client,
            loaded["window_start"],
            loaded["window_end"],
            loaded["cursor_published_at"],
            loaded["cursor_comment_id"],
        )
        self.assertEqual([row["comment_id"] for row in resumed], ["t5"])

        monthly_sweep.checkpoint_state(self.client, state, resumed)
        monthly_sweep.complete_state(self.client, state)
        completed = monthly_sweep.load_state(self.client)
        self.assertEqual(completed["status"], "completed")
        self.assertEqual(completed["processed_count"], 5)

    def test_reconcile_resume_state_absorbs_late_arriving_threads(self):
        # initialize_state 経由で実物の monthly_sweep_state 行を作る
        # (total_count=5, processed_count=2 -> 残り3件のはずが、後から
        # 検知された過去投稿の分だけ4件返ってきたケースを模擬)。
        state, _ = monthly_sweep.initialize_state(
            self.client, days=1, window_end=500, resume_offset=2,
        )
        threads = [{"comment_id": f"extra{i}"} for i in range(4)]

        monthly_sweep.reconcile_resume_state(self.client, state, threads)

        self.assertEqual(state["total_count"], 6)
        persisted = monthly_sweep.load_state(self.client)
        self.assertEqual(persisted["total_count"], 6)

    def test_reconcile_resume_state_raises_when_threads_disappear(self):
        state, _ = monthly_sweep.initialize_state(
            self.client, days=1, window_end=500, resume_offset=2,
        )
        threads = [{"comment_id": "t3"}]  # 1 < 5-2=3 のはず

        with self.assertRaises(RuntimeError):
            monthly_sweep.reconcile_resume_state(self.client, state, threads)

    def test_reconcile_resume_state_is_noop_when_count_matches(self):
        state, remaining = monthly_sweep.initialize_state(
            self.client, days=1, window_end=500, resume_offset=2,
        )

        monthly_sweep.reconcile_resume_state(self.client, state, remaining)

        self.assertEqual(state["total_count"], 5)
        # UPDATEを発行していないので行自体は変化していない
        self.assertEqual(monthly_sweep.load_state(self.client)["total_count"], 5)


if __name__ == "__main__":
    unittest.main()
