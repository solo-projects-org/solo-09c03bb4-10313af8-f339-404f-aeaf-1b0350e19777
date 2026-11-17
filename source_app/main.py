"""检验源服务：返回样品号与浓度。支持运行时配置延迟、并行名额(槽位)、首次失败。
客户端超时断开后，已受理的请求仍继续执行并向编排端上报服务端跨度(迟到证据)。

名额调度(加急/常规)：
- 已开始执行的名额不中断；
- 释放名额时，加急请求可越过已排队常规请求；
- 同级严格按进入等待队列的先后顺序放行；
- 连续放行两次加急后，若已有常规请求在等待，下一名额必须留给最早的常规请求(防饿死)；
- 超时后的重试是全新请求，按所属批次级别重新入队；旧请求继续排队/执行。
"""
import asyncio
import hashlib
import os
import time
from collections import deque

import httpx
from fastapi import FastAPI, HTTPException

ORCH_URL = os.environ.get("ORCH_URL", "http://localhost:8000")

app = FastAPI(title="检验源")

CFG = {
    "delay_ms": int(os.environ.get("SOURCE_DELAY_MS", "300")),
    "slots": int(os.environ.get("SOURCE_SLOTS", "3")),
    "fail_first": os.environ.get("SOURCE_FAIL_FIRST", "0") == "1",
}
_seen = set()  # fail_first 已触发过的样品号

PRIORITY_LABEL = {"urgent": "加急", "normal": "常规"}


def normalize_priority(value) -> str:
    return "urgent" if str(value).strip().lower() in ("urgent", "加急") else "normal"


class Waiter:
    """在名额门口排队的请求：arrive_seq 是进入等待队列的先后凭据。"""

    __slots__ = ("arrive_seq", "priority", "info", "future")

    def __init__(self, arrive_seq, priority, info):
        self.arrive_seq = arrive_seq
        self.priority = priority
        self.info = info
        self.future = asyncio.get_running_loop().create_future()


class Ticket:
    """一次成功取得名额的凭据，记录实际等待与放行序号。"""

    __slots__ = ("key", "arrive_seq", "admit_seq", "priority", "admitted_at", "wait_ms")

    def __init__(self, key, arrive_seq, admit_seq, priority, admitted_at, wait_ms):
        self.key = key
        self.arrive_seq = arrive_seq
        self.admit_seq = admit_seq
        self.priority = priority
        self.admitted_at = admitted_at
        self.wait_ms = wait_ms


class SlotGate:
    """并行名额(槽位) + 加急优先的公平调度。

    所有状态只由事件循环内的锁保护，放行决定是确定性的(不靠"谁先被唤醒")。
    """

    def __init__(self, n):
        self.n = max(1, int(n))
        self.cur = 0
        self._arrive_seq = 0
        self._admit_seq = 0
        self._urgent_streak = 0      # 连续放行的加急次数
        self._waiters = []           # 全部未取得名额的请求
        self._executing = {}         # key -> 执行中记录
        self._recent = deque(maxlen=300)
        self._lock = asyncio.Lock()

    # ---------- 调度 ----------

    def _by_seq(self, items):
        return min(items, key=lambda w: w.arrive_seq)

    def _choose_locked(self):
        urgent = [w for w in self._waiters if w.priority == "urgent"]
        normal = [w for w in self._waiters if w.priority == "normal"]
        # 连续两次加急之后，常规等待者优先；否则加急优先；同级取最早进队者
        if self._urgent_streak >= 2 and normal:
            return self._by_seq(normal)
        if urgent:
            return self._by_seq(urgent)
        if normal:
            return self._by_seq(normal)
        return None

    def _pump_locked(self):
        # 等待中被取消(客户端断开)的请求不再参与放行
        self._waiters = [w for w in self._waiters if not w.future.cancelled()]
        while self.cur < self.n and self._waiters:
            w = self._choose_locked()
            self._waiters.remove(w)
            self.cur += 1
            self._admit_seq += 1
            if w.priority == "urgent":
                self._urgent_streak += 1
            else:
                self._urgent_streak = 0
            now = time.time()
            key = w.info["key"]
            ticket = Ticket(key, w.arrive_seq, self._admit_seq, w.priority, now,
                            (now - w.info["arrive_ts"]) * 1000.0)
            rec = {
                "key": key,
                "priority": w.priority,
                "batch_id": w.info["batch_id"],
                "sample_id": w.info["sample_id"],
                "attempt": w.info["attempt"],
                "request_id": w.info["request_id"],
                "arrive_seq": w.arrive_seq,
                "admit_seq": self._admit_seq,
                "admitted_at": now,
                "exec_start_ts": now,
                "wait_ms": round(ticket.wait_ms, 1),
                "end_ts": None,
                "status": "running",
            }
            self._executing[key] = rec
            self._recent.append(rec)
            w.future.set_result(ticket)

    async def acquire(self, priority, info):
        async with self._lock:
            self._arrive_seq += 1
            w = Waiter(self._arrive_seq, priority, info)
            self._waiters.append(w)
            self._pump_locked()
            if w.future.done():
                return w.future.result()
        try:
            return await w.future
        except asyncio.CancelledError:
            async with self._lock:
                if w in self._waiters:
                    self._waiters.remove(w)
                elif w.future.done() and not w.future.cancelled():
                    # 刚被放行调用方就被取消：把名额还回去，不丢槽位
                    self._release_locked(w.future.result(), "cancelled")
            raise

    def _release_locked(self, ticket, status):
        rec = self._executing.pop(ticket.key, None)
        if rec is not None:
            rec["end_ts"] = time.time()
            rec["status"] = status
        self.cur = max(0, self.cur - 1)
        self._pump_locked()

    async def release(self, ticket, status="ok"):
        async with self._lock:
            self._release_locked(ticket, status)

    async def set_slots(self, n):
        async with self._lock:
            self.n = max(1, int(n))
            self._pump_locked()

    # ---------- 观测 ----------

    def snapshot(self):
        now = time.time()
        waiting = sorted(self._waiters, key=lambda w: w.arrive_seq)
        executing = sorted(self._executing.values(), key=lambda r: r["admit_seq"])
        return {
            "slots": self.n,
            "running": self.cur,
            "urgent_streak": self._urgent_streak,
            "policy": "加急可越过排队常规；同级先进先出；连续放行2次加急后若有常规等待，"
                      "下一名额留给最早常规；已执行不中断",
            "waiting": [{
                "arrive_seq": w.arrive_seq, "priority": w.priority,
                "priority_label": PRIORITY_LABEL[w.priority],
                "batch_id": w.info["batch_id"], "sample_id": w.info["sample_id"],
                "attempt": w.info["attempt"], "request_id": w.info["request_id"],
                "wait_ms": round((now - w.info["arrive_ts"]) * 1000.0, 1),
            } for w in waiting],
            "executing": [{
                "admit_seq": r["admit_seq"], "arrive_seq": r["arrive_seq"],
                "priority": r["priority"], "priority_label": PRIORITY_LABEL[r["priority"]],
                "batch_id": r["batch_id"], "sample_id": r["sample_id"],
                "attempt": r["attempt"], "request_id": r["request_id"],
                "wait_ms": r["wait_ms"],
                "running_ms": round((now - r["exec_start_ts"]) * 1000.0, 1),
            } for r in executing],
            "recent": [{
                "admit_seq": r["admit_seq"], "arrive_seq": r["arrive_seq"],
                "priority": r["priority"], "priority_label": PRIORITY_LABEL[r["priority"]],
                "batch_id": r["batch_id"], "sample_id": r["sample_id"],
                "attempt": r["attempt"], "request_id": r["request_id"],
                "wait_ms": r["wait_ms"], "status": r["status"],
                "exec_start_ts": r["exec_start_ts"], "end_ts": r["end_ts"],
            } for r in self._recent],
        }


gate = SlotGate(CFG["slots"])


def concentration_for(sid: str) -> float:
    h = int(hashlib.md5(sid.encode()).hexdigest()[:8], 16)
    val = round((h % 1000) / 100.0, 2)  # 0.00 .. 9.99
    if "NEG" in sid:  # 演示钩子：负浓度触发质控①不通过
        val = -(val + 0.5)
    return val


_report_client = None


async def report(span):
    """向编排端上报服务端跨度；编排端不可达不影响源站自身工作。"""
    global _report_client
    try:
        if _report_client is None:
            _report_client = httpx.AsyncClient(timeout=1.5)
        await _report_client.post(f"{ORCH_URL}/api/ingest", json=span)
    except Exception:
        pass


class InjectedFailure(Exception):
    pass


async def do_work(sample_id, request_id, batch_id, attempt, priority):
    start = time.time()
    delay_ms = CFG["delay_ms"]   # 按请求受理时的配置执行，避免运行中被改配置导致记录失真
    slots = gate.n
    key = request_id or f"anon-{start:.6f}-{sample_id}"
    info = {"key": key, "arrive_ts": start, "batch_id": batch_id,
            "sample_id": sample_id, "attempt": attempt, "request_id": request_id}
    base = {"batch_id": batch_id, "sample_id": sample_id, "stage": "fetch",
            "service": "source", "attempt": attempt, "request_id": request_id,
            "priority": priority, "start_ts": start}
    # 进队即上报：编排端能实时看到"谁在等、等了多久"
    await report({**base, "status": "running", "queue_ms": 0,
                  "detail": {"state": "waiting", "arrival_ts": start, "delay_ms": delay_ms,
                             "slots": slots}})
    info["arrive_ts"] = time.time()   # 排队时长从真正进队时刻算起
    ticket = await gate.acquire(priority, info)
    queue_ms = ticket.wait_ms
    # 放行即上报：到达序/放行序/实际开始执行时刻
    await report({**base, "status": "running", "queue_ms": round(queue_ms, 1),
                  "arrive_seq": ticket.arrive_seq, "admit_seq": ticket.admit_seq,
                  "exec_start_ts": ticket.admitted_at,
                  "detail": {"state": "executing", "delay_ms": delay_ms, "slots": slots,
                             "priority": priority, "admit_seq": ticket.admit_seq}})
    status, payload, err = "ok", None, None
    release_status = "ok"
    try:
        await asyncio.sleep(delay_ms / 1000.0)
        if CFG["fail_first"] and sample_id not in _seen:
            _seen.add(sample_id)
            status, err = "failed", "injected first failure (fail_first)"
            release_status = "failed"
        else:
            actual = sample_id + "-X" if "MIX" in sample_id else sample_id  # 演示钩子：错位样品号
            payload = {"sample_id": actual, "concentration": concentration_for(sample_id)}
    except asyncio.CancelledError:
        release_status = "cancelled"
        raise
    except Exception:
        release_status = "failed"
        raise
    finally:
        await gate.release(ticket, release_status)
    end = time.time()
    await report({**base, "status": status, "end_ts": end,
                  "queue_ms": round(queue_ms, 1),
                  "arrive_seq": ticket.arrive_seq, "admit_seq": ticket.admit_seq,
                  "exec_start_ts": ticket.admitted_at,
                  "detail": {"state": "finished", "delay_ms": delay_ms, "slots": slots,
                             "priority": priority, "admit_seq": ticket.admit_seq,
                             **({"error": err} if err else {})}})
    if err:
        raise InjectedFailure(err)
    return payload


@app.get("/fetch")
async def fetch(sample_id: str, request_id: str = "", batch_id: int = 0, attempt: int = 1,
                priority: str = "normal"):
    priority = normalize_priority(priority)
    task = asyncio.create_task(do_work(sample_id, request_id, batch_id, attempt, priority))
    try:
        return await asyncio.shield(task)
    except InjectedFailure as e:
        raise HTTPException(status_code=500, detail=str(e))
    except asyncio.CancelledError:
        # 客户端已放弃：任务继续跑完并上报跨度，结果成为"迟到结果"
        task.add_done_callback(lambda t: t.exception() if not t.cancelled() else None)
        raise


@app.get("/queue")
async def queue():
    """实时名额/队列快照：等待中、执行中与最近放行顺序(供诊断网页展示)。"""
    return gate.snapshot()


@app.post("/config")
async def set_config(body: dict):
    if "delay_ms" in body:
        CFG["delay_ms"] = max(0, int(body["delay_ms"]))
    if "slots" in body:
        CFG["slots"] = max(1, int(body["slots"]))
        await gate.set_slots(CFG["slots"])
    if "fail_first" in body:
        CFG["fail_first"] = bool(body["fail_first"])
    _seen.clear()
    return CFG


@app.get("/config")
async def get_config():
    return CFG


@app.get("/health")
async def health():
    return {"ok": True}
