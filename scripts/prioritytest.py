#!/usr/bin/env python3
"""加急/常规名额调度自测。

用法:
  python scripts/prioritytest.py --unit           仅跑调度单元测试(不需服务)
  python scripts/prioritytest.py [BASE]           单元 + 端到端(需三服务,默认 http://localhost:8000)

单元测试直接实例化源站的 SlotGate，确定性地验证：
  已执行不中断、加急越过排队常规、同级先进先出、连续两次加急后留位常规、取消不丢名额。
端到端验证：真实三服务下的执行顺序、批次级别、重试按批级别入队、迟到不重复入仓。
"""
import asyncio
import json
import os
import sys
import time

import httpx

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FAILS = []


def check(name, cond, extra=""):
    print(("PASS " if cond else "FAIL ") + name + (f"  [{extra}]" if extra else ""))
    if not cond:
        FAILS.append(name)


# ---------------- 单元：SlotGate ----------------

def unit_tests():
    sys.path.insert(0, os.path.join(ROOT, "source_app"))
    from main import SlotGate  # noqa: E402

    async def scenario():
        gate = SlotGate(1)
        admitted = []

        def info(name):
            return {"key": name, "arrive_ts": time.time(), "batch_id": 0,
                    "sample_id": name, "attempt": 1, "request_id": name}

        async def worker(name, priority, first=False):
            t = await gate.acquire(priority, info(name))
            admitted.append((name, priority, t.admit_seq))
            if first:
                # 占住唯一名额，等后续请求全部进队
                await asyncio.sleep(0.15)
            await gate.release(t)

        # 1 个已在执行 + 3 常规排队 + 3 加急排队
        tasks = [asyncio.create_task(worker("N0", "normal", first=True))]
        await asyncio.sleep(0.02)
        tasks += [asyncio.create_task(worker(n, "normal")) for n in ("N1", "N2", "N3")]
        await asyncio.sleep(0.02)
        tasks += [asyncio.create_task(worker(u, "urgent")) for u in ("U1", "U2", "U3")]
        await asyncio.sleep(0.02)
        snap = gate.snapshot()
        await asyncio.gather(*tasks)
        return admitted, snap

    admitted, snap = asyncio.run(scenario())
    order = [a[0] for a in admitted]
    # 期望:已执行的 N0 不被打断 -> U1,U2 -> 留位常规 N1 -> U3 -> N2,N3
    check("单元:已执行请求不中断且仍第一位", order and order[0] == "N0", str(order))
    check("单元:加急越过排队常规(前两名为加急)", order[1:3] == ["U1", "U2"], str(order))
    check("单元:连续两次加急后下一名额留给最早常规", order[3:4] == ["N1"], str(order))
    check("单元:常规留位后加急继续放行", order[4:5] == ["U3"], str(order))
    check("单元:同级按进队先后(常规)", order[5:] == ["N2", "N3"], str(order))
    check("单元:放行序号全局递增", [a[2] for a in admitted] == list(range(1, len(admitted) + 1)),
          str([a[2] for a in admitted]))
    check("单元:队列快照含等待与执行", isinstance(snap.get("waiting"), list)
          and isinstance(snap.get("executing"), list))

    async def cancel_case():
        gate = SlotGate(1)
        blocker = await gate.acquire("normal", {"key": "B", "arrive_ts": time.time(),
                                                "batch_id": 0, "sample_id": "B",
                                                "attempt": 1, "request_id": "B"})
        task = asyncio.create_task(gate.acquire("urgent", {"key": "X", "arrive_ts": time.time(),
                                                           "batch_id": 0, "sample_id": "X",
                                                           "attempt": 1, "request_id": "X"}))
        await asyncio.sleep(0.02)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        await gate.release(blocker)
        t = await gate.acquire("normal", {"key": "Y", "arrive_ts": time.time(),
                                          "batch_id": 0, "sample_id": "Y",
                                          "attempt": 1, "request_id": "Y"})
        snap = gate.snapshot()
        await gate.release(t)
        return snap

    snap = asyncio.run(cancel_case())
    check("单元:取消的等待请求不占名额、不留幽灵", snap["running"] == 1
          and not any(w["request_id"] == "X" for w in snap["waiting"]), json.dumps(snap)[:160])


# ---------------- 端到端 ----------------

def set_config(c, **kw):
    r = c.post(f"{BASE}/api/config", json=kw, timeout=10)
    r.raise_for_status()
    j = r.json()
    assert j["push"]["source"] == "ok" and j["push"]["qc"] == "ok", f"配置下发失败: {j['push']}"
    return j["config"]


def submit(c, ids, priority):
    r = c.post(f"{BASE}/api/batches", json={"sample_ids": ids, "priority": priority}, timeout=10)
    r.raise_for_status()
    j = r.json()
    assert j.get("priority") == priority, j
    return j["id"]


def wait_batch(c, bid, timeout=90):
    t0 = time.time()
    while time.time() - t0 < timeout:
        d = c.get(f"{BASE}/api/batches/{bid}", timeout=10).json()
        if d["batch"]["status"] != "running":
            return d
        time.sleep(0.1)
    raise TimeoutError(f"batch {bid} still running")


def wait_queue(c, pred, timeout=15):
    t0 = time.time()
    last = {}
    while time.time() - t0 < timeout:
        last = c.get(f"{BASE}/api/queue", timeout=5).json()
        if "error" not in last and pred(last):
            return last
        time.sleep(0.05)
    raise TimeoutError(f"queue condition not met: {json.dumps(last, ensure_ascii=False)[:300]}")


def fetch_server_spans(d):
    return [s for s in d["spans"] if s["side"] == "server" and s["stage"] == "fetch"]


def e2e_mixed(c):
    """加急/常规交错：名额=1，先排常规，再提交加急，验证越过/留位/同级 FIFO/不中断。"""
    set_config(c, source_delay_ms=1000, source_slots=1, source_fail_first=False,
               qc_nonneg_delay_ms=50, qc_match_delay_ms=50,
               client_timeout_ms=15000, max_attempts=2)
    n_ids = [f"P-N{i}" for i in range(5)]
    u_ids = [f"P-U{i}" for i in range(3)]
    bn = submit(c, n_ids, "normal")
    # 等到常规请求确实在排队(已在执行的 1 个 + 至少 4 个等待)
    wait_queue(c, lambda q: len(q["waiting"]) >= 4)
    urgent_submit_ts = time.time()
    bu = submit(c, u_ids, "urgent")
    wait_queue(c, lambda q: len(q["waiting"]) >= 6)
    snap = c.get(f"{BASE}/api/queue").json()
    waiting_n = [w for w in snap["waiting"] if w["batch_id"] == bn]
    waiting_u = [w for w in snap["waiting"] if w["batch_id"] == bu]
    check("交错:加急请求进入同一等待队列", len(waiting_u) >= 2, f"{len(waiting_u)} 个")
    check("交错:常规仍在排队(未被丢弃)", len(waiting_n) >= 3, f"{len(waiting_n)} 个")

    dn = wait_batch(c, bn)
    du = wait_batch(c, bu)
    check("交错:常规批次完成", dn["batch"]["status"] == "done", dn["batch"]["status"])
    check("交错:加急批次完成", du["batch"]["status"] == "done", du["batch"]["status"])
    check("交错:常规全部入仓", len(dn["warehouse"]) == 5, f"{len(dn['warehouse'])}/5")
    check("交错:加急全部入仓", len(du["warehouse"]) == 3, f"{len(du['warehouse'])}/3")

    spans = sorted(fetch_server_spans(dn) + fetch_server_spans(du),
                   key=lambda s: (s["admit_seq"], s["start_ts"]))
    pat = "".join("U" if s["priority"] == "urgent" else "N" for s in spans)
    check("交错:执行顺序符合加急优先+连二留一",
          pat == "NUUNUNNN", f"实际 {pat}(共 {len(spans)} 次)")
    check("交错:所有请求均有到达/放行序", all(s["arrive_seq"] and s["admit_seq"] for s in spans))
    # 已执行不中断：加急提交前就在执行的常规请求，其执行时长远超加急等待，且正常完成
    first = spans[0]
    check("交错:已执行的常规请求未被加急打断", first["priority"] == "normal"
          and (first["end_ts"] - first["exec_start_ts"]) >= 0.95
          and first["end_ts"] > urgent_submit_ts,
          f"{first['sample_id']} 时长 {first['end_ts'] - first['exec_start_ts']:.2f}s")
    # 越过判定：至少一个加急的到达序晚于某个常规，却先被放行
    overtaken = any(
        n["arrive_seq"] < u["arrive_seq"] and n["admit_seq"] > u["admit_seq"]
        for u in spans if u["priority"] == "urgent"
        for n in spans if n["priority"] == "normal")
    check("交错:记录到加急越过已排队常规", overtaken)
    check("交错:结论含执行顺序与越过次数",
          bool(du["conclusion"]["exec_order"]) and len(du["conclusion"]["overtakes"]) >= 1,
          json.dumps(du["conclusion"].get("overtakes"), ensure_ascii=False)[:160])
    # 批次级别展示与结果
    check("交错:批次列表/详情带级别", du["batch"]["priority"] == "urgent"
          and du["conclusion"]["priority_label"] == "加急")
    # 加急批次整体先于常规批次完成
    check("交错:加急批次先完成", du["batch"]["finished_at"] < dn["batch"]["finished_at"],
          f"加急 {du['batch']['finished_at']:.2f} / 常规 {dn['batch']['finished_at']:.2f}")


def e2e_retry_priority(c):
    """超时重试按批次级别重新入队；旧请求继续执行；迟到不重复入仓。"""
    set_config(c, source_delay_ms=1600, source_slots=1, source_fail_first=False,
               qc_nonneg_delay_ms=30, qc_match_delay_ms=30,
               client_timeout_ms=500, max_attempts=3)
    ids = ["R-U0", "R-U1"]
    b = submit(c, ids, "urgent")
    d = wait_batch(c, b, timeout=120)
    check("重试:加急批次最终完成", d["batch"]["status"] == "done", d["batch"]["status"])
    check("重试:迟到不重复入仓", len(d["warehouse"]) == len(ids)
          and len({w["sample_id"] for w in d["warehouse"]}) == len(ids),
          f"{len(d['warehouse'])} 条")
    fetch_client = [s for s in d["spans"] if s["side"] == "client" and s["stage"] == "fetch"]
    retries = [s for s in fetch_client if s["attempt"] > 1]
    check("重试:超时后产生了新尝试", len(retries) >= 2, f"{len(retries)} 次")
    check("重试:新尝试按本批级别(加急)入队",
          all(s["priority"] == "urgent" for s in retries))
    srv = fetch_server_spans(d)
    check("重试:源站记录中重试请求按加急排", all(s["priority"] == "urgent" for s in srv))
    late = [s for s in fetch_client if s["status"] == "late"]
    eff = [s for s in fetch_client if s["status"] == "effective"]
    check("重试:存在迟到尝试且每样品一个有效尝试",
          len(eff) == len(ids) and len(late) >= 1, f"迟到 {len(late)}、有效 {len(eff)}")
    wh_req = {w["request_id"] for w in d["warehouse"]}
    check("重试:入仓只取有效尝试的结果", wh_req <= {s["request_id"] for s in eff})
    # 旧尝试仍在源站执行完并上报跨度(不中断)
    srv_by_req = {s["request_id"]: s for s in srv}
    old = [s for s in fetch_client if s["attempt"] == 1]
    check("重试:超时放弃的旧请求仍在源站执行完并上报",
          all(s["request_id"] in srv_by_req and srv_by_req[s["request_id"]]["end_ts"]
              for s in old))


def e2e_queue_view(c):
    snap = c.get(f"{BASE}/api/queue").json()
    check("队列视图:含名额/执行中/等待中/策略",
          all(k in snap for k in ("slots", "running", "urgent_streak", "waiting",
                                   "executing", "policy")))


def e2e_visibility_and_slots(c):
    """排队过程在网页接口可见；调大名额立即放行排队请求且请求不丢。"""
    set_config(c, source_delay_ms=900, source_slots=1, client_timeout_ms=10000, max_attempts=2)
    ids = ["V1", "V2", "V3", "V4"]
    b = submit(c, ids, "normal")
    wait_queue(c, lambda q: len(q["waiting"]) >= 2)
    d = c.get(f"{BASE}/api/batches/{b}", timeout=5).json()
    srv = [s for s in d["spans"] if s["side"] == "server" and s["stage"] == "fetch"]
    waiting_rows = [s for s in srv if s.get("exec_start_ts") is None]
    check("可见性:批次详情能实时看到仍在排队的取数请求", len(waiting_rows) >= 1,
          f"{len(waiting_rows)} 条")
    check("可见性:排队请求带到达时刻且无结束时刻",
          all(s["end_ts"] is None and s["start_ts"] for s in waiting_rows))
    j = c.post(f"{BASE}/api/config", json={"source_slots": 3}, timeout=10).json()
    check("名额调整:下发成功", j["push"]["source"] == "ok", str(j["push"]))
    q = wait_queue(c, lambda q: q["running"] >= 3 and q["slots"] == 3 and not q["waiting"])
    check("名额调大:排队请求立即被放行", len(q["executing"]) >= 3, f"{len(q['executing'])} 执行中")
    dv = wait_batch(c, b, timeout=60)
    check("名额调大:批次仍全部入仓", dv["batch"]["status"] == "done"
          and len(dv["warehouse"]) == 4, f"{len(dv['warehouse'])}/4")
    set_config(c, source_slots=3, source_delay_ms=300)


def main():
    unit_tests()
    if "--unit" in sys.argv:
        print()
        sys.exit(1 if FAILS else print("全部单元自测通过") or 0)
    global BASE
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    BASE = args[0] if args else "http://localhost:8000"
    c = httpx.Client()
    check("编排端健康", c.get(f"{BASE}/api/health", timeout=5).json().get("ok") is True)
    e2e_queue_view(c)
    e2e_mixed(c)
    e2e_visibility_and_slots(c)
    e2e_retry_priority(c)
    # 还原基线参数，避免影响后续测试
    set_config(c, source_delay_ms=300, source_slots=3, source_fail_first=False,
               qc_nonneg_delay_ms=200, qc_match_delay_ms=200,
               client_timeout_ms=2000, max_attempts=3)
    print()
    if FAILS:
        print(f"共 {len(FAILS)} 项失败: {FAILS}")
        sys.exit(1)
    print("全部加急自测通过")


if __name__ == "__main__":
    main()
