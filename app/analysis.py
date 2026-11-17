"""批次结论分析：关键链(决定完成时间的等待与执行)、瓶颈判定、失败阻断定位。
所有结论只依据 spans(真实调用) 与 warehouse(真实入仓记录)。"""
import json

import db

STAGE_LABELS = {
    "fetch": "取数",
    "qc_nonneg": "质控①非负值",
    "qc_match": "质控②样品号对应",
    "warehouse": "入仓",
}
PRIORITY_LABELS = {"urgent": "加急", "normal": "常规"}


def _f(x):
    return round(float(x), 3)


def _sort_ts(s):
    """排序用时间：真正开始执行的时刻；仍在等待的排到最后；老记录退回到达时刻。"""
    if s.get("exec_start_ts"):
        return s["exec_start_ts"]
    if s.get("end_ts") is not None:
        return s["start_ts"]
    return 10 ** 18


def _order_row(i, s):
    pr = s.get("priority") or "normal"
    exec_ts = s.get("exec_start_ts")
    if exec_ts is None and s.get("end_ts") is not None:
        exec_ts = s["start_ts"]   # 老记录没有开始执行时刻：用到达时刻近似
    return {
        "order": i,
        "span_id": s["id"],
        "batch_id": s["batch_id"],
        "sample_id": s["sample_id"],
        "attempt": s["attempt"],
        "priority": pr,
        "priority_label": PRIORITY_LABELS.get(pr, "常规"),
        "arrive_seq": s.get("arrive_seq"),
        "admit_seq": s.get("admit_seq"),
        "arrive_ts": s["start_ts"],
        "admit_ts": exec_ts,          # None = 查询时仍在等待
        "queue_ms": s.get("queue_ms"),
        "exec_start_ts": exec_ts,
        "end_ts": s["end_ts"],
        "status": s["status"],
        "request_id": s["request_id"],
        "own_batch": s.get("_own", True),
    }


def _overlap_window(executed):
    """本批取数在源站占用的排队窗口：最早到达到最后执行结束/开始执行。"""
    if not executed:
        return None
    begin = min(s["start_ts"] for s in executed)   # 服务端跨度开始=到达源站时刻
    marks = []
    for s in executed:
        if s["end_ts"]:
            marks.append(s["end_ts"])
        if s.get("exec_start_ts"):
            marks.append(s["exec_start_ts"])
    return begin, max(marks) if marks else begin


def _overlap_peers(batch_id, window):
    """与本批排队窗口时间上重叠的其它批次取数请求。

    判断加急是否越过常规必须看同一时间窗内谁在排队：被越过的常客可能属于别的批次。
    这里以服务端跨度的时间戳为准(跨进程/重启也成立)，序号仅用于展示。
    """
    if not window:
        return []
    begin, end = window
    rows = db.query(
        "SELECT * FROM spans WHERE side='server' AND stage='fetch' AND batch_id<>?"
        " AND start_ts<=? AND (end_ts IS NULL OR end_ts>=?)",
        (batch_id, end, begin))
    out = []
    for r in rows:
        r = dict(r)
        r["_own"] = False
        out.append(r)
    return out


def _overtakes(combined, own_span_ids):
    """加急越过已排队常规：加急晚于某常规到达，却早于该常规开始执行。

    以真实时间戳判定(不依赖放行序号，避免源站进程重启后序号重置造成误判)；
    仍在等待的常规(admit_ts 为空)同样算被越过。
    """
    tol = 0.002
    normals = [e for e in combined if e["priority"] == "normal"]
    out = []
    for e in combined:
        if e["priority"] != "urgent" or e["admit_ts"] is None:
            continue
        beaten = [n for n in normals
                  if n["arrive_ts"] < e["arrive_ts"] - tol
                  and (n["admit_ts"] is None or n["admit_ts"] > e["admit_ts"] + tol)]
        if beaten:
            out.append({
                "urgent_sample": e["sample_id"], "urgent_batch_id": e["batch_id"],
                "urgent_admit_seq": e["admit_seq"], "admit_ts": e["admit_ts"],
                "overtook": [n["sample_id"] for n in beaten[:5]],
                "overtook_count": len(beaten),
                "involves_this_batch": e["span_id"] in own_span_ids
                                      or any(n["span_id"] in own_span_ids for n in beaten),
            })
    out.sort(key=lambda o: (not o["involves_this_batch"], o["admit_ts"]))
    return out


def _err_of(span):
    d = span.get("detail")
    if d:
        try:
            j = json.loads(d)
            if isinstance(j, dict):
                if j.get("error"):
                    return str(j["error"])
                if j.get("status_code"):
                    return f"HTTP {j['status_code']}"
                if j.get("response") is not None:
                    return json.dumps(j["response"], ensure_ascii=False)
        except Exception:
            pass
    return span["status"]


def analyze(batch_id):
    batch = db.query_one("SELECT * FROM batches WHERE id=?", (batch_id,))
    if not batch:
        return {"status": "failed", "text": "批次不存在"}
    sample_ids = list(dict.fromkeys(json.loads(batch["sample_ids"])))
    spans = db.query("SELECT * FROM spans WHERE batch_id=? ORDER BY start_ts,id", (batch_id,))
    wh = db.query("SELECT * FROM warehouse WHERE batch_id=?", (batch_id,))
    client = [s for s in spans if s["side"] == "client"]
    server_by_req = {s["request_id"]: s for s in spans if s["side"] == "server"}

    t0 = min([s["start_ts"] for s in spans], default=batch["created_at"])
    ends = [s["end_ts"] for s in spans if s["end_ts"]] + [w["stored_at"] for w in wh]
    t1 = max(ends) if ends else t0
    wh_by_sample = {w["sample_id"]: w for w in wh}
    done = bool(sample_ids) and all(s in wh_by_sample for s in sample_ids)
    priority = batch.get("priority") or "normal"

    # 源站实际执行顺序：服务端跨度按真实开始执行时刻排列(仍在等待的排最后)
    executed = sorted(
        [s for s in spans if s["side"] == "server" and s["stage"] == "fetch"],
        key=lambda s: (_sort_ts(s), s["start_ts"]))
    exec_order = [_order_row(i, s) for i, s in enumerate(executed, 1)]

    # 同期源站放行顺序：把与本批排队窗口重叠的其它批次请求也纳入，才能如实判断
    # "加急是否越过已排队的常规"(越过方与本批之外的常规请求竞争同一名额)
    window = _overlap_window(executed)
    peers = _overlap_peers(batch_id, window)
    combined = sorted(executed + peers, key=lambda s: (_sort_ts(s), s["start_ts"]))
    peer_order = [_order_row(i, s) for i, s in enumerate(combined, 1)]
    # 是否出现加急越过常规：以真实到达/开始执行时间判定(被越过者可能属其它批次)
    overtakes = _overtakes(peer_order, {s["id"] for s in spans})

    # 各阶段统计：sum(各次耗时求和) 与 wall(墙钟跨度) 分开，并行/重叠不相加
    per_stage = {}
    for st in ("fetch", "qc_nonneg", "qc_match", "warehouse"):
        ss = [s for s in client if s["stage"] == st and s["end_ts"] is not None]
        if not ss:
            continue
        per_stage[st] = {
            "label": STAGE_LABELS[st],
            "attempts": len(ss),
            "sum_s": _f(sum(s["end_ts"] - s["start_ts"] for s in ss)),
            "wall_s": _f(max(s["end_ts"] for s in ss) - min(s["start_ts"] for s in ss)),
            "effective": sum(1 for s in ss if s["status"] == "effective"),
            "ok": sum(1 for s in ss if s["status"] == "ok"),
            "failed": sum(1 for s in ss if s["status"] == "failed"),
            "timeout": sum(1 for s in ss if s["status"] == "timeout"),
            "late": sum(1 for s in ss if s["status"] == "late"),
            # 超时放弃是经历而非最终状态(之后会转为有效/迟到/失败)，从备注中统计
            "gave_up": sum(1 for s in ss if "超时" in (s["note"] or "")),
        }

    # 工作量归因：等名额(源站服务端上报的槽位等待) / 源站执行 / 两项质控 / 入仓
    eff_fetch = [s for s in client if s["stage"] == "fetch" and s["status"] == "effective"]
    slot_wait = sum((server_by_req.get(s["request_id"]) or {}).get("queue_ms") or 0
                    for s in eff_fetch) / 1000.0
    fetch_sum = sum(s["end_ts"] - s["start_ts"] for s in eff_fetch)
    parts = {
        "等名额(源站槽位)": slot_wait,
        "源站取数执行": max(fetch_sum - slot_wait, 0.0),
        "质控①非负值": per_stage.get("qc_nonneg", {}).get("sum_s", 0.0),
        "质控②样品号对应": per_stage.get("qc_match", {}).get("sum_s", 0.0),
        "入仓": per_stage.get("warehouse", {}).get("sum_s", 0.0),
    }
    bottleneck = max(parts, key=lambda k: parts[k]) if parts else None

    result = {
        "status": "done" if done else "failed",
        "t0": t0, "t1": t1, "wall_s": _f(t1 - t0),
        "samples_total": len(sample_ids),
        "samples_stored": len(wh_by_sample),
        "warehouse_rows": len(wh),
        "client_calls": len(client),
        "priority": priority,
        "priority_label": PRIORITY_LABELS.get(priority, "常规"),
        "exec_order": exec_order,
        "peer_order": peer_order,
        "overtakes": overtakes,
        "per_stage": per_stage,
        "work_parts_s": {k: _f(v) for k, v in parts.items()},
        "bottleneck": bottleneck,
        "chain": None,
        "blocking": None,
    }
    if exec_order:
        own_ov = [o for o in overtakes if o["involves_this_batch"]]
        waiting_now = [e for e in exec_order if e["exec_start_ts"] is None
                       and e["end_ts"] is None]
        result["text_prefix"] = (
            f"源站实际执行顺序:" +
            " → ".join(f"#{e['order']}{e['priority_label']}·{e['sample_id']}"
                       f"(尝试{e['attempt']},等{e['queue_ms'] or 0:.0f}ms)"
                       for e in exec_order[:12]) +
            ("…" if len(exec_order) > 12 else "") +
            (f";仍有 {len(waiting_now)} 个请求在源站排队" if waiting_now else "") +
            (f";本批相关加急越过排队常规 {len(own_ov)} 次"
             f"(同期源站共记录 {len(overtakes)} 次)"
             if overtakes else ";未发生加急越过常规") + "。")

    if done:
        # 关键链：最后完成入仓的样品决定了批次完成时间
        last_w = max(wh, key=lambda w: w["stored_at"])
        sid = last_w["sample_id"]
        sps = [s for s in client if s["sample_id"] == sid]
        fe = next((s for s in sps if s["stage"] == "fetch" and s["status"] == "effective"), None)
        wh_span = next((s for s in sps if s["stage"] == "warehouse" and s["end_ts"]), None)
        qcs = [s for s in sps if s["stage"] in ("qc_nonneg", "qc_match") and s["end_ts"]]
        segs = []
        if fe and wh_span:
            srv = server_by_req.get(fe["request_id"])
            note = ""
            if srv and srv.get("queue_ms"):
                note = f"含等名额 {srv['queue_ms']:.0f}ms(源站服务端记录)"
            segs.append({"kind": "exec", "label": f"取数·尝试#{fe['attempt']}(有效)",
                         "start": fe["start_ts"], "end": fe["end_ts"],
                         "span_ids": [fe["id"]], "note": note})
            prev = fe["end_ts"]
            if qcs:
                qs, qe = min(s["start_ts"] for s in qcs), max(s["end_ts"] for s in qcs)
                if qs - prev > 0.0005:
                    segs.append({"kind": "wait", "label": "调度间隙",
                                 "start": prev, "end": qs, "span_ids": []})
                slower = max(qcs, key=lambda s: s["end_ts"] - s["start_ts"])
                segs.append({"kind": "exec",
                             "label": f"并行质控×{len(qcs)}(较慢:{STAGE_LABELS[slower['stage']]})",
                             "start": qs, "end": qe,
                             "span_ids": [s["id"] for s in qcs],
                             "note": "两项并行，耗时取较慢者，不相加"})
                prev = qe
            if wh_span["start_ts"] - prev > 0.0005:
                segs.append({"kind": "wait", "label": "调度间隙",
                             "start": prev, "end": wh_span["start_ts"], "span_ids": []})
            segs.append({"kind": "exec", "label": "入仓",
                         "start": wh_span["start_ts"], "end": wh_span["end_ts"],
                         "span_ids": [wh_span["id"]], "note": f"入仓记录#{last_w['id']}"})
            result["chain"] = {"sample_id": sid, "segments": segs,
                               "total_s": _f(wh_span["end_ts"] - fe["start_ts"])}
        bits = []
        for seg in segs:
            d = _f(seg["end"] - seg["start"])
            bits.append(f"{seg['label']} {d:.3f}s" + (f"({seg['note']})" if seg.get("note") else ""))
        fr = per_stage.get("fetch", {})
        result["text"] = (
            f"批次#{batch_id}({PRIORITY_LABELS.get(priority, '常规')}) 已完成:"
            f"{len(wh_by_sample)}/{len(sample_ids)} 样品入仓"
            f"(入仓记录 {len(wh)} 条、跨服务调用 {len(client)} 次,结论以实际记录为据)。"
            f"墙钟总耗时 {_f(t1 - t0)}s。"
            + (result.get("text_prefix") or "")
            + f"决定完成时间的关键链(样品 {sid}): " + " → ".join(bits) + "。"
            f"瓶颈判定: {bottleneck}(各环节工作量秒: "
            + "、".join(f"{k}={v}" for k, v in result["work_parts_s"].items())
            + ";并行质控与重叠重试按墙钟计,不直接相加)。"
            f"取数尝试 {fr.get('attempts', 0)} 次:有效 {fr.get('effective', 0)}、"
            f"失败 {fr.get('failed', 0)}、迟到 {fr.get('late', 0)}"
            f"(其中曾超时放弃 {fr.get('gave_up', 0)} 次)。")
    else:
        blocking = []
        for sid in sample_ids:
            if sid in wh_by_sample:
                continue
            sps = [s for s in client if s["sample_id"] == sid]
            fetches = [s for s in sps if s["stage"] == "fetch"]
            eff = [s for s in fetches if s["status"] == "effective"]
            if fetches and not eff:
                last = max(fetches, key=lambda s: s["start_ts"])
                tries = "、".join(f"#{s['attempt']}:{s['status']}"
                                 for s in sorted(fetches, key=lambda x: x["attempt"]))
                blocking.append({"sample_id": sid, "stage": "fetch",
                                 "detail": f"取数 {len(fetches)} 次尝试均未成功({tries});"
                                           f"最后错误:{_err_of(last)}"})
                continue
            bad_qc = [s for s in sps if s["stage"] in ("qc_nonneg", "qc_match")
                      and s["status"] != "ok"]
            if bad_qc:
                s = bad_qc[0]
                blocking.append({"sample_id": sid, "stage": s["stage"],
                                 "detail": f"{STAGE_LABELS[s['stage']]}未通过/失败:{_err_of(s)}"})
                continue
            if not fetches:
                blocking.append({"sample_id": sid, "stage": "fetch",
                                 "detail": "尚无取数记录(批次可能仍在运行)"})
            else:
                blocking.append({"sample_id": sid, "stage": "warehouse",
                                 "detail": "入仓环节未完成"})
        result["blocking"] = blocking
        bl = "；".join(f"样品 {b['sample_id']} 阻断于{STAGE_LABELS.get(b['stage'], b['stage'])}"
                       f"({b['detail']})" for b in blocking) or "无阻断明细"
        result["text"] = (
            f"批次#{batch_id}({PRIORITY_LABELS.get(priority, '常规')}) 未完成:"
            f"{len(wh_by_sample)}/{len(sample_ids)} 样品入仓"
            f"(入仓记录 {len(wh)} 条)。"
            + (result.get("text_prefix") or "")
            + f"阻断环节: {bl}。失败批次不生成成功链。")
    return result
