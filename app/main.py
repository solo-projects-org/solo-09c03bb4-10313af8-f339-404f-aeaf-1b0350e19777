"""诊断网页与编排 API：提交批次、查询批次/时间线、运行时调参并下发到源站/质控。"""
import asyncio
import json
import os
import time

import httpx
from fastapi import Body, FastAPI
from fastapi.responses import FileResponse, JSONResponse

import analysis
import db
import pipeline

SOURCE_URL = os.environ.get("SOURCE_URL", "http://localhost:8001")
QC_URL = os.environ.get("QC_URL", "http://localhost:8002")
STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")

app = FastAPI(title="样品入仓链路诊断")


@app.get("/")
async def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


@app.post("/api/batches")
async def create_batch(body: dict = Body(...)):
    raw = body.get("sample_ids") or []
    ids = list(dict.fromkeys(str(s).strip() for s in raw if str(s).strip()))
    if not ids:
        return JSONResponse({"error": "样品号清单为空"}, status_code=400)
    priority = pipeline.normalize_priority(body.get("priority"))
    cur = db.execute(
        "INSERT INTO batches(created_at, sample_ids, priority, status) VALUES(?,?,?,?)",
        (time.time(), json.dumps(ids, ensure_ascii=False), priority, "running"))
    bid = cur.lastrowid
    asyncio.create_task(pipeline.run_batch(bid, ids, priority))
    return {"id": bid, "samples": len(ids), "priority": priority}


@app.get("/api/batches")
async def list_batches():
    rows = db.query("SELECT * FROM batches ORDER BY id DESC LIMIT 100")
    out = []
    for b in rows:
        item = {"id": b["id"], "status": b["status"], "created_at": b["created_at"],
                "finished_at": b["finished_at"], "samples": len(json.loads(b["sample_ids"])),
                "priority": b.get("priority") or "normal",
                "priority_label": pipeline.PRIORITY_LABEL.get(b.get("priority") or "normal")}
        if b["conclusion"]:
            c = json.loads(b["conclusion"])
            item.update({"bottleneck": c.get("bottleneck"), "wall_s": c.get("wall_s"),
                         "stored": c.get("samples_stored")})
        out.append(item)
    return out


@app.get("/api/batches/{bid}")
async def batch_detail(bid: int):
    b = db.query_one("SELECT * FROM batches WHERE id=?", (bid,))
    if not b:
        return JSONResponse({"error": "not found"}, status_code=404)
    spans = db.query("SELECT * FROM spans WHERE batch_id=? ORDER BY start_ts,id", (bid,))
    wh = db.query("SELECT * FROM warehouse WHERE batch_id=? ORDER BY stored_at", (bid,))
    # 结论按当前记录实时重算：迟到跨度随时可能补记，看到的一定与记录一致
    concl = analysis.analyze(bid) if spans else None
    return {
        "batch": {"id": b["id"], "status": b["status"], "created_at": b["created_at"],
                  "finished_at": b["finished_at"], "sample_ids": json.loads(b["sample_ids"]),
                  "priority": b.get("priority") or "normal",
                  "priority_label": pipeline.PRIORITY_LABEL.get(b.get("priority") or "normal")},
        "spans": spans,
        "warehouse": wh,
        "conclusion": concl,
    }


@app.get("/api/queue")
async def queue_snapshot():
    """源站名额队列快照：等待/执行中/最近放行顺序；源站不可达时如实报错。"""
    try:
        async with httpx.AsyncClient(timeout=3) as c:
            r = await c.get(f"{SOURCE_URL}/queue")
            if r.status_code != 200:
                return JSONResponse({"error": f"source http {r.status_code}"}, status_code=502)
            return r.json()
    except Exception as e:
        return JSONResponse({"error": f"source unreachable: {e}"}, status_code=502)


@app.post("/api/ingest")
async def ingest(body: dict = Body(...)):
    """接收源站/质控的服务端跨度，按 request_id 与客户端跨度关联。

    源站的同一取数请求会上报多次(进队→放行→完成)，按
    (stage, request_id, attempt) 幂等更新，逐步补全等待/执行/结束证据。
    """
    try:
        fields = {k: body.get(k) for k in db.SERVER_SPAN_FIELDS}
        if fields.get("priority") is not None:
            fields["priority"] = pipeline.normalize_priority(fields["priority"])
        fields["stage"] = str(fields.get("stage") or "")
        fields["sample_id"] = str(fields.get("sample_id") or "")
        fields["service"] = str(fields.get("service") or "")
        fields["request_id"] = str(fields.get("request_id") or "")
        fields["batch_id"] = int(fields.get("batch_id") or 0)
        fields["attempt"] = int(fields.get("attempt") or 1)
        fields["status"] = str(fields.get("status") or "ok")
        fields["start_ts"] = float(fields.get("start_ts") or 0)
        db.upsert_server_span(fields)
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)
    return {"ok": True}


@app.get("/api/config")
async def get_config():
    return db.get_config()


@app.post("/api/config")
async def set_config(body: dict = Body(...)):
    cfg = db.get_config()
    for k in db.CONFIG_DEFAULTS:
        if k in body:
            cfg[k] = body[k]
    db.save_config(cfg)
    push = await _push_config(cfg)
    return {"config": cfg, "push": push}


async def _push_config(cfg):
    """把运行时参数下发到源站与质控服务；服务不可达时如实返回错误。"""
    out = {}
    async with httpx.AsyncClient(timeout=3) as c:
        try:
            r = await c.post(f"{SOURCE_URL}/config", json={
                "delay_ms": cfg["source_delay_ms"],
                "slots": cfg["source_slots"],
                "fail_first": cfg["source_fail_first"]})
            out["source"] = "ok" if r.status_code == 200 else f"http {r.status_code}"
        except Exception as e:
            out["source"] = f"error: {e}"
        try:
            r = await c.post(f"{QC_URL}/config", json={"checks": {
                "nonneg": {"delay_ms": cfg["qc_nonneg_delay_ms"],
                           "fail_first": cfg["qc_nonneg_fail_first"]},
                "match": {"delay_ms": cfg["qc_match_delay_ms"],
                          "fail_first": cfg["qc_match_fail_first"]}}})
            out["qc"] = "ok" if r.status_code == 200 else f"http {r.status_code}"
        except Exception as e:
            out["qc"] = f"error: {e}"
    return out


@app.get("/api/health")
async def health():
    return {"ok": True}
