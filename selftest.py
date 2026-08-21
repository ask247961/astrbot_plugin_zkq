"""自测：验证插件 WS 握手边界 + 快照上传 + 结果帧健壮性（阶段 B' 协议 §13.3/§13.4）。"""
import asyncio
import json
import time
import urllib.request

import aiohttp

BASE = "http://127.0.0.1:16841"
WS_URL = "ws://127.0.0.1:16841/zkq_ws"
DEVICE = "selftest_dev"
NONCE = "nonce-abc-123"

PASS = 0
FAIL = 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✅ {name}")
    else:
        FAIL += 1
        print(f"  ❌ {name} {detail}")


def post_snapshot(device, nonce):
    body = json.dumps({
        "token": "", "deviceId": device, "nonce": nonce,
        "snapshot": {"mode": "升级时间上号", "running": True, "currentAccount": 3,
                     "lastEvent": "login ok"},
        "ts": time.time(),
    }).encode("utf-8")
    req = urllib.request.Request(f"{BASE}/zkq_snapshot", data=body,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=5) as resp:
        return json.loads(resp.read().decode("utf-8"))


async def try_connect(session, device, nonce, token=""):
    """Returns (ws, close_code) or (None, close_code_on_error)."""
    params = {"deviceId": device, "nonce": nonce}
    headers = {"X-Zkq-Token": token} if token else None
    try:
        ws = await session.ws_connect(f"{WS_URL}?{urllib.parse.urlencode(params)}",
                                      timeout=5, headers=headers)
        return ws, None
    except aiohttp.WSServerHandshakeError as e:
        return None, e.status


async def main():
    # 1. 快照上传（阶段 A 契约）
    r = await asyncio.to_thread(post_snapshot, DEVICE, NONCE)
    check("快照上传 status=ok", r.get("status") == "ok", r)
    check("recommended_interval=60", r.get("recommended_interval") == 60, r)

    # 1b. 同 deviceId 不同 nonce -> device_conflict
    r2 = await asyncio.to_thread(post_snapshot, DEVICE, "other-nonce")
    check("快照 nonce 冲突返回 device_conflict",
          r2.get("code") == "device_conflict", r2)

    async with aiohttp.ClientSession() as session:
        # 2. 正常握手
        ws, code = await try_connect(session, DEVICE, NONCE)
        check("WS 正常握手成功", ws is not None, f"code={code}")
        if ws:
            # 3. 同一设备同 nonce 重连 -> 旧连接被替换（1001）
            ws2, code2 = await try_connect(session, DEVICE, NONCE)
            check("同设备重连成功", ws2 is not None, f"code={code2}")
            if ws2:
                # 旧连接应被服务端关闭
                try:
                    old_closed = await asyncio.wait_for(ws.receive(), timeout=3)
                    check("旧连接被替换关闭", old_closed.type == aiohttp.WSMsgType.CLOSE,
                          str(old_closed))
                except Exception as e:
                    check("旧连接被替换关闭", False, str(e))
            # 4. WS 快照上报（合并通道测试）
            snap_frame = {
                "type": "snapshot",
                "deviceId": DEVICE,
                "nonce": NONCE,
                "snapshot": {"mode": "升级时间上号", "running": True, "currentAccount": 3, "lastEvent": "ws snapshot ok"},
                "ts": time.time(),
            }
            await ws2.send_str(json.dumps(snap_frame))
            ack_msg = await asyncio.wait_for(ws2.receive(), timeout=3)
            ack_data = json.loads(ack_msg.data) if ack_msg.type == aiohttp.WSMsgType.TEXT else {}
            check("WS 快照上报收到 snapshot_ack", ack_data.get("type") == "snapshot_ack", ack_data)

            # 4b. 垃圾结果帧（无 pending）不应导致断开
            await ws2.send_str(json.dumps({"requestId": 99999, "ok": True, "data": "x"}))
            await asyncio.sleep(1)
            check("垃圾帧后连接仍存活", not ws2.closed)
            await ws2.close()

        # 5. 缺 deviceId -> HTTP 400 拒绝握手
        try:
            await session.ws_connect(f"{WS_URL}?nonce=abc", timeout=5)
            check("缺 deviceId 被拒", False)
        except aiohttp.WSServerHandshakeError:
            check("缺 deviceId 被拒", True)

        # 6. nonce 冲突握手 -> HTTP 409 拒绝
        ws3, code3 = await try_connect(session, DEVICE, "nonce-DIFFERENT")
        check("握手 nonce 冲突被拒", ws3 is None and code3 == 409, f"code={code3}")

        # 7. 心跳：正常连接保持 8s（服务端 heartbeat=30 发 ping）
        ws4, _ = await try_connect(session, DEVICE, NONCE)
        if ws4:
            await asyncio.sleep(8)
            check("长连接 8s 存活", not ws4.closed)
            await ws4.close()

    print(f"\n结果: {PASS} 通过 / {FAIL} 失败")
    if FAIL:
        raise SystemExit(1)


if __name__ == "__main__":
    asyncio.run(main())
