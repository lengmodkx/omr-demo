"""OMR 诊断脚本: 直接调 /v1/recognize 看实际识别结果.

用法:
  python scripts/diagnose_omr.py --base-url http://127.0.0.1:9501 \
    --template-id 2071887110821445633 \
    --scan-url "http://oss.example.com/scan1.jpg" \
    [--columns x1,y1,x2,y2,question_no,num_q,num_options ...] \
    [--personal-info x1,y1,x2,y2,field page_index]

如果不传 --columns,会显示 Java 端目前 buildFastApiColumns 的输出.
"""
from __future__ import annotations

import argparse
import json
import sys
from typing import Any
from urllib import request as urlrequest


def parse_kv(s: str) -> dict[str, Any]:
    """Parse 'k1=v1,k2=v2' style."""
    out: dict[str, Any] = {}
    for pair in s.split(","):
        if "=" not in pair:
            continue
        k, v = pair.split("=", 1)
        out[k.strip()] = v.strip()
    return out


def parse_rect(s: str) -> dict[str, Any]:
    """Parse 'x1,y1,x2,y2,field[,page_index]'."""
    parts = [p.strip() for p in s.split(",")]
    return {
        "x1": int(parts[0]),
        "y1": int(parts[1]),
        "x2": int(parts[2]),
        "y2": int(parts[3]),
        "field": parts[4],
        "page_index": int(parts[5]) if len(parts) > 5 else 0,
    }


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--base-url", default="http://127.0.0.1:9501")
    p.add_argument("--template-id", required=True)
    p.add_argument("--scan-url", action="append", required=True,
                   help="答卷图片 URL, 可重复传多张")
    p.add_argument("--columns", action="append", default=[],
                   help="选择题列定义: 'x1,y1,x2,y2,question_no,num_q,num_options[,option_axis,reverse_q,page_index]' (坐标取像素绝对值,不要写表达式)")
    p.add_argument("--personal-info", action="append", default=[],
                   help="个人信息区域: 'x1,y1,x2,y2,field[,page_index]'")
    p.add_argument("--question-no", type=int, default=None)
    p.add_argument("--no-personal-info", action="store_true",
                   help="不传 personal_info_region (用于对比测试)")
    args = p.parse_args()

    columns = []
    for c in args.columns:
        parts = [p.strip() for p in c.split(",")]
        cfg = {
            "x1": int(parts[0]),
            "y1": int(parts[1]),
            "x2": int(parts[2]),
            "y2": int(parts[3]),
            "start_q": int(parts[4]),
            "num_q": int(parts[5]),
            "num_options": int(parts[6]),
        }
        if len(parts) > 7:
            cfg["option_axis"] = parts[7]
        if len(parts) > 8:
            cfg["reverse_q"] = parts[8].lower() in ("1", "true", "yes")
        if len(parts) > 9:
            cfg["page_index"] = int(parts[9])
        columns.append(cfg)

    # Step 1: parse_golden_template 先缓存
    parse_req = {
        "template_id": args.template_id,
        "template_image_url": args.scan_url[0],  # 用第一张图作为模板图
        "columns": columns,
    }
    if args.personal_info and not args.no_personal_info:
        parse_req["personal_info_region"] = [parse_rect(pi) for pi in args.personal_info]

    print("=" * 60)
    print("Step 1: POST /v1/templates/parse (缓存模板)")
    print("Request:")
    print(json.dumps(parse_req, indent=2, ensure_ascii=False))
    print()

    parse_resp = http_post(f"{args.base_url}/v1/templates/parse", parse_req)
    print("Response:")
    print(json.dumps(parse_resp, indent=2, ensure_ascii=False)[:2000])
    print()

    # Step 2: recognize 实际识别
    print("=" * 60)
    print("Step 2: POST /v1/recognize (实际识别)")
    rec_req = {
        "template_id": args.template_id,
        "scan_image_urls": args.scan_url,
        "question_no": args.question_no,
    }
    if args.personal_info and not args.no_personal_info:
        rec_req["personal_info_region"] = [parse_rect(pi) for pi in args.personal_info]
    print("Request:")
    print(json.dumps(rec_req, indent=2, ensure_ascii=False))
    print()

    rec_resp = http_post(f"{args.base_url}/v1/recognize", rec_req)
    print("Response:")
    print(json.dumps(rec_resp, indent=2, ensure_ascii=False))
    print()

    # 摘要
    print("=" * 60)
    print("Summary")
    answers = rec_resp.get("answers", [])
    pi = rec_resp.get("personal_info", [])
    code = rec_resp.get("code")
    bubbles = rec_resp.get("bubbles", [])
    print(f"code: {code}")
    print(f"answers count: {len(answers)}")
    print(f"answers: {answers[:5]}{'...' if len(answers) > 5 else ''}")
    print(f"personal_info: {pi}")
    print(f"bubbles count: {len(bubbles)}")
    if bubbles:
        print(f"first bubble: {bubbles[0]}")
    return 0


def http_post(url: str, body: dict) -> dict:
    req = urlrequest.Request(
        url,
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlrequest.urlopen(req, timeout=60) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as e:
        return {"_error": str(e)}


if __name__ == "__main__":
    sys.exit(main())