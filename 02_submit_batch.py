# 使用 OpenAI Python SDK 提交 Batch 任务并轮询直至完成，然后自动下载结果文件。
# 运行前：pip install openai
# export OPENAI_API_KEY=...
import time, sys
from pathlib import Path
from openai import OpenAI
import os

REQUESTS_JSONL = Path("batch_requests.jsonl")
OUTPUT_DIR = Path("batch_outputs")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    raise RuntimeError("OPENAI_API_KEY 未设置。请先在终端执行：export OPENAI_API_KEY=你的key")
client = OpenAI(api_key=api_key)

upload = client.files.create(file=open(REQUESTS_JSONL, "rb"), purpose="batch")
print("upload.id =", upload.id)
try:
    batch = client.batches.create(input_file_id=upload.id, endpoint="/v1/responses", completion_window="24h")
except Exception as e:
    # 为常见的账单上限错误给出更友好的提示
    msg = str(e)
    if "billing_hard_limit_reached" in msg or "Billing hard limit has been reached" in msg:
        print("[错误] 账单硬上限已达到（billing_hard_limit_reached）。请在 OpenAI 控制台为当前账号/组织增加配额或充值，然后重试。")
        raise
    else:
        raise
print("batch.id =", batch.id)

while True:
    b = client.batches.retrieve(batch.id)
    print("status =", b.status)
    if b.status in ("completed","failed","cancelled","expired"):
        break
    time.sleep(10)

# 如果 batch 失败，尝试下载错误详情文件（如有）
if b.status == "failed":
    try:
        if getattr(b, "error_file_id", None):
            err_txt = client.files.content(b.error_file_id).read().decode("utf-8", errors="ignore")
            err_path = OUTPUT_DIR / f"batch_error_{batch.id}.log"
            err_path.write_text(err_txt, encoding="utf-8")
            print("已写出错误详情：", err_path)
        elif getattr(b, "errors", None):
            # 某些版本返回 errors 为复杂对象，做通用序列化
            err_path = OUTPUT_DIR / f"batch_error_{batch.id}.json"
            from json import dumps

            def to_jsonable(x):
                try:
                    # pydantic v2
                    return x.model_dump()
                except Exception:
                    pass
                try:
                    # pydantic v1 或类似
                    return x.dict()
                except Exception:
                    pass
                if isinstance(x, (list, tuple)):
                    return [to_jsonable(i) for i in x]
                if isinstance(x, dict):
                    return {k: to_jsonable(v) for k, v in x.items()}
                # fallback
                return str(x)

            try:
                err_json = to_jsonable(b.errors)
                err_path.write_text(dumps(err_json, ensure_ascii=False, indent=2), encoding="utf-8")
                print("已写出错误详情：", err_path)
            except Exception as e_json:
                print("序列化错误详情失败：", e_json)
        else:
            print("Batch 失败但未提供错误文件。")
    except Exception as e:
        print("尝试获取错误详情失败：", e)


if b.status == "completed":
    if b.output_file_id:
        print("✅ 有输出文件：", b.output_file_id)
    else:
        print("⚠️ 已完成但没有输出文件，可能全部失败")
        if getattr(b, "error_file_id", None):
            err_txt = client.files.content(b.error_file_id).read().decode("utf-8", errors="ignore")
            err_path = OUTPUT_DIR / f"batch_error_{batch.id}.json"
            err_path.write_text(err_txt, encoding="utf-8")
            print("已写出错误详情：", err_path)
        elif getattr(b, "errors", None):
            print("Batch errors =", b.errors)
        else:
            print("没有 error_file_id，也没有 errors，可能是 bug")

content = client.files.content(b.output_file_id).read().decode("utf-8")
out_path = OUTPUT_DIR / f"batch_output_{batch.id}.jsonl"
out_path.write_text(content, encoding="utf-8")
print("已下载到：", out_path)
