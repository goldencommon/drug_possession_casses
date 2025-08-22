
# 聚合 Batch 输出，统计总克数、按毒品品类汇总，并导出 CSV。
import json, glob
from decimal import Decimal, ROUND_HALF_UP
from collections import defaultdict
from pathlib import Path
import pandas as pd

OUTPUT_DIR = Path("batch_outputs")
files = sorted(glob.glob(str(OUTPUT_DIR / "batch_output_*.jsonl")))
assert files, "未找到 batch 输出文件，请先运行 02_submit_batch.py"

DATA_DIR = Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

per_case_rows = []
by_drug = defaultdict(Decimal)

for f in files:
    with open(f, "r", encoding="utf-8") as r:
        for line in r:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            print(obj)
            try:
                parsed = obj["response"]["output_parsed"]
            except KeyError:
                parsed_str = obj.get("response", {}).get("body", {}).get("output", {})[0].get("content")[0].get("text")
                parsed = json.loads(parsed_str) if parsed_str else None
            if not parsed:
                continue

            case_id = parsed.get("case_id","")
            print(case_id)
            for d in parsed.get("drugs", []):
                name = (d.get("name_std") or "").strip() or "未指明"
                print(name)
                amt = Decimal(str(d.get("amount_grams", 0) or 0))
                by_drug[name] += amt
                per_case_rows.append({
                    "case_id": case_id,
                    "drug": name,
                    "amount_grams": float(amt.quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)),
                    "name_raw": d.get("name_raw",""),
                    "unit_raw": d.get("unit_raw",""),
                    "amount_raw_text": d.get("amount_raw_text",""),
                    "evidence_span": d.get("evidence_span",""),
                    "certainty": d.get("certainty","")
                })

df_details = pd.DataFrame(per_case_rows)
df_details.to_csv("data/details_per_evidence.csv", index=False, encoding="utf-8-sig")
print(by_drug)
df_summary = pd.DataFrame([{"drug": k, "total_grams": float(v)} for k, v in by_drug.items()])\
                .sort_values("total_grams", ascending=False)
df_summary.to_csv("data/summary_by_drug.csv", index=False, encoding="utf-8-sig")

total_grams = float(sum(by_drug.values()))
with open("data/TOTAL.txt","w",encoding="utf-8") as w:
    w.write(f"总量(克): {{total_grams}}\n")

print("已写出：")
print(" - 细项：data/details_per_evidence.csv")
print(" - 汇总：data/summary_by_drug.csv")
print(" - 总量：data/TOTAL.txt")
