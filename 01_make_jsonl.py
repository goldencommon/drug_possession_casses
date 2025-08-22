import pandas as pd, json
from pathlib import Path

INPUT = Path("drug_possession_cases_sample.xlsx")
OUT_JSONL = Path("batch_requests.jsonl")
SCHEMA_PATH = Path("schema.json")

SCHEMA = {
  "name": "CaseDrugExtraction",
  "type": "json_schema",
  "schema": {
    "type": "object",
    "properties": {
      "case_id": {"type": "string"},
      "drugs": {
        "type": "array",
        "items": {
          "type": "object",
          "properties": {
            "name_std": {"type": "string"},
            "name_raw": {"type": "string"},
            "amount_grams": {"type": "number"},
            "unit_raw": {"type": "string"},
            "amount_raw_text": {"type": "string"},
            "evidence_span": {"type": "string"},
            "certainty": {"type": "string", "enum": ["high","medium","low"]}
          },
          "required": [
            "name_std","name_raw","amount_grams",
            "unit_raw","amount_raw_text","evidence_span","certainty"
          ],
          "additionalProperties": False   # 在 items 对象里平级
        }
      },
      "totals": {
        "additionalProperties": False,
        "type": "object",
        "properties": {"amount_grams_sum": {"type": "number"}},
        "required": ["amount_grams_sum"]
      },
      "notes": {"type": "string"}
    },
    "additionalProperties": False,
    "required": ["case_id","drugs","totals","notes"]
  },
  "strict": True
}

# 系统提示改为字符串而不是 tuple
SYSTEM_PROMPT = """你是法律信息抽取助手。只从文本中抽取已查明的毒品名称与净重量，
严格区分毛重/净重，多个表述以净重为准；无法确定的数量不要臆测。
输出遵循给定JSON架构。
若文中仅有毛重，amount_grams填0，并在notes说明。
对别名：冰毒=甲基苯丙胺；K粉=氯胺酮；麻古=复方（若无法拆分则按判决归类）；海洛因=二乙酰吗啡。
单位统一转换为克；mg→÷1000, kg→×1000。
返回 JSON 对象，字段严格包含：case_id, drugs[{name_std,name_raw,amount_grams,unit_raw,amount_raw_text,evidence_span,certainty}], totals{amount_grams_sum}, notes；不得输出多余字段。
"""

df = pd.read_excel(INPUT)
assert 'case_number' in df.columns and 'judgment' in df.columns, "缺少列 case_number 或 judgment"

SCHEMA_PATH.write_text(json.dumps(SCHEMA, ensure_ascii=False, indent=2), encoding="utf-8")

with OUT_JSONL.open("w", encoding="utf-8") as w:
    for _, row in df.iterrows():
        case_id = str(row['case_number'])
        text = "" if pd.isna(row['judgment']) else str(row['judgment'])

        messages = [
          {"role":"system","content": SYSTEM_PROMPT},
          {"role":"user","content": f"case_id={case_id}\n{text}"}
        ]

        one = {
          "custom_id": f"case-{case_id}",
          "method": "POST",
          "url": "/v1/responses",
          "body": {
            "model": "gpt-4o-mini",
            "input": messages,
            "text": {
              "format": SCHEMA,
            },
            "temperature": 0
          }
        }
        w.write(json.dumps(one, ensure_ascii=False) + "\n")

print(f"已生成 JSONL: {OUT_JSONL} ({sum(1 for _ in open(OUT_JSONL, 'r', encoding='utf-8'))} 行)")
