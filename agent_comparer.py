# agent_comparer.py
from typing import Optional
import os
from openai import OpenAI
# OPTIONAL: choose OpenAI or Azure OpenAI at runtime

MODEL_NAME = os.getenv("OPENAI_MODEL", "gpt-4o-mini")  # change as needed

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def _build_agent_prompt(cand: str, other: str, df) -> str:
    """Builds a grounded prompt using the comparison table for the candidates."""
    table_md = ""
    table_md = df.to_markdown(index=False)


    return f"""You are a precise hiring brief writer.

Task: Using the comparison table below, write a cohesive recruiter-ready summary
comparing **{cand}** and **{other}**. Base your summary **only on the facts** from
the table — do **not** invent statistics, percentages, or claims not present.

Comparison table (primary source of truth):
---
{table_md}
---

Output format:
- 1 concise paragraph summarizing the differences and strengths.
- 3–5 bullet points highlighting key factual takeaways.
- If any GENOS traits are low/very low or Athena mismatches exist, highlight them explicitly.
- Include 2–3 targeted interview questions that help decide between the candidates.
"""

def compare_summaries_agent(
    cand: str,
    other: str,
    df,
    *,
    model: Optional[str] = None,
) -> str:
    """Entry point called by candidates.py."""
    prompt = _build_agent_prompt(cand, other, df)
    if client is None:
        return "(Agent not configured)"

    model = model or MODEL_NAME

    try:
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "You are a precise, factual HR analyst."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.3,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        return f"(Agent error: {e})"
