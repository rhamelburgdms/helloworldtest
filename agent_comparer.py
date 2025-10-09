from typing import Optional
import os
from openai import AzureOpenAI

client = AzureOpenAI(
    api_key=os.environ["OPENAI_API_KEY"],
    api_version="2024-08-01-preview",
    azure_endpoint="https://forhr.openai.azure.com/"
)

def _build_summary_prompt(cand_summary: str, other_summaries: dict[str, str]) -> str:
    others_text = "\n\n".join(
        f"--- {name} ---\n{summary}" for name, summary in other_summaries.items()
    )
    return f"""
You are a precise hiring brief writer.

Task: Compare the current candidate's summary with the others' applying for the same role, focusing specifically on "People Orientation + Tolerance; Decision-making + Ability to Notice; Dealing with Difficult Situations + Tolerance; Trainability + Role ID / Receptiveness to Change."

Mention any other 'leg ups' that either candidate may have over the other. Do not refer to the current candidate as 'current candidate', use their name. 

Current Candidate:
---
{cand_summary}
---

Other candidates:
---
{others_text}
---

Output format:
- 1 cohesive paragraph summarizing differences.
- 3–5 factual bullet points.
- 2–3 targeted interview questions.
"""

def compare_summaries_agent(
    cand_summary: str,
    other_summaries: dict[str, str],
    *,
    model: Optional[str] = None,
) -> str:
    """Compare one candidate’s summary against multiple others."""
    prompt = _build_summary_prompt(cand_summary, other_summaries)
    if client is None:
        return "(Agent not configured)"

    model = model or "gpt-4"

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
