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

Task: Compare {cand_name}'s summary with the other candidates applying for the same role. Focus specifically on the following capability pairings:
- People Orientation + Tolerance
- Decision-making + Ability to Notice
- Dealing with Difficult Situations + Tolerance
- Trainability + Role ID / Receptiveness to Change

Instructions:
- If {cand_name} is stronger or weaker than another candidate in any area, state this clearly with evidence.
- Mention any additional 'leg ups' either candidate may have.
- Write in a professional, manager-friendly tone.

Inputs:
Current Candidate ({cand_name}):
---
{cand_summary}
---

Other Candidates:
---
{others_text}
---

Output format:
1. **Narrative Comparison**: A cohesive paragraph summarizing relative strengths, risks, and fit.
2. **Factual Highlights**: 3–5 concise bullet points with specific comparisons (trait vs trait).
3. **Interview Probes**: 2–3 targeted interview questions for each candidate, focusing on areas of uncertainty, contradictions, or critical gaps.
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
