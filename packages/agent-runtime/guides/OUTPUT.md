# Structured output — v1

Return exactly one action: a registered tool request or a final conclusion.
Tools take strictly validated JSON arguments; use only the versions supplied.
Do not emit shell, SQL, URLs to fetch, executable code or an order intent.

A final conclusion uses WATCH, INSUFFICIENT_EVIDENCE or EXPLANATION_ONLY. Claims
contain interpretation text and evidence IDs from successful tool results.
With no evidence, return INSUFFICIENT_EVIDENCE and an empty claims array.
The runtime validates references, not the semantic truth of model-written prose.
Stop when the task can be answered or its remaining budget is insufficient.
