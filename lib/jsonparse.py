"""DIG — robust JSON extraction from LLM responses.

Models wrap JSON in ```code fences```, prepend prose, or append trailing text
("Extra data"). This pulls the first complete JSON value (array or object) out
of such a response via string-aware bracket matching. Shared by every
LLM-calling path (ai_recommend, label_discovery, analyze_pool, deep_crawl,
genre_embeddings) so they all parse identically and robustly.
"""
import json


def extract_json(raw):
    """Return the first complete JSON value (list or dict) in `raw`, else None.

    Strips code fences, tries a direct parse, then walks brackets (skipping
    those inside strings) from the first '[' or '{' to its matching close.
    """
    if not raw:
        return None
    s = raw.strip()
    # Strip ``` / ```json fences
    if s.startswith("```"):
        nl = s.find("\n")
        if nl >= 0:
            s = s[nl + 1:]
        s = s.rstrip()
        if s.endswith("```"):
            s = s[:-3].rstrip()
        s = s.strip()
    # Fast path: clean JSON
    try:
        return json.loads(s)
    except Exception:
        pass
    # Locate the first opening bracket (array or object) and bracket-match it
    candidates = [i for i in (s.find("["), s.find("{")) if i >= 0]
    if not candidates:
        return None
    start = min(candidates)
    open_ch = s[start]
    close_ch = "]" if open_ch == "[" else "}"
    depth = 0
    in_str = False
    esc = False
    for i in range(start, len(s)):
        ch = s[i]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
        elif ch == '"':
            in_str = True
        elif ch == open_ch:
            depth += 1
        elif ch == close_ch:
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(s[start:i + 1])
                except Exception:
                    return None
    return None
