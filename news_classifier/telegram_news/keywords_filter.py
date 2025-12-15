import os
from typing import List

_KEYWORDS = []
if os.path.exists(os.path.join(os.path.dirname(__file__), "keywords.txt")):
    with open(os.path.join(os.path.dirname(__file__), "keywords.txt"), "r", encoding="utf-8") as f:
        _KEYWORDS = [line.strip() for line in f if line.strip()]

def keyword_filter(rows: List[List[str]]) -> List[List[str]]:
    if not _KEYWORDS:
        return rows
        
    filtered_rows: List[List[str]] = []
    
    for row in rows:
        text = row[7]
        if any(keyword in text for keyword in _KEYWORDS):
            filtered_rows.append(row)
    return filtered_rows