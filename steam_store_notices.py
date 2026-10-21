"""
Parse Steam store-page disclosure notices that are not reliably present in appdetails.

Extracts:
  - Made with AI (AI Generated Content Disclosure)
  - Kernel-level anti-cheat
  - Third-party DRM (incl. Denuvo flag)
  - Third-party EULA requirement
"""

from __future__ import annotations

import re
from html import unescape
from typing import Any, Dict, Optional


def _clean(text: str) -> str:
    text = unescape(text or "")
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def empty_store_notices() -> Dict[str, Any]:
    return {
        "made_with_ai": False,
        "made_with_ai_disclosure": None,
        "requires_kernel_anticheat": False,
        "kernel_anticheat_name": None,
        "has_third_party_drm": False,
        "third_party_drm": None,
        "has_denuvo": False,
        "requires_eula": False,
        "eula_label": None,
    }


def parse_store_notices(html: Optional[str]) -> Dict[str, Any]:
    """Return structured notice flags/details from a Steam store HTML page."""
    notices = empty_store_notices()
    if not html:
        return notices

    # AI Generated Content Disclosure (dedicated section, not in appdetails)
    ai_m = re.search(
        r"<h2>\s*AI Generated Content Disclosure\s*</h2>(.*?)(?:</div>\s*<div|</div>\s*$)",
        html,
        re.I | re.S,
    )
    if not ai_m:
        ai_m = re.search(
            r"<h2>\s*AI Generated Content Disclosure\s*</h2>(.{0,2500})",
            html,
            re.I | re.S,
        )
    if ai_m:
        notices["made_with_ai"] = True
        paras = re.findall(r"<p>(.*?)</p>", ai_m.group(1), re.I | re.S)
        body = [_clean(p) for p in paras]
        body = [
            p
            for p in body
            if p and not p.lower().startswith("the developers describe")
        ]
        notices["made_with_ai_disclosure"] = (" ".join(body)[:2000] or None)

    # Kernel-level anti-cheat
    if re.search(r"Uses Kernel Level Anti-Cheat", html, re.I):
        notices["requires_kernel_anticheat"] = True
        name_m = re.search(
            r"Uses Kernel Level Anti-Cheat.*?"
            r'class="anticheat_name"[^>]*>(.*?)(?:<span|</div>)',
            html,
            re.I | re.S,
        )
        if name_m:
            notices["kernel_anticheat_name"] = _clean(name_m.group(1)) or None
        else:
            # Fallback: text immediately after the label in a compact block
            loose = re.search(
                r"Uses Kernel Level Anti-Cheat\s*</div>\s*<div[^>]*>(.*?)</div>",
                html,
                re.I | re.S,
            )
            if loose:
                notices["kernel_anticheat_name"] = _clean(loose.group(1)).split(" - ")[0] or None

    # Third-party DRM (e.g. "Incorporates 3rd-party DRM: Denuvo")
    drm_names = []
    for m in re.finditer(
        r"Incorporates 3rd-party DRM:\s*([^<\n\r]+)",
        html,
        re.I,
    ):
        name = _clean(m.group(1))
        if name:
            drm_names.append(name)
    if drm_names:
        # Preserve order, drop dupes
        uniq = list(dict.fromkeys(drm_names))
        notices["has_third_party_drm"] = True
        notices["third_party_drm"] = "; ".join(uniq)

    # Third-party EULA
    if re.search(r"Requires agreement to a 3rd-party EULA", html, re.I):
        notices["requires_eula"] = True
        label_m = re.search(
            r"Requires agreement to a 3rd-party EULA.*?"
            r'href="[^"]*eula[^"]*"[^>]*>(.*?)</a>',
            html,
            re.I | re.S,
        )
        if label_m:
            notices["eula_label"] = _clean(label_m.group(1)) or None

    denuvo_blob = " ".join(
        filter(
            None,
            [
                notices.get("third_party_drm"),
                notices.get("kernel_anticheat_name"),
            ],
        )
    ).lower()
    notices["has_denuvo"] = "denuvo" in denuvo_blob

    return notices
