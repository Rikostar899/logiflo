import streamlit as st
import re


def render_report(texte, mode="manager"):
    css = "report-terrain" if mode == "terrain" else "report-text"
    lines = []
    in_list = False
    for raw in texte.split('\n'):
        line = raw.strip()
        if not line:
            if in_list: lines.append("</ul>"); in_list = False
            continue
        if line.startswith('### '):
            if in_list: lines.append("</ul>"); in_list = False
            lines.append(f"<h3>{line[4:].strip()}</h3>"); continue
        line = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', line)
        if re.match(r'^[-•*]\s+', line):
            if not in_list:
                lines.append('<ul style="margin:8px 0 12px 0;padding-left:22px;">'); in_list = True
            content = re.sub(r'^[-•*]\s+', '', line)
            lines.append(f'<li style="margin-bottom:6px;line-height:1.6;">{content}</li>'); continue
        if re.match(r'^\d+\.\s+', line):
            if in_list: lines.append("</ul>"); in_list = False
            m = re.match(r'^(\d+)\.\s+(.+)$', line)
            if m:
                lines.append(
                    f'<div style="background:white;border-left:3px solid #00C896;border-radius:6px;'
                    f'padding:12px 16px;margin:10px 0;display:flex;gap:12px;align-items:flex-start;">'
                    f'<div style="background:#00C896;color:white;font-family:Syne,sans-serif;font-weight:800;'
                    f'border-radius:50%;width:26px;height:26px;display:flex;align-items:center;'
                    f'justify-content:center;flex-shrink:0;font-size:13px;">{m.group(1)}</div>'
                    f'<div style="flex:1;line-height:1.65;color:#0B2545;">{m.group(2)}</div></div>')
                continue
        if in_list: lines.append("</ul>"); in_list = False
        lines.append(f'<p style="margin:8px 0;line-height:1.7;">{line}</p>')
    if in_list: lines.append("</ul>")
    return f'<div class="{css}">{"".join(lines)}</div>'


class StepProgress:
    def __init__(self, steps, text=None):
        self._ph = st.empty()
        self._n = max(len(steps), 1)
        self._i = 0
        lang = st.session_state.get("language", "fr")
        self._txt = text or ("Computing..." if lang == "en" else "Calcul en cours...")
        self._ph.progress(0, text=self._txt)
    def step(self, label=None):
        self._i += 1
        self._ph.progress(min(self._i / self._n, 1.0), text=self._txt)
    def done(self):
        self._ph.empty()
