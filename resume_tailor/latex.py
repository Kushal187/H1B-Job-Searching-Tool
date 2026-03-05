"""LaTeX rendering and PDF compilation helpers."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import tempfile
import unicodedata
from pathlib import Path

try:
    from reportlab.lib.pagesizes import LETTER
    from reportlab.pdfgen import canvas
except Exception:  # pragma: no cover - optional fallback dependency
    canvas = None
    LETTER = (612.0, 792.0)

LATEX_ESCAPE_MAP = {
    "\\": r"\textbackslash{}",
    "&": r"\&",
    "%": r"\%",
    "$": r"\$",
    "#": r"\#",
    "_": r"\_",
    "{": r"\{",
    "}": r"\}",
    "~": r"\textasciitilde{}",
    "^": r"\textasciicircum{}",
}
LATEX_ESCAPE_RE = re.compile(r"[\\&%$#_{}~^]")
_MACRO_SPECIAL_RE = re.compile(r"[&%$#_]")

UNICODE_REPLACEMENTS = {
    "\u2010": "-",
    "\u2011": "-",  # non-breaking hyphen
    "\u2012": "-",
    "\u2013": "-",
    "\u2014": "-",
    "\u2015": "-",
    "\u2212": "-",
    "\u2018": "'",
    "\u2019": "'",
    "\u201c": '"',
    "\u201d": '"',
    "\u2026": "...",
    "\u00a0": " ",
}


def _to_ascii_safe(text: str) -> str:
    value = text or ""
    for src, dst in UNICODE_REPLACEMENTS.items():
        value = value.replace(src, dst)
    value = unicodedata.normalize("NFKC", value)
    # Keep resume output TeX-safe and deterministic across engines.
    return value.encode("ascii", "ignore").decode("ascii")


def latex_escape(text: str) -> str:
    clean = _to_ascii_safe(text)
    return LATEX_ESCAPE_RE.sub(lambda m: LATEX_ESCAPE_MAP[m.group(0)], clean)


def _prepare_macro_bullet_text(text: str, max_len: int = 360) -> str:
    """Sanitize model text for safe injection into \\resumeItem{...}.

    This is the ONLY sanitizer that should be applied to macro bullet text.
    Do NOT also call latex_escape on the result — that causes double-escaping
    which breaks brace balance inside \\resumeItem{}.
    """
    value = _to_ascii_safe(text or "")
    value = value.replace("\\", " ")
    value = value.replace("{", "(").replace("}", ")")
    value = value.replace("~", " ").replace("^", " ")
    value = _MACRO_SPECIAL_RE.sub(lambda m: "\\" + m.group(0), value)
    value = re.sub(r"\s+", " ", value).strip()
    return value[:max_len]


def load_template() -> str:
    env_path = os.environ.get("RESUME_LATEX_TEMPLATE_PATH", "").strip()
    if env_path:
        path = Path(env_path)
        if path.exists():
            return path.read_text(encoding="utf-8")

    default = Path(__file__).parent / "templates" / "resume_template.tex"
    return default.read_text(encoding="utf-8")


def _render_placeholder_template(
    template: str,
    *,
    name: str,
    location: str,
    headline: str,
    summary: str,
    bullets: list[str],
    skills_line: str,
) -> str:
    bullets_tex = "\n".join(
        f"\\item {latex_escape(item.strip())}" for item in bullets if item.strip()
    )
    payload = {
        "name": latex_escape(name),
        "location": latex_escape(location),
        "headline": latex_escape(headline),
        "summary": latex_escape(summary),
        "bullets": bullets_tex,
        "skills_line": latex_escape(skills_line),
    }

    tex = template
    for key, value in payload.items():
        tex = tex.replace("{{" + key + "}}", value)
    return tex


def _tokenize_for_match(text: str) -> set[str]:
    value = _to_ascii_safe(text or "").lower()
    tokens = re.findall(r"[a-z0-9+#]+", value)
    stop = {
        "the",
        "and",
        "for",
        "with",
        "from",
        "that",
        "this",
        "into",
        "across",
        "using",
        "used",
        "over",
        "under",
        "about",
        "were",
        "was",
        "are",
        "is",
        "to",
        "of",
        "in",
        "on",
        "by",
        "an",
        "a",
    }
    keep_short = {"ai", "ml", "ui", "ux"}
    out = set()
    for token in tokens:
        if token in stop:
            continue
        if len(token) >= 3 or token in keep_short:
            out.add(token)
    return out


def _assign_bullets_to_roles(
    generated_bullets: list[str],
    role_contexts: list[str],
    role_sizes: list[int],
) -> list[list[str]]:
    """Distribute bullets to role blocks using token overlap with each role context."""
    role_count = len(role_contexts)
    if role_count == 0:
        return []
    clean = [b.strip() for b in generated_bullets if b and b.strip()]
    if not clean:
        return [[] for _ in range(role_count)]

    role_tokens = [_tokenize_for_match(ctx) for ctx in role_contexts]
    if role_count > 1:
        has_signal = False
        for bullet in clean:
            btokens = _tokenize_for_match(bullet)
            if btokens and any(btokens & rtokens for rtokens in role_tokens):
                has_signal = True
                break
        if not has_signal:
            # No role-disambiguation signal: keep edits on the most recent role
            # instead of spreading generic bullets across experiences.
            return [clean] + [[] for _ in range(role_count - 1)]

    caps = []
    for idx in range(role_count):
        size = role_sizes[idx] if idx < len(role_sizes) else 3
        caps.append(max(2, min(size, 4)))

    chunks: list[list[str]] = [[] for _ in range(role_count)]
    for bullet in clean:
        btokens = _tokenize_for_match(bullet)
        best_idx = 0
        best_key = None
        for idx in range(role_count):
            overlap = len(btokens & role_tokens[idx]) if btokens else 0
            has_capacity = len(chunks[idx]) < caps[idx]
            # Prefer roles with available capacity, then stronger overlap,
            # then less-loaded buckets to avoid skew.
            score_key = (
                1 if has_capacity else 0,
                overlap,
                -len(chunks[idx]),
                -idx,
            )
            if best_key is None or score_key > best_key:
                best_key = score_key
                best_idx = idx
        chunks[best_idx].append(bullet)
    return chunks


def _normalize_role_key(text: str) -> str:
    value = _to_ascii_safe(text or "").lower().strip()
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def _make_role_match_key(*, title: str, company: str = "") -> str:
    title_key = _normalize_role_key(title)
    company_key = _normalize_role_key(company)
    if not title_key:
        return ""
    if company_key:
        return f"{company_key}::{title_key}"
    return title_key


ACADEMIC_TITLE_PATTERNS = (
    "teaching assistant",
    "research assistant",
    "teaching fellow",
)


def extract_macro_role_targets(template: str, *, max_roles: int = 6) -> list[dict]:
    """Extract industry role targets from macro templates for role-aware bullet generation.

    Skips academic roles (e.g. Teaching Assistant, Research Assistant) so only
    industry/co-op/internship roles get tailored bullets.
    """
    exp_marker = r"\section{Experience}"
    proj_marker = r"\section{Projects}"
    start = template.find(exp_marker)
    end = template.find(proj_marker)
    if start < 0 or end < 0 or end <= start:
        return []

    exp_section = template[start:end]
    role_re = re.compile(
        r"^[ \t]*\\resumeSubheading\s*"
        r"\{(?P<company>[^}]*)\}\s*"
        r"\{[^}]*\}\s*"
        r"\{(?P<title>[^}]*)\}\s*"
        r"\{[^}]*\}",
        re.M,
    )
    out: list[dict] = []
    for m in role_re.finditer(exp_section):
        title = _to_ascii_safe(m.group("title")).strip()
        company = _to_ascii_safe(m.group("company")).strip()
        if not title:
            continue
        title_lower = title.lower()
        if any(pat in title_lower for pat in ACADEMIC_TITLE_PATTERNS):
            continue
        out.append({"title": title, "company": company})
        if len(out) >= max_roles:
            break
    return out


def _render_macro_resume_template(
    template: str,
    *,
    name: str,
    location: str,
    summary: str,
    bullets: list[str],
    role_bullets: list[dict] | None,
    skills_line: str,
) -> str:
    """Render Jake Gutierrez-style macro template by replacing role bullet blocks."""
    tex = template

    # Update heading name and location if present.
    tex = re.sub(
        r"(\{\\Huge\s+\\scshape\s+)([^}]+)(\})",
        lambda m: m.group(1) + latex_escape(name.strip()) + m.group(3),
        tex,
        count=1,
    )
    if location and "Boston, MA" in tex:
        tex = tex.replace("Boston, MA", latex_escape(location.strip()), 1)

    exp_marker = r"\section{Experience}"
    proj_marker = r"\section{Projects}"
    start = tex.find(exp_marker)
    end = tex.find(proj_marker)
    if start < 0 or end < 0 or end <= start:
        return _render_placeholder_template(
            template,
            name=name,
            location=location,
            headline="",
            summary=summary,
            bullets=bullets,
            skills_line=skills_line,
        )

    exp_section = tex[start:end]
    role_re = re.compile(
        r"(?P<head>^[ \t]*\\resumeSubheading\s*"
        r"\{(?P<company>[^}]*)\}\s*"
        r"\{[^}]*\}\s*"
        r"\{(?P<title>[^}]*)\}\s*"
        r"\{[^}]*\}"
        r"(?:\n[ \t]*%[^\n]*)*"
        r"\n[ \t]*\\resumeItemListStart)"
        r"(?P<body>.*?)"
        r"(?P<tail>^[ \t]*\\resumeItemListEnd)",
        re.S | re.M,
    )
    matches = list(role_re.finditer(exp_section))

    if matches:
        role_bullets = role_bullets or []
        grouped_by_exact_key: dict[str, list[str]] = {}
        grouped_by_title: dict[str, list[str]] = {}
        titles_with_company_specific: set[str] = set()
        for role in role_bullets:
            title = str(role.get("title", ""))
            company = str(role.get("company", ""))
            key = _make_role_match_key(title=title, company=company)
            title_key = _normalize_role_key(title)
            rb = [str(b).strip() for b in role.get("bullets", []) if str(b).strip()]
            if rb and key:
                grouped_by_exact_key[key] = rb
            if rb and title_key and _normalize_role_key(company):
                titles_with_company_specific.add(title_key)
            if rb and title_key and title_key not in grouped_by_title:
                grouped_by_title[title_key] = rb

        title_occurrences: dict[str, int] = {}
        for m in matches:
            title_key = _normalize_role_key(m.group("title"))
            if title_key:
                title_occurrences[title_key] = title_occurrences.get(title_key, 0) + 1
        title_only_consumed: set[str] = set()

        role_limit = min(2, len(matches))
        role_sizes: list[int] = []
        role_contexts: list[str] = []
        for m in matches[:role_limit]:
            body = m.group("body")
            old_count = len(re.findall(r"\\resumeItem\{", body))
            role_sizes.append(old_count if old_count > 0 else 3)
            role_contexts.append(m.group("head") + "\n" + body)
        fallback_chunks = _assign_bullets_to_roles(bullets, role_contexts, role_sizes)

        rebuilt = []
        last = 0
        for idx, m in enumerate(matches):
            rebuilt.append(exp_section[last : m.start()])
            body = m.group("body")
            mapped_items: list[str] = []

            if grouped_by_exact_key or grouped_by_title:
                title_key = _normalize_role_key(m.group("title"))
                exact_key = _make_role_match_key(
                    title=m.group("title"), company=m.group("company")
                )
                if exact_key:
                    mapped_items = grouped_by_exact_key.get(exact_key, [])
                if not mapped_items and title_key:
                    if title_key in titles_with_company_specific:
                        title_only = []
                    else:
                        title_only = grouped_by_title.get(title_key, [])
                    if title_occurrences.get(title_key, 0) <= 1 or title_key not in title_only_consumed:
                        mapped_items = title_only
                        if mapped_items and title_occurrences.get(title_key, 0) > 1:
                            title_only_consumed.add(title_key)
            elif idx < len(fallback_chunks):
                mapped_items = fallback_chunks[idx]

            if mapped_items:
                new_items = []
                for item in mapped_items:
                    safe = _prepare_macro_bullet_text(item)
                    new_items.append(f"    \\resumeItem{{{safe}}}")
                body = "\n" + "\n".join(new_items) + "\n"

            rebuilt.append(m.group("head") + body + m.group("tail"))
            last = m.end()

        rebuilt.append(exp_section[last:])
        exp_section = "".join(rebuilt)

    tex = tex[:start] + exp_section + tex[end:]
    return tex


def render_latex(
    template: str,
    *,
    name: str,
    location: str,
    headline: str,
    summary: str,
    bullets: list[str],
    skills_line: str,
    role_bullets: list[dict] | None = None,
) -> str:
    has_placeholders = all(
        token in template for token in ("{{name}}", "{{summary}}", "{{bullets}}")
    )
    if has_placeholders:
        return _render_placeholder_template(
            template,
            name=name,
            location=location,
            headline=headline,
            summary=summary,
            bullets=bullets,
            skills_line=skills_line,
        )

    if r"\resumeSubheading" in template and r"\section{Experience}" in template:
        return _render_macro_resume_template(
            template,
            name=name,
            location=location,
            summary=summary,
            bullets=bullets,
            role_bullets=role_bullets,
            skills_line=skills_line,
        )

    return _render_placeholder_template(
        template,
        name=name,
        location=location,
        headline=headline,
        summary=summary,
        bullets=bullets,
        skills_line=skills_line,
    )


def _sanitize_for_tectonic(tex_content: str) -> str:
    """Remove pdfTeX-specific unicode lines that commonly break Tectonic runs."""
    out_lines: list[str] = []
    for line in tex_content.splitlines():
        stripped = line.strip()
        if stripped.startswith(r"\input{glyphtounicode}"):
            out_lines.append("% disabled: \\input{glyphtounicode} (incompatible in some tectonic setups)")
            continue
        if stripped.startswith(r"\pdfgentounicode"):
            out_lines.append("% disabled: \\pdfgentounicode=1 (pdfTeX-specific)")
            continue
        out_lines.append(line)
    return "\n".join(out_lines) + ("\n" if tex_content.endswith("\n") else "")


def _build_minimal_pdf(message: str) -> bytes:
    """Build a tiny valid PDF without third-party dependencies."""
    safe = (message or "Resume generation fallback").replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
    content = f"BT /F1 11 Tf 40 740 Td ({safe}) Tj ET"

    objects = [
        "1 0 obj << /Type /Catalog /Pages 2 0 R >> endobj\n",
        "2 0 obj << /Type /Pages /Kids [3 0 R] /Count 1 >> endobj\n",
        (
            "3 0 obj << /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
            "/Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >> endobj\n"
        ),
        f"4 0 obj << /Length {len(content)} >> stream\n{content}\nendstream endobj\n",
        "5 0 obj << /Type /Font /Subtype /Type1 /BaseFont /Helvetica >> endobj\n",
    ]

    chunks: list[bytes] = [b"%PDF-1.4\n"]
    offsets = [0]
    position = len(chunks[0])

    for obj in objects:
        offsets.append(position)
        blob = obj.encode("utf-8")
        chunks.append(blob)
        position += len(blob)

    xref_offset = position
    xref = [f"xref\n0 {len(offsets)}\n".encode("utf-8"), b"0000000000 65535 f \n"]
    for off in offsets[1:]:
        xref.append(f"{off:010d} 00000 n \n".encode("utf-8"))

    trailer = (
        f"trailer << /Size {len(offsets)} /Root 1 0 R >>\nstartxref\n{xref_offset}\n%%EOF\n"
    ).encode("utf-8")
    chunks.extend(xref)
    chunks.append(trailer)
    return b"".join(chunks)


def compile_to_pdf(tex_content: str, *, trace_id: str) -> tuple[bytes, list[str]]:
    warnings: list[str] = []
    tectonic_bin = os.environ.get("TECTONIC_BIN", "tectonic")

    with tempfile.TemporaryDirectory(prefix="resume-tailor-") as tmpdir:
        tmp = Path(tmpdir)
        tex_path = tmp / "resume.tex"
        tex_path.write_text(_sanitize_for_tectonic(tex_content), encoding="utf-8")

        if shutil.which(tectonic_bin):
            cmd = [
                tectonic_bin,
                str(tex_path),
                "--outdir",
                str(tmp),
                "--keep-logs",
                "--keep-intermediates",
            ]
            try:
                subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=45)
                pdf_bytes = (tmp / "resume.pdf").read_bytes()
                return pdf_bytes, warnings
            except subprocess.CalledProcessError as exc:
                stderr = (exc.stderr or "").strip().splitlines()
                detail = stderr[-1] if stderr else str(exc)
                log_detail = ""
                log_path = tmp / "resume.log"
                if log_path.exists():
                    try:
                        log_lines = log_path.read_text(encoding="utf-8", errors="ignore").splitlines()
                        tex_errors = [ln.strip() for ln in log_lines if ln.strip().startswith("!")]
                        if tex_errors:
                            log_detail = tex_errors[-1]
                    except Exception:
                        log_detail = ""
                if log_detail:
                    warnings.append(f"Tectonic compile failed: {log_detail}")
                else:
                    warnings.append(f"Tectonic compile failed: {detail}")
            except Exception as exc:
                warnings.append(f"Tectonic compile failed: {exc}")
        else:
            warnings.append("Tectonic not found on server; using fallback PDF renderer.")

        # Fallback single-page PDF so extension flow still works.
        if canvas is None:
            warnings.append("reportlab unavailable; returning minimal fallback PDF.")
            return _build_minimal_pdf(
                "Resume generation fallback. Install tectonic for template-accurate PDF."
            ), warnings

        out_path = tmp / f"{trace_id}.pdf"
        c = canvas.Canvas(str(out_path), pagesize=LETTER)
        width, height = LETTER
        y = height - 50
        c.setFont("Helvetica-Bold", 14)
        c.drawString(40, y, "Resume Generation Fallback")
        y -= 25
        c.setFont("Helvetica", 10)
        for line in [
            "Primary LaTeX compilation is currently unavailable.",
            "Install tectonic and set TECTONIC_BIN to enable template-accurate PDFs.",
            "",
            "Rendered TeX preview:",
        ]:
            c.drawString(40, y, line)
            y -= 14

        c.setFont("Courier", 7)
        for raw_line in tex_content.splitlines()[:120]:
            line = raw_line[:130]
            if y < 40:
                c.showPage()
                c.setFont("Courier", 7)
                y = height - 40
            c.drawString(35, y, line)
            y -= 10

        c.save()
        return out_path.read_bytes(), warnings
