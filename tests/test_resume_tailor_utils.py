from resume_tailor.latex import extract_macro_role_targets, latex_escape, render_latex
from resume_tailor.text_utils import keyword_coverage


def test_latex_escape_special_chars():
    raw = r"50% growth & impact_1 #ship"
    escaped = latex_escape(raw)
    assert r"\%" in escaped
    assert r"\&" in escaped
    assert r"\_" in escaped
    assert r"\#" in escaped


def test_render_latex_replaces_placeholders():
    template = "Name: {{name}}\nBullets:\n{{bullets}}"
    tex = render_latex(
        template,
        name="Test User",
        location="Boston",
        headline="Engineer",
        summary="Summary",
        bullets=["Built APIs", "Optimized latency"],
        skills_line="Python, SQL",
    )
    assert "{{name}}" not in tex
    assert r"\item Built APIs" in tex
    assert r"\item Optimized latency" in tex


def test_keyword_coverage_returns_missing():
    coverage, missing = keyword_coverage(
        ["python", "aws", "kafka"],
        "Built distributed systems with Python and AWS Lambda",
    )
    assert abs(coverage - (2 / 3)) < 1e-3
    assert missing == ["kafka"]


def test_render_macro_template_injects_tailored_content():
    template = r"""
\begin{center}
{\Huge \scshape OLD NAME}
Boston, MA
\end{center}
\section{Experience}
\resumeSubHeadingListStart
\resumeSubheading{Company A}{2025}{Role A}{City}
\resumeItemListStart
\resumeItem{Old bullet A1}
\resumeItem{Old bullet A2}
\resumeItemListEnd
\resumeSubheading{Company B}{2024}{Role B}{City}
\resumeItemListStart
\resumeItem{Old bullet B1}
\resumeItemListEnd
\resumeSubHeadingListEnd
\section{Projects}
\section{Technical Skills}
\begin{itemize}[leftmargin=0.15in, label={}]
  \small \item {
    \textbf{Languages:} Python, Java \\
  }
\end{itemize}
"""
    tex = render_latex(
        template,
        name="New Name",
        location="Seattle, WA",
        headline="",
        summary="Fact-grounded summary line.",
        bullets=["Tailored A", "Tailored B", "Tailored C"],
        skills_line="kafka, distributed systems",
    )
    assert "New Name" in tex
    assert "Seattle, WA" in tex
    assert r"\resumeItem{Tailored A}" in tex
    assert r"\resumeItem{Tailored B}" in tex
    assert r"\section{Professional Summary}" not in tex
    assert r"\textbf{Targeted Keywords:}" not in tex


def test_render_macro_template_ignores_commented_item_blocks():
    template = r"""
\section{Experience}
\resumeSubHeadingListStart
\resumeSubheading{99 Yards}{2025}{Software Engineer Intern}{NY}
% \resumeItemListStart
% \resumeItem{Commented old bullet}
% \resumeItemListEnd
\resumeItemListStart
\resumeItem{Old bullet}
\resumeItemListEnd
\resumeSubHeadingListEnd
\section{Projects}
"""
    tex = render_latex(
        template,
        name="New Name",
        location="Seattle, WA",
        headline="",
        summary="",
        bullets=["Tailored one", "Tailored two"],
        skills_line="",
    )
    assert tex.count(r"\resumeItemListStart") == tex.count(r"\resumeItemListEnd")
    assert r"\resumeItem{Tailored one}" in tex


def test_render_macro_template_assigns_bullets_to_matching_roles():
    template = r"""
\section{Experience}
\resumeSubHeadingListStart
\resumeSubheading{Company A}{2025}{Android Engineer}{City}
\resumeItemListStart
\resumeItem{Built Android app using Kotlin and Jetpack Compose}
\resumeItemListEnd
\resumeSubheading{Company B}{2024}{AI Engineer}{City}
\resumeItemListStart
\resumeItem{Built RAG services with AWS Lambda and Weaviate}
\resumeItemListEnd
\resumeSubHeadingListEnd
\section{Projects}
"""
    tex = render_latex(
        template,
        name="New Name",
        location="Seattle, WA",
        headline="",
        summary="",
        bullets=[
            "Built retrieval services on AWS Lambda and Weaviate for grounded responses.",
            "Developed Android features in Kotlin and Jetpack Compose for better UX.",
        ],
        skills_line="",
    )
    role_a_start = tex.find(r"\resumeSubheading{Company A}")
    role_b_start = tex.find(r"\resumeSubheading{Company B}")
    role_end = tex.find(r"\resumeSubHeadingListEnd", role_b_start)
    role_a_block = tex[role_a_start:role_b_start]
    role_b_block = tex[role_b_start:role_end]

    assert "Kotlin and Jetpack Compose" in role_a_block
    assert "AWS Lambda and Weaviate" not in role_a_block
    assert "AWS Lambda and Weaviate" in role_b_block
    assert "Kotlin and Jetpack Compose" not in role_b_block


def test_render_macro_template_slots_role_groups_by_title_not_position():
    template = r"""
\section{Experience}
\resumeSubHeadingListStart
\resumeSubheading{Company A}{2025}{Android Engineer}{City}
\resumeItemListStart
\resumeItem{Old A}
\resumeItemListEnd
\resumeSubheading{Company B}{2024}{AI Engineer}{City}
\resumeItemListStart
\resumeItem{Old B}
\resumeItemListEnd
\resumeSubHeadingListEnd
\section{Projects}
"""
    tex = render_latex(
        template,
        name="New Name",
        location="Seattle, WA",
        headline="",
        summary="",
        bullets=[],
        role_bullets=[
            {"title": "AI Engineer", "bullets": ["RAG with AWS Lambda and Weaviate"]},
            {"title": "Android Engineer", "bullets": ["Built Android UI in Kotlin Compose"]},
        ],
        skills_line="",
    )
    role_a_start = tex.find(r"\resumeSubheading{Company A}")
    role_b_start = tex.find(r"\resumeSubheading{Company B}")
    role_end = tex.find(r"\resumeSubHeadingListEnd", role_b_start)
    role_a_block = tex[role_a_start:role_b_start]
    role_b_block = tex[role_b_start:role_end]

    assert "Built Android UI in Kotlin Compose" in role_a_block
    assert "RAG with AWS Lambda and Weaviate" not in role_a_block
    assert "RAG with AWS Lambda and Weaviate" in role_b_block
    assert "Built Android UI in Kotlin Compose" not in role_b_block


def test_extract_macro_role_targets_reads_titles():
    template = r"""
\section{Experience}
\resumeSubHeadingListStart
\resumeSubheading{Company A}{2025}{Android Engineer}{City}
\resumeItemListStart
\resumeItem{Old A}
\resumeItemListEnd
\resumeSubheading{Company B}{2024}{AI Engineer}{City}
\resumeItemListStart
\resumeItem{Old B}
\resumeItemListEnd
\resumeSubHeadingListEnd
\section{Projects}
"""
    roles = extract_macro_role_targets(template, max_roles=2)
    assert roles == [
        {"title": "Android Engineer", "company": "Company A"},
        {"title": "AI Engineer", "company": "Company B"},
    ]


def test_render_macro_template_does_not_reuse_title_only_mapping_on_duplicate_titles():
    template = r"""
\section{Experience}
\resumeSubHeadingListStart
\resumeSubheading{99 Yards}{2025}{Software Engineer Intern}{NY}
\resumeItemListStart
\resumeItem{Old 99}
\resumeItemListEnd
\resumeSubheading{Boeing}{2023}{Software Engineer Intern}{Bangalore}
\resumeItemListStart
\resumeItem{Old Boeing}
\resumeItemListEnd
\resumeSubHeadingListEnd
\section{Projects}
"""
    tex = render_latex(
        template,
        name="New Name",
        location="Seattle, WA",
        headline="",
        summary="",
        bullets=[],
        role_bullets=[
            {
                "title": "Software Engineer Intern",
                "company": "99 Yards",
                "bullets": ["Tailored bullet for 99 Yards"],
            }
        ],
        skills_line="",
    )

    role_99_start = tex.find(r"\resumeSubheading{99 Yards}")
    role_boeing_start = tex.find(r"\resumeSubheading{Boeing}")
    role_end = tex.find(r"\resumeSubHeadingListEnd", role_boeing_start)
    role_99_block = tex[role_99_start:role_boeing_start]
    role_boeing_block = tex[role_boeing_start:role_end]

    assert "Tailored bullet for 99 Yards" in role_99_block
    assert "Tailored bullet for 99 Yards" not in role_boeing_block
    assert "Old Boeing" in role_boeing_block
