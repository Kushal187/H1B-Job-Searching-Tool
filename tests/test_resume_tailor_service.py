from pathlib import Path

import config
from db import database
from resume_tailor.models import ProfileUpsertRequest, ResumeGenerateRequest
import resume_tailor.service as service_module
from resume_tailor.service import ResumeTailorService


def test_profile_upsert_and_generate(monkeypatch, tmp_path: Path):
    db_path = tmp_path / "resume_tailor_test.db"
    monkeypatch.setattr(config, "DB_PATH", str(db_path))
    monkeypatch.setattr(database, "DATABASE_URL", None)
    monkeypatch.setenv("RESUME_ENABLE_BEDROCK", "false")
    monkeypatch.setattr(
        service_module,
        "compile_to_pdf",
        lambda tex_content, trace_id: (b"%PDF-1.4\\n%fake\\n", []),
    )

    database.init_db()
    service = ResumeTailorService()

    profile = service.upsert_profile(
        ProfileUpsertRequest(
            name="Test User",
            location="Boston, MA",
            headline="Software Engineer",
            constraints={"inference_mode": "light"},
            facts=[
                {
                    "fact_type": "experience",
                    "source_section": "Experience",
                    "raw_text": "Built backend APIs in Python and AWS Lambda",
                    "normalized_keywords": ["python", "aws", "lambda"],
                    "priority": 90,
                    "active": True,
                },
                {
                    "fact_type": "project",
                    "source_section": "Projects",
                    "raw_text": "Improved application latency by 30% with query optimization",
                    "normalized_keywords": ["performance", "sql"],
                    "priority": 85,
                    "active": True,
                },
            ],
        )
    )
    assert profile.id > 0
    assert len(profile.facts) == 2

    result = service.generate(
        ResumeGenerateRequest(
            jd_text=(
                "We need a software engineer with Python, AWS, and performance optimization "
                "experience building scalable backend systems."
            ),
            jd_url="https://example.com/job/1",
            page_title="Software Engineer",
            profile_id=profile.id,
            target_role="Software Engineer",
            strictness="balanced",
            return_pdf_base64=True,
        )
    )

    assert result.trace_id
    assert result.filename.endswith(".pdf")
    assert result.pdf_base64
    assert result.keyword_coverage >= 0


def test_normalize_role_bullets_keeps_company():
    roles = ResumeTailorService._normalize_role_bullets(
        [
            {
                "title": "Software Engineer Intern",
                "company": "99 Yards",
                "bullets": ["Built feature A", "Improved flow B"],
            }
        ]
    )
    assert roles == [
        {
            "title": "Software Engineer Intern",
            "company": "99 Yards",
            "bullets": ["Built feature A", "Improved flow B"],
        }
    ]
