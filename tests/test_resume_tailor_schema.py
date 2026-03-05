from resume_tailor.output_schemas import (
    validate_parse_output,
    validate_rewrite_output,
    validate_validate_output,
)


def test_validate_parse_output_accepts_expected_shape():
    payload = {
        "requirements": ["3+ years backend engineering"],
        "responsibilities": ["Build API services"],
        "skills": ["python", "aws"],
        "keywords": ["python", "aws", "api"],
    }
    parsed = validate_parse_output(payload)
    assert parsed is not None
    assert parsed["keywords"][0] == "python"


def test_validate_rewrite_output_rejects_missing_bullets_and_roles():
    payload = {"summary": "abc", "skills_line": "python, aws"}
    assert validate_rewrite_output(payload) is None


def test_validate_rewrite_output_accepts_role_grouped_shape():
    payload = {
        "summary": "abc",
        "skills_line": "python, aws",
        "roles": [
            {"title": "Software Engineer Intern", "bullets": ["Built APIs in Python"]},
        ],
    }
    parsed = validate_rewrite_output(payload)
    assert parsed is not None
    assert parsed["roles"][0]["title"] == "Software Engineer Intern"


def test_validate_validate_output_sanitizes_lines():
    payload = {
        "unsupported_claims": ["  claim 1  ", ""],
        "risk_notes": ["  review needed  "],
    }
    parsed = validate_validate_output(payload)
    assert parsed is not None
    assert parsed["unsupported_claims"] == ["claim 1"]
    assert parsed["risk_notes"] == ["review needed"]
