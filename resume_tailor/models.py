"""Pydantic models for resume tailoring APIs."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


FactType = Literal[
    "experience",
    "project",
    "education",
    "skill",
    "award",
    "summary",
    "other",
]


class ProfileFactInput(BaseModel):
    fact_type: FactType = "experience"
    source_section: str = Field(default="")
    raw_text: str = Field(min_length=2, max_length=1200)
    normalized_keywords: list[str] = Field(default_factory=list)
    priority: int = Field(default=50, ge=0, le=100)
    active: bool = True


class ProfileUpsertRequest(BaseModel):
    profile_id: int | None = None
    name: str = Field(min_length=2, max_length=120)
    location: str = Field(default="", max_length=120)
    headline: str = Field(default="", max_length=180)
    constraints: dict[str, str | bool | int | float | list[str]] = Field(
        default_factory=dict
    )
    facts: list[ProfileFactInput] = Field(default_factory=list)


class ProfileResponse(BaseModel):
    id: int
    name: str
    location: str = ""
    headline: str = ""
    constraints: dict = Field(default_factory=dict)
    facts: list[ProfileFactInput] = Field(default_factory=list)
    updated_at: str


class ResumeGenerateRequest(BaseModel):
    jd_text: str = Field(min_length=20, max_length=60000)
    jd_url: str = Field(default="", max_length=2000)
    page_title: str = Field(default="", max_length=400)
    profile_id: int
    target_role: str = Field(default="", max_length=120)
    strictness: Literal["strict", "balanced", "light"] = "light"
    return_pdf_base64: bool = True


class ResumeValidateRequest(BaseModel):
    profile_id: int
    jd_text: str = Field(min_length=20, max_length=60000)
    generated_summary: str = Field(default="", max_length=3000)
    generated_bullets: list[str] = Field(default_factory=list)
    target_role: str = Field(default="", max_length=120)


class ValidationResult(BaseModel):
    unsupported_claims: list[str] = Field(default_factory=list)
    missing_critical_keywords: list[str] = Field(default_factory=list)
    keyword_coverage: float = Field(default=0.0)
    warnings: list[str] = Field(default_factory=list)


class GenerationResult(BaseModel):
    trace_id: str
    profile_id: int
    keyword_coverage: float
    warnings: list[str] = Field(default_factory=list)
    missing_keywords: list[str] = Field(default_factory=list)
    validation: ValidationResult
    filename: str
    pdf_base64: str | None = None
    pdf_mime_type: str = "application/pdf"
    model_route: dict[str, str] = Field(default_factory=dict)
    duration_ms: int
