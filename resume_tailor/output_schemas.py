"""Strong output schemas for each model step."""

from __future__ import annotations

from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator


class ParseJDOutput(BaseModel):
    requirements: list[str] = Field(default_factory=list, max_length=20)
    responsibilities: list[str] = Field(default_factory=list, max_length=20)
    skills: list[str] = Field(default_factory=list, max_length=40)
    keywords: list[str] = Field(default_factory=list, max_length=20)

    @field_validator("requirements", "responsibilities", "skills", "keywords")
    @classmethod
    def _strip_items(cls, values: list[str]) -> list[str]:
        out = []
        for value in values:
            item = str(value).strip()
            if item:
                out.append(item[:400])
        return out


class RoleRewriteOutput(BaseModel):
    title: str = Field(min_length=2, max_length=180)
    company: str = Field(default="", max_length=180)
    bullets: list[str] = Field(default_factory=list, max_length=6)

    @field_validator("title", "company")
    @classmethod
    def _strip_text(cls, value: str) -> str:
        return str(value or "").strip()

    @field_validator("bullets")
    @classmethod
    def _strip_role_bullets(cls, values: list[str]) -> list[str]:
        out = []
        for value in values:
            item = str(value).strip()
            if item:
                out.append(item[:500])
        return out


class RewriteOutput(BaseModel):
    summary: str = Field(default="", max_length=1800)
    bullets: list[str] = Field(default_factory=list, max_length=12)
    roles: list[RoleRewriteOutput] = Field(default_factory=list, max_length=8)
    skills_line: str = Field(default="", max_length=800)

    @field_validator("summary", "skills_line")
    @classmethod
    def _strip_text(cls, value: str) -> str:
        return str(value or "").strip()

    @field_validator("bullets")
    @classmethod
    def _strip_bullets(cls, values: list[str]) -> list[str]:
        out = []
        for value in values:
            item = str(value).strip()
            if item:
                out.append(item[:500])
        return out

    @model_validator(mode="after")
    def _require_bullets_or_roles(self):
        has_flat_bullets = bool(self.bullets)
        has_role_bullets = any(role.bullets for role in self.roles)
        if not has_flat_bullets and not has_role_bullets:
            raise ValueError("rewrite output must include bullets or roles with bullets")
        if not has_flat_bullets and has_role_bullets:
            self.bullets = [
                b for role in self.roles for b in role.bullets
            ]
        return self


class ValidateOutput(BaseModel):
    unsupported_claims: list[str] = Field(default_factory=list, max_length=20)
    risk_notes: list[str] = Field(default_factory=list, max_length=20)

    @field_validator("unsupported_claims", "risk_notes")
    @classmethod
    def _strip_lines(cls, values: list[str]) -> list[str]:
        out = []
        for value in values:
            item = str(value).strip()
            if item:
                out.append(item[:500])
        return out


def validate_parse_output(payload: dict) -> dict | None:
    try:
        return ParseJDOutput.model_validate(payload).model_dump()
    except ValidationError:
        return None


def validate_rewrite_output(payload: dict) -> dict | None:
    try:
        return RewriteOutput.model_validate(payload).model_dump()
    except ValidationError:
        return None


def validate_validate_output(payload: dict) -> dict | None:
    try:
        return ValidateOutput.model_validate(payload).model_dump()
    except ValidationError:
        return None
