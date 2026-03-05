"""Resume generation and validation pipeline service."""

from __future__ import annotations

import base64
import logging
import os
import time
import uuid

from .bedrock import BedrockOrchestrator, BedrockUsage
from .latex import (
    compile_to_pdf,
    extract_macro_role_targets,
    load_template,
    render_latex,
)
from .models import (
    GenerationResult,
    ProfileResponse,
    ProfileUpsertRequest,
    ResumeGenerateRequest,
    ResumeValidateRequest,
    ValidationResult,
)
from .repository import (
    current_daily_tokens,
    get_profile,
    log_generation_event,
    upsert_profile,
)
from .text_utils import extract_keywords, keyword_coverage, normalize_text

logger = logging.getLogger(__name__)


class ResumeTailorService:
    def __init__(self):
        self.orchestrator = BedrockOrchestrator()
        self.daily_token_budget = int(os.environ.get("RESUME_DAILY_TOKEN_BUDGET", "600000"))
        self.request_token_budget = int(
            os.environ.get("RESUME_REQUEST_TOKEN_BUDGET", "70000")
        )
        self.max_jd_chars = int(os.environ.get("RESUME_MAX_JD_CHARS", "20000"))

    def upsert_profile(self, payload: ProfileUpsertRequest) -> ProfileResponse:
        profile = upsert_profile(payload.model_dump())
        return ProfileResponse(**profile)

    def get_profile(self, profile_id: int | None = None) -> ProfileResponse | None:
        profile = get_profile(profile_id)
        if not profile:
            return None
        return ProfileResponse(**profile)

    def validate(self, payload: ResumeValidateRequest) -> ValidationResult:
        profile = get_profile(payload.profile_id)
        if not profile:
            return ValidationResult(warnings=["Profile not found"])

        usage = BedrockUsage()
        jd = normalize_text(payload.jd_text, max_chars=self.max_jd_chars)
        jd_keywords = extract_keywords(jd, top_k=18)
        generated_blob = "\n".join([payload.generated_summary, *payload.generated_bullets])
        coverage, missing = keyword_coverage(jd_keywords[:10], generated_blob)

        verifier = self.orchestrator.validate(
            jd_keywords=jd_keywords,
            generated_summary=payload.generated_summary,
            generated_bullets=payload.generated_bullets,
            facts=profile["facts"],
            usage=usage,
        )

        return ValidationResult(
            unsupported_claims=verifier.get("unsupported_claims", []),
            missing_critical_keywords=missing,
            keyword_coverage=coverage,
            warnings=verifier.get("risk_notes", []),
        )

    def generate(self, payload: ResumeGenerateRequest) -> GenerationResult:
        trace_id = str(uuid.uuid4())
        started = time.time()
        usage = BedrockUsage()

        profile = get_profile(payload.profile_id)
        if not profile:
            raise ValueError("Profile not found")

        budget_warning = None
        used_today = current_daily_tokens()
        original_enable_bedrock = self.orchestrator.enable_bedrock
        original_rewriter_model = self.orchestrator.rewriter_model
        if used_today >= self.daily_token_budget:
            budget_warning = "Daily token budget exceeded; using deterministic fallback heuristics."
            self.orchestrator.enable_bedrock = False
        elif used_today >= int(self.daily_token_budget * 0.8):
            budget_warning = (
                "Daily token budget above 80%; routing rewrite step to parser-tier model "
                "for cost control."
            )
            self.orchestrator.rewriter_model = self.orchestrator.parser_model

        jd = normalize_text(payload.jd_text, max_chars=self.max_jd_chars)
        template = load_template()
        role_targets = extract_macro_role_targets(template)
        try:
            jd_summary = self.orchestrator.parse_jd(jd, usage)
            jd_keywords = jd_summary.get("keywords") or extract_keywords(jd, top_k=16)
            rewritten = self.orchestrator.rewrite(
                jd_summary,
                profile["facts"],
                usage,
                strictness=payload.strictness,
                role_targets=role_targets,
            )
        finally:
            self.orchestrator.enable_bedrock = original_enable_bedrock
            self.orchestrator.rewriter_model = original_rewriter_model

        summary_text = str(rewritten.get("summary", ""))
        role_bullets = self._normalize_role_bullets(rewritten.get("roles", []))
        bullet_texts = [str(x) for x in rewritten.get("bullets", [])]
        if role_bullets:
            bullet_texts = self._flatten_role_bullets(role_bullets)
        validation_payload = ResumeValidateRequest(
            profile_id=payload.profile_id,
            jd_text=jd,
            generated_summary=summary_text,
            generated_bullets=bullet_texts,
            target_role=payload.target_role,
        )
        validation = self.validate(validation_payload)

        if validation.unsupported_claims:
            logger.info(
                "Validator flagged %d claim(s) for review: %s",
                len(validation.unsupported_claims),
                validation.unsupported_claims[:3],
            )

        model_tokens = usage.input_tokens + usage.output_tokens
        warnings = list(validation.warnings)
        if validation.unsupported_claims:
            warnings.append(
                f"Review suggested: {len(validation.unsupported_claims)} claim(s) may "
                "stretch beyond your documented experience. See unsupported_claims for details."
            )
        warnings.extend(self.orchestrator.drain_warnings())
        if self.orchestrator.enable_bedrock and model_tokens == 0:
            warnings.append(
                "No Bedrock tokens were consumed; fallback logic was used instead of model tailoring."
            )
        if model_tokens > self.request_token_budget:
            warnings.append(
                f"Model token budget warning: request consumed {model_tokens} tokens (limit {self.request_token_budget})."
            )
        if budget_warning:
            warnings.append(budget_warning)

        tex = render_latex(
            template,
            name=profile["name"],
            location=profile.get("location", ""),
            headline=profile.get("headline", ""),
            summary=summary_text,
            bullets=bullet_texts[:10],
            skills_line=rewritten.get("skills_line", ""),
            role_bullets=role_bullets,
        )
        pdf_bytes, compile_warnings = compile_to_pdf(tex, trace_id=trace_id)
        warnings.extend(compile_warnings)

        candidate_text = "\n".join(
            [summary_text, *bullet_texts]
        )
        coverage, missing = keyword_coverage(jd_keywords[:10], candidate_text)

        duration_ms = int((time.time() - started) * 1000)
        status = "ok" if not validation.unsupported_claims else "needs_review"
        log_generation_event(
            trace_id=trace_id,
            status=status,
            latency_ms=duration_ms,
            model_route=self.orchestrator.model_route,
            token_in=usage.input_tokens,
            token_out=usage.output_tokens,
            error_code="",
        )

        filename = f"tailored-resume-{payload.profile_id}-{trace_id[:8]}.pdf"
        return GenerationResult(
            trace_id=trace_id,
            profile_id=payload.profile_id,
            keyword_coverage=coverage,
            warnings=warnings,
            missing_keywords=missing,
            validation=validation,
            filename=filename,
            pdf_base64=(
                base64.b64encode(pdf_bytes).decode("ascii")
                if payload.return_pdf_base64
                else None
            ),
            model_route=self.orchestrator.model_route,
            duration_ms=duration_ms,
        )

    @staticmethod
    def _normalize_role_bullets(raw_roles: list[dict] | None) -> list[dict]:
        roles = []
        for item in raw_roles or []:
            title = str(item.get("title", "")).strip()
            company = str(item.get("company", "")).strip()
            bullets = [str(x).strip() for x in item.get("bullets", []) if str(x).strip()]
            if not title or not bullets:
                continue
            role_item = {"title": title, "bullets": bullets[:4]}
            if company:
                role_item["company"] = company
            roles.append(role_item)
        return roles

    @staticmethod
    def _flatten_role_bullets(role_bullets: list[dict]) -> list[str]:
        out: list[str] = []
        for role in role_bullets:
            out.extend(role.get("bullets", []))
        return out

