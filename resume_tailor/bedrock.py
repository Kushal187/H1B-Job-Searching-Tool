"""Bedrock integration with provider-specific adapters and strict output validation."""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass

from .output_schemas import (
    validate_parse_output,
    validate_rewrite_output,
    validate_validate_output,
)
from .text_utils import extract_keywords, split_sentences

logger = logging.getLogger(__name__)

try:
    import boto3
except Exception:  # pragma: no cover - optional runtime dependency
    boto3 = None


@dataclass
class BedrockUsage:
    input_tokens: int = 0
    output_tokens: int = 0


class BedrockOrchestrator:
    """Executes parser/rewriter/validator model calls with graceful fallback."""

    def __init__(self):
        self.region = os.environ.get("BEDROCK_REGION", "us-east-1")
        self.enable_bedrock = (
            os.environ.get("RESUME_ENABLE_BEDROCK", "true").lower()
            in {"1", "true", "yes", "on"}
        )
        self.inference_profile_id = os.environ.get(
            "RESUME_BEDROCK_INFERENCE_PROFILE_ID", ""
        ).strip()
        self.parser_model = os.environ.get(
            "RESUME_PARSER_MODEL", "openai.gpt-oss-20b-1:0"
        )
        self.rewriter_model = os.environ.get(
            "RESUME_REWRITER_MODEL", "anthropic.claude-3-7-sonnet-20250219-v1:0"
        )
        self.validator_model = os.environ.get(
            "RESUME_VALIDATOR_MODEL", self.parser_model
        )
        self.max_tokens = int(os.environ.get("RESUME_MAX_MODEL_TOKENS", "3000"))
        self._client = None
        self._runtime_warnings: list[str] = []

    @property
    def model_route(self) -> dict[str, str]:
        return {
            "parser": self.parser_model,
            "rewriter": self.rewriter_model,
            "validator": self.validator_model,
        }

    def _client_or_none(self):
        if not self.enable_bedrock or boto3 is None:
            if self.enable_bedrock and boto3 is None:
                self._runtime_warnings.append(
                    "boto3 is not available; using non-LLM fallback."
                )
            return None
        if self._client is None:
            self._client = boto3.client("bedrock-runtime", region_name=self.region)
        return self._client

    def drain_warnings(self) -> list[str]:
        warnings = list(self._runtime_warnings)
        self._runtime_warnings = []
        return warnings

    _JD_KEY_ALIASES: dict[str, str] = {
        "qualifications": "requirements",
        "must_have": "requirements",
        "required_qualifications": "requirements",
        "minimum_qualifications": "requirements",
        "duties": "responsibilities",
        "job_duties": "responsibilities",
        "key_responsibilities": "responsibilities",
        "tech_stack": "skills",
        "technologies": "skills",
        "tools": "skills",
        "technical_skills": "skills",
        "tools_and_technologies": "skills",
    }

    @staticmethod
    def _normalize_jd_keys(raw: dict) -> dict:
        out: dict[str, list] = {}
        for k, v in raw.items():
            normalized = BedrockOrchestrator._JD_KEY_ALIASES.get(k, k)
            if isinstance(v, str):
                v = [v]
            if normalized in out and isinstance(out[normalized], list) and isinstance(v, list):
                out[normalized].extend(v)
            else:
                out[normalized] = v
        return out

    def parse_jd(self, jd_text: str, usage: BedrockUsage) -> dict:
        prompt = (
            "You are a technical recruiter assistant. Parse the following job description "
            "and extract structured information.\n\n"
            "Return ONLY valid JSON with exactly these four keys:\n"
            '- "requirements": array of qualification requirements '
            '(e.g. "3+ years Python experience", "BS in Computer Science"). '
            "Extract 4-8 items.\n"
            '- "responsibilities": array of core job duties '
            '(e.g. "Design and implement REST APIs"). Extract 4-8 items.\n'
            '- "skills": array of specific technical skills, tools, and frameworks mentioned '
            '(e.g. "Kubernetes", "React", "PostgreSQL"). Extract 8-15 items.\n'
            '- "keywords": array of 12-16 important terms an ATS system would match on, '
            "including both technical and domain terms.\n\n"
            "Do not include soft skills, company values, or benefits. "
            "Do not wrap in markdown. Return raw JSON only.\n\n"
            "Example output:\n"
            "{\n"
            '  "requirements": ["3+ years Python experience", "BS in Computer Science"],\n'
            '  "responsibilities": ["Design and implement REST APIs", "Build CI/CD pipelines"],\n'
            '  "skills": ["Python", "Kubernetes", "PostgreSQL", "Docker", "AWS"],\n'
            '  "keywords": ["distributed systems", "microservices", "CI/CD", "cloud infrastructure"]\n'
            "}\n\n"
            "Job description:\n"
            f"{jd_text[:14000]}"
        )
        content = self._invoke_best_effort(self.parser_model, prompt, usage)
        raw_json = self._safe_json(content)
        if raw_json is None and content:
            logger.warning(
                "JD parser: model returned %d chars but JSON extraction failed. "
                "First 200 chars: %s",
                len(content),
                content[:200],
            )

        if raw_json is not None:
            raw_json = self._normalize_jd_keys(raw_json)

        parsed, validation_err = validate_parse_output(raw_json or {})
        if parsed:
            if not parsed.get("keywords"):
                parsed["keywords"] = extract_keywords(jd_text, top_k=16)
            return parsed

        if raw_json is not None:
            self._runtime_warnings.append(
                "JD parser model JSON failed schema validation; using heuristic extraction."
            )
            logger.warning(
                "JD parser: validation error: %s | keys received: %s",
                validation_err,
                list(raw_json.keys()),
            )
        else:
            self._runtime_warnings.append(
                "JD parser model output failed validation; using heuristic extraction."
            )
        sentences = split_sentences(jd_text)
        return {
            "requirements": [s for s in sentences[:6]],
            "responsibilities": [s for s in sentences[6:12]],
            "skills": extract_keywords(jd_text, top_k=12),
            "keywords": extract_keywords(jd_text, top_k=16),
        }

    @staticmethod
    def _build_structured_facts(facts: list[dict], max_facts: int = 30) -> str:
        """Group facts by source_section for clearer context in the prompt."""
        by_section: dict[str, list[str]] = {}
        count = 0
        for f in facts:
            if count >= max_facts:
                break
            raw = str(f.get("raw_text", "")).strip()
            if not raw:
                continue
            section = (
                str(f.get("source_section", "")).strip()
                or str(f.get("fact_type", "general")).strip()
            )
            by_section.setdefault(section, []).append(raw)
            count += 1

        if not by_section:
            return "(No candidate facts provided.)"

        parts: list[str] = []
        for section, items in by_section.items():
            parts.append(f"[{section}]")
            for item in items:
                parts.append(f"  - {item}")
        return "\n".join(parts)

    def rewrite(
        self,
        jd_summary: dict,
        facts: list[dict],
        usage: BedrockUsage,
        *,
        strictness: str = "balanced",
        role_targets: list[dict] | None = None,
    ) -> dict:
        role_targets = role_targets or []
        facts_block = self._build_structured_facts(facts, max_facts=20)

        strictness_note = (
            "Do not introduce any claim, company-value statement, or motivation statement "
            "unless explicitly supported by the candidate facts below."
        )
        if strictness == "light":
            strictness_note = (
                "Write in polished, high-impact resume language. Prefer concise, "
                "recruiter-friendly abstractions over literal restatement. You may "
                "abstract upward (e.g. 'national learning platform' instead of a "
                "specific product name) for stronger framing. Preserve all technologies, "
                "metrics, and concrete numerical facts exactly. Do not invent new metrics, "
                "company names, or technologies that are absent from the candidate facts."
            )
        elif strictness == "strict":
            strictness_note = (
                "Use strictly fact-grounded language only. If evidence is missing, omit the claim entirely."
            )

        roles_instruction = ""
        if role_targets:
            roles_json = json.dumps(role_targets, ensure_ascii=True)
            roles_instruction = (
                f"The candidate has these roles on their resume (in order):\n"
                f"{roles_json}\n"
                "Output one item in the roles array for EACH role target, in the same order. "
                "Copy the title and company strings EXACTLY as given.\n"
            )
        else:
            roles_instruction = (
                "No specific roles were provided. Put all bullets in a single roles item "
                'with title "Experience" and company "".\n'
            )

        prompt = (
            "You are a resume tailoring expert. Your job is to rewrite resume bullets "
            "so they are SPECIFICALLY TAILORED to the target job description below. "
            "Every bullet you write must be grounded in the candidate's real facts, "
            "but the EMPHASIS, KEYWORD CHOICE, and FRAMING must shift to match "
            "what THIS specific JD asks for.\n\n"

            "TAILORING STRATEGY (CRITICAL):\n"
            "1. Read the JD requirements, responsibilities, skills, and keywords carefully.\n"
            "2. For each role, pick the candidate facts that BEST match the JD's needs. "
            "Lead with the most JD-relevant achievement in each role.\n"
            "3. Weave JD keywords and technologies into bullets WHERE the candidate "
            "genuinely used them. If the JD says 'microservices' and the candidate "
            "built microservices, use that exact word. If the JD says 'CI/CD' and the "
            "candidate set up pipelines, mention CI/CD explicitly.\n"
            "4. The summary MUST directly address the JD's core requirements — mention "
            "the specific domain, technologies, or role type the JD emphasizes.\n"
            "5. The skills_line MUST be reordered to lead with skills the JD prioritizes, "
            "and should include JD-mentioned skills the candidate actually has.\n"
            "6. If the JD emphasizes a domain (e.g. fintech, healthcare, e-commerce, "
            "AI/ML), frame the candidate's experience toward that domain where honest.\n"
            "7. Two resumes generated for DIFFERENT JDs should look noticeably different "
            "in emphasis, keyword density, and bullet ordering — even though they draw "
            "from the same candidate facts.\n\n"

            "OUTPUT FORMAT:\n"
            "Return ONLY valid JSON (no markdown fences, no commentary) with this exact schema:\n"
            "{\n"
            '  "summary": "A 1-2 sentence professional summary highlighting the candidate\'s '
            'strongest relevant qualifications for THIS SPECIFIC role and JD.",\n'
            '  "roles": [\n'
            '    {"title": "exact role title", "company": "exact company name", '
            '"bullets": ["bullet1", "bullet2", "bullet3"]}\n'
            "  ],\n"
            '  "skills_line": "Comma-separated technical skills, ordered by JD relevance"\n'
            "}\n\n"

            "BULLET WRITING RULES:\n"
            "- Start each bullet with a strong past-tense action verb "
            "(e.g. Built, Designed, Reduced, Implemented, Deployed, Engineered).\n"
            "- Include quantifiable impact (numbers, percentages, scale) ONLY when "
            "directly supported by the candidate facts. Use % for percentages.\n"
            "- Target 20-35 words per bullet. Be concise — the resume must fit on ONE page.\n"
            "- ACTIVELY incorporate keywords from the JD requirements and skills into "
            "bullet phrasing. Don't just mention them — integrate them naturally into "
            "the achievement description.\n"
            "- Write 3 bullets per role (4 only if the role is the most recent or most relevant). "
            "Only include bullets you can fully support with the candidate facts.\n"
            "- Prefer polished, outcome-oriented phrasing over literal restatement of facts. "
            "Frame work in terms of impact and scope rather than raw implementation details.\n"
            "- Do NOT use these special characters: $ { } \\ & # _ ~ ^  "
            "(% is OK for percentages).\n\n"

            f"GROUNDING RULE ({strictness}):\n"
            f"{strictness_note}\n"
            "Every claim must trace back to at least one candidate fact. "
            "If a JD requirement has no matching fact, omit it — do NOT fabricate experience.\n\n"

            "---\n\n"

            "JOB DESCRIPTION SUMMARY (tailor to THIS):\n"
            f"{json.dumps(jd_summary, ensure_ascii=True)}\n\n"

            f"ROLE TARGETS:\n{roles_instruction}\n"

            "CANDIDATE FACTS (grouped by source):\n"
            f"{facts_block}\n"
        )
        content = self._invoke_best_effort(self.rewriter_model, prompt, usage)
        raw_json = self._safe_json(content)
        if raw_json is None and content:
            self._runtime_warnings.append(
                f"Rewriter returned non-JSON output ({len(content)} chars); using fallback."
            )
        parsed = validate_rewrite_output(raw_json or {})
        if parsed:
            return parsed

        if raw_json is not None:
            self._runtime_warnings.append(
                "Rewriter JSON failed schema validation; using fallback."
            )
        elif not content:
            self._runtime_warnings.append(
                "Rewriter returned empty response; using fallback."
            )

        top_bullets = [f.get("raw_text", "").strip() for f in facts if f.get("raw_text")]
        top_bullets = [b for b in top_bullets if b][:8]
        summary = top_bullets[0] if top_bullets else ""
        role_groups: list[dict] = []
        if role_targets:
            scoped = top_bullets[:]
            for idx, role in enumerate(role_targets):
                title = str(role.get("title", "")).strip()
                company = str(role.get("company", "")).strip()
                if not title:
                    continue
                bucket = scoped[idx * 3 : (idx + 1) * 3]
                if not bucket and idx == 0:
                    bucket = scoped[:3]
                role_item: dict = {"title": title, "bullets": bucket}
                if company:
                    role_item["company"] = company
                role_groups.append(role_item)
        skills_line = ", ".join(sorted({k for k in jd_summary.get("keywords", [])[:12]}))
        if not skills_line:
            skills_line = ", ".join(
                sorted({k for f in facts for k in f.get("normalized_keywords", [])[:3]})
            )
        return {
            "summary": summary,
            "bullets": top_bullets,
            "roles": role_groups,
            "skills_line": skills_line,
        }

    def validate(
        self,
        jd_keywords: list[str],
        generated_summary: str,
        generated_bullets: list[str],
        facts: list[dict],
        usage: BedrockUsage,
    ) -> dict:
        facts_block = "\n".join(
            f"- {f.get('raw_text', '')}" for f in facts[:24] if f.get("raw_text")
        )
        bullets_block = "\n".join(f"- {b}" for b in generated_bullets if b.strip())

        prompt = (
            "You are a resume fact-checker. Your job is to verify that generated resume "
            "content is fully grounded in the candidate's real experience facts.\n\n"

            "TASK: Compare each generated line against the candidate facts. Flag any line "
            "that makes a claim not directly supported by the facts.\n\n"

            "Return ONLY valid JSON (no markdown fences) with this schema:\n"
            "{\n"
            '  "unsupported_claims": ["full text of each unsupported line"],\n'
            '  "risk_notes": ["brief explanation of why each flagged line is risky"]\n'
            "}\n\n"

            "RULES:\n"
            "- A claim is unsupported if it states a metric, achievement, or technology "
            "that does not appear in the candidate facts.\n"
            "- Reasonable paraphrasing of a fact is OK (supported).\n"
            "- Combining two real facts into one bullet is OK if both facts are present.\n"
            "- Inventing new numbers, company names, or technologies is NOT OK.\n"
            "- If everything is supported, return empty arrays.\n\n"

            "GENERATED SUMMARY:\n"
            f"{generated_summary}\n\n"
            "GENERATED BULLETS:\n"
            f"{bullets_block}\n\n"
            "CANDIDATE FACTS:\n"
            f"{facts_block}\n"
        )
        content = self._invoke_best_effort(self.validator_model, prompt, usage)
        parsed = validate_validate_output(self._safe_json(content) or {})
        if parsed:
            return parsed

        self._runtime_warnings.append(
            "Validator model output failed; using local grounding check."
        )
        fact_text = "\n".join(f.get("raw_text", "") for f in facts).lower()
        unsupported = []
        for line in [generated_summary, *generated_bullets]:
            check = (line or "").strip().lower()
            if len(check) < 20:
                continue
            tokens = [t for t in check.split() if len(t) > 3]
            overlaps = sum(1 for token in tokens if token in fact_text)
            if overlaps < max(4, len(tokens) // 4):
                unsupported.append(line)
        return {"unsupported_claims": unsupported, "risk_notes": []}

    def _invoke_best_effort(self, model_id: str, prompt: str, usage: BedrockUsage) -> str:
        client = self._client_or_none()
        if client is None:
            return ""

        provider = model_id.split(".", 1)[0]

        strategies = [
            lambda: self._invoke_converse(client, model_id, prompt, usage),
        ]
        if provider == "anthropic":
            strategies.append(lambda: self._invoke_anthropic_messages(client, model_id, prompt, usage))
        elif provider == "amazon":
            strategies.append(lambda: self._invoke_amazon_legacy(client, model_id, prompt, usage))
        elif provider == "openai":
            strategies.append(lambda: self._invoke_openai_chat(client, model_id, prompt, usage))
            strategies.append(lambda: self._invoke_openai_responses(client, model_id, prompt, usage))

        # universal fallback for any model that accepts legacy text payload
        strategies.append(lambda: self._invoke_amazon_legacy(client, model_id, prompt, usage))

        errors: list[str] = []
        for call in strategies:
            try:
                text = call()
                if text:
                    return text
            except Exception as exc:  # pragma: no cover - network/runtime dependency
                logger.warning("Bedrock invocation strategy failed for %s: %s", model_id, exc)
                errors.append(str(exc))

        if errors:
            self._runtime_warnings.append(
                f"Bedrock call failed for {model_id}; using fallback. Last error: {errors[-1]}"
            )
        return ""

    def _supports_inference_profile_retry(self, exc: Exception) -> bool:
        msg = str(exc).lower()
        return "inference profile" in msg and "on-demand throughput" in msg

    def _maybe_retry_with_profile(self, model_id: str, call):
        try:
            return call(model_id)
        except Exception as exc:
            if (
                self.inference_profile_id
                and self.inference_profile_id != model_id
                and self._supports_inference_profile_retry(exc)
            ):
                self._runtime_warnings.append(
                    f"Retrying model call with inference profile: {self.inference_profile_id}"
                )
                return call(self.inference_profile_id)
            raise

    def _invoke_converse(self, client, model_id: str, prompt: str, usage: BedrockUsage) -> str:
        def _call(target_model_id: str):
            return client.converse(
                modelId=target_model_id,
                messages=[
                    {
                        "role": "user",
                        "content": [{"text": prompt}],
                    }
                ],
                inferenceConfig={
                    "temperature": 0.05,
                    "topP": 0.9,
                    "maxTokens": self.max_tokens,
                },
            )

        response = self._maybe_retry_with_profile(model_id, _call)
        usage_info = response.get("usage", {})
        usage.input_tokens += int(usage_info.get("inputTokens", max(len(prompt) // 4, 1)))
        usage.output_tokens += int(usage_info.get("outputTokens", 1))

        output = response.get("output", {})
        message = output.get("message", {})
        content = message.get("content", [])
        if isinstance(content, list):
            texts = [item.get("text", "") for item in content if isinstance(item, dict)]
            return "\n".join(t for t in texts if t).strip()
        return ""

    def _invoke_anthropic_messages(self, client, model_id: str, prompt: str, usage: BedrockUsage) -> str:
        body = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": self.max_tokens,
            "temperature": 0.05,
            "messages": [
                {
                    "role": "user",
                    "content": [{"type": "text", "text": prompt}],
                }
            ],
        }
        def _call(target_model_id: str):
            return client.invoke_model(modelId=target_model_id, body=json.dumps(body))

        response = self._maybe_retry_with_profile(model_id, _call)
        payload = self._decode_body(response)
        parsed = self._safe_json(payload) or {}

        usage_data = parsed.get("usage", {})
        usage.input_tokens += int(usage_data.get("input_tokens", max(len(prompt) // 4, 1)))
        usage.output_tokens += int(usage_data.get("output_tokens", max(len(payload) // 8, 1)))

        content = parsed.get("content", [])
        if isinstance(content, list):
            texts = []
            for item in content:
                if isinstance(item, dict) and item.get("type") == "text":
                    texts.append(str(item.get("text", "")))
            return "\n".join(t for t in texts if t).strip()
        return ""

    def _invoke_openai_responses(self, client, model_id: str, prompt: str, usage: BedrockUsage) -> str:
        body = {
            "input": [
                {
                    "role": "user",
                    "content": [{"type": "input_text", "text": prompt}],
                }
            ],
            "max_output_tokens": self.max_tokens,
            "temperature": 0.05,
        }
        def _call(target_model_id: str):
            return client.invoke_model(modelId=target_model_id, body=json.dumps(body))

        response = self._maybe_retry_with_profile(model_id, _call)
        payload = self._decode_body(response)
        parsed = self._safe_json(payload) or {}

        usage_data = parsed.get("usage", {})
        usage.input_tokens += int(usage_data.get("input_tokens", max(len(prompt) // 4, 1)))
        usage.output_tokens += int(usage_data.get("output_tokens", max(len(payload) // 8, 1)))

        output = parsed.get("output", [])
        if isinstance(output, list):
            texts = []
            for item in output:
                if not isinstance(item, dict):
                    continue
                for content in item.get("content", []):
                    if isinstance(content, dict):
                        if content.get("type") in {"output_text", "text"}:
                            texts.append(str(content.get("text", "")))
            return "\n".join(t for t in texts if t).strip()

        return self._extract_text(payload)

    def _invoke_openai_chat(self, client, model_id: str, prompt: str, usage: BedrockUsage) -> str:
        body = {
            "messages": [
                {
                    "role": "user",
                    "content": prompt,
                }
            ],
            "max_tokens": self.max_tokens,
            "temperature": 0.05,
        }

        def _call(target_model_id: str):
            return client.invoke_model(modelId=target_model_id, body=json.dumps(body))

        response = self._maybe_retry_with_profile(model_id, _call)
        payload = self._decode_body(response)
        parsed = self._safe_json(payload) or {}

        usage_data = parsed.get("usage", {})
        usage.input_tokens += int(
            usage_data.get("prompt_tokens", max(len(prompt) // 4, 1))
        )
        usage.output_tokens += int(
            usage_data.get("completion_tokens", max(len(payload) // 8, 1))
        )

        choices = parsed.get("choices", [])
        if isinstance(choices, list) and choices:
            message = choices[0].get("message", {}) if isinstance(choices[0], dict) else {}
            content = message.get("content", "")
            if isinstance(content, list):
                texts = []
                for part in content:
                    if isinstance(part, dict) and part.get("type") in {"text", "output_text"}:
                        texts.append(str(part.get("text", "")))
                return "\n".join(t for t in texts if t).strip()
            if isinstance(content, str):
                return content.strip()

        return self._extract_text(payload)

    def _invoke_amazon_legacy(self, client, model_id: str, prompt: str, usage: BedrockUsage) -> str:
        body = {
            "inputText": prompt,
            "textGenerationConfig": {
                "maxTokenCount": self.max_tokens,
                "temperature": 0.05,
                "topP": 0.9,
            },
        }
        def _call(target_model_id: str):
            return client.invoke_model(modelId=target_model_id, body=json.dumps(body))

        response = self._maybe_retry_with_profile(model_id, _call)
        payload = self._decode_body(response)
        usage.input_tokens += max(len(prompt) // 4, 1)
        usage.output_tokens += max(len(payload) // 8, 1)
        return self._extract_text(payload)

    @staticmethod
    def _decode_body(response: dict) -> str:
        raw = response.get("body")
        if hasattr(raw, "read"):
            return raw.read().decode("utf-8")
        return str(raw)

    @staticmethod
    def _extract_text(payload: str) -> str:
        parsed = BedrockOrchestrator._safe_json(payload)
        if not parsed:
            return payload

        for key in ("results", "output", "outputs", "content"):
            value = parsed.get(key)
            if isinstance(value, list) and value:
                item = value[0]
                if isinstance(item, dict):
                    for text_key in ("outputText", "text"):
                        if text_key in item:
                            return str(item[text_key])
                return str(item)
            if isinstance(value, str):
                return value

        return json.dumps(parsed)

    @staticmethod
    def _safe_json(text: str) -> dict | None:
        text = (text or "").strip()
        if not text:
            return None

        def _try_parse(s: str) -> dict | None:
            if not s:
                return None
            s = s.strip()
            try:
                return json.loads(s)
            except json.JSONDecodeError:
                pass
            fixed = re.sub(r",\s*([}\]])", r"\1", s)
            if fixed != s:
                try:
                    return json.loads(fixed)
                except json.JSONDecodeError:
                    pass
            return None

        result = _try_parse(text)
        if result:
            return result

        stripped = text
        if "```" in stripped:
            match = re.search(r"```(?:json)?\s*\n?(.*?)```", stripped, re.DOTALL)
            if match:
                stripped = match.group(1).strip()
                result = _try_parse(stripped)
                if result:
                    return result

        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            result = _try_parse(text[start : end + 1])
            if result:
                return result

        return None
