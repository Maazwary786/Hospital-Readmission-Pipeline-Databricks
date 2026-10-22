"""
GenAI Clinical Guideline RAG Bot
Compliant with Problem Statement #05:
- Synthetic clinical guideline knowledge base (ICU protocols, SpO2 hypoxia, 30-day readmissions)
- Strict NO-DIAGNOSIS Guardrails: Intercepts and blocks any personalized diagnosis requests
- Semantic / TF-IDF Retrieval Augmented Generation with citations
"""

import os
import sys
import json
import re
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAG_DIR = os.path.join(ROOT_DIR, "genai_rag")
KB_FILE = os.path.join(RAG_DIR, "guidelines_kb.json")

# Synthetic Clinical Guidelines Knowledge Base
CLINICAL_GUIDELINES = [
    {
        "guideline_id": "CLIN-GL-01",
        "topic": "Acute Hypoxemia & Deterioration Protocol",
        "keywords": ["spo2", "hypoxia", "deterioration", "oxygen", "icu", "saturation"],
        "content": (
            "Guideline CLIN-GL-01 (Acute Bedside Hypoxemia Management):\n"
            "1. Clinical Threshold: Sustained SpO2 < 90% for >= 3 consecutive minutes represents acute physiological deterioration.\n"
            "2. Operational SLA: bedside alert latency must be < 10 seconds to notify nursing staff.\n"
            "3. Immediate Action: Initiate supplemental O2 via nasal cannula (2-4 L/min) or Venturi mask; verify sensor placement and pulse amplitude.\n"
            "4. Escalation: ICU escalation response time must occur within < 3 minutes if SpO2 fails to recover > 92%."
        )
    },
    {
        "guideline_id": "CLIN-GL-02",
        "topic": "30-Day Hospital Readmission Reduction (CMS / ClearPath Protocol)",
        "keywords": ["readmission", "30-day", "discharge", "penalty", "prior", "utilization"],
        "content": (
            "Guideline CLIN-GL-02 (Post-Discharge Readmission Mitigation):\n"
            "1. High-Risk Indicators: Patients with >= 2 prior inpatient admissions within 12 months exhibit 3x higher readmission risk (26% vs 8% baseline).\n"
            "2. Chronic Conditions: Circulatory failure (Heart Failure, AMI) and complex Diabetes carry the highest 30-day readmission vulnerability.\n"
            "3. Transition of Care: Mandatory clinical touchpoint within 48 hours of discharge and reconciliation of outpatient medications reduces readmissions by 15%."
        )
    },
    {
        "guideline_id": "CLIN-GL-03",
        "topic": "HIPAA Data Governance & PHI De-identification",
        "keywords": ["hipaa", "privacy", "phi", "masking", "sha-256", "anonymization", "audit"],
        "content": (
            "Guideline CLIN-GL-03 (HIPAA Compliance & Safe Harbor Anonymization):\n"
            "1. Identification Masking: All Direct identifiers (Name, SSN, MRN) must be masked with SHA-256 cryptographic hashing.\n"
            "2. Role-Based Access: Restricted analytics roles must only query 100% masked views in Databricks Unity Catalog.\n"
            "3. Auditability: Delta Lake Time Travel audit logs must be maintained to verify tamper-proof data lineage."
        )
    },
    {
        "guideline_id": "CLIN-GL-04",
        "topic": "Tachycardia & Hemodynamic Instability Protocol",
        "keywords": ["heart rate", "tachycardia", "blood pressure", "shock", "vital signs"],
        "content": (
            "Guideline CLIN-GL-04 (Hemodynamic Deterioration):\n"
            "1. Tachycardia Criteria: Sustained Heart Rate > 120 bpm at rest accompanied by mean arterial pressure (MAP) drop < 65 mmHg.\n"
            "2. Escalation: Rapid response team evaluation, serum lactate measurement, and bedside 12-lead ECG."
        )
    }
]

# Guardrail Patterns: Block personal diagnosis queries
DIAGNOSIS_FORBIDDEN_PATTERNS = [
    r"do i have",
    r"diagnose me",
    r"what disease do i have",
    r"am i sick",
    r"what is my illness",
    r"what cancer do i have",
    r"should i take this medicine",
    r"prescribe me",
    r"tell me what medication to take",
    r"is my heart failing",
    r"do you think i am dying"
]


class RAGProviderError(RuntimeError):
    def __init__(self, message, status_code=None):
        super().__init__(message)
        self.status_code = status_code


class ClinicalGuidelineRAG:
    def __init__(self):
        self._loaded_env_names = set()
        self._load_env_file()
        self.guidelines = CLINICAL_GUIDELINES
        self._init_kb()

    def _load_env_file(self):
        """Reload .env values while preserving environment variables set outside this app."""
        env_file = os.path.join(RAG_DIR, ".env")
        if not os.path.isfile(env_file):
            for name in self._loaded_env_names:
                os.environ.pop(name, None)
            self._loaded_env_names.clear()
            return

        file_values = {}
        with open(env_file, "r", encoding="utf-8") as file:
            for line in file:
                entry = line.strip()
                if not entry or entry.startswith("#") or "=" not in entry:
                    continue
                name, value = entry.split("=", 1)
                name = name.strip()
                value = value.strip()
                if value[:1] in ("'", '"') and value[-1:] == value[:1]:
                    value = value[1:-1]
                if name:
                    file_values[name] = value

        for name in self._loaded_env_names - file_values.keys():
            os.environ.pop(name, None)
        for name, value in file_values.items():
            if name in self._loaded_env_names or name not in os.environ:
                os.environ[name] = value
                self._loaded_env_names.add(name)

    def _init_kb(self):
        with open(KB_FILE, "w", encoding="utf-8") as f:
            json.dump(self.guidelines, f, indent=2)

        self.docs = [g["content"] for g in self.guidelines]
        self.vectorizer = TfidfVectorizer(stop_words='english')
        self.tfidf_matrix = self.vectorizer.fit_transform(self.docs)

    def check_guardrails(self, query: str):
        """Strict No-Diagnosis Guardrail Check"""
        q_lower = query.lower().strip()
        for pattern in DIAGNOSIS_FORBIDDEN_PATTERNS:
            if re.search(pattern, q_lower):
                return False, (
                    "🚫 [STRICT CLINICAL GUARDRAIL ENFORCED]:\n"
                    "As an AI Clinical Assistant operating under HIPAA and Healthcare Quality Guidelines, "
                    "I am strictly prohibited from diagnosing individual patients, evaluating personal symptoms, "
                    "or prescribing medical treatments. Please contact emergency services or consult a licensed physician immediately."
                )
        return True, ""

    @staticmethod
    def _configured_provider(provider=None):
        provider = (provider if provider is not None else os.getenv("RAG_PROVIDER", "auto")).strip().lower()
        if provider not in {"auto", "groq", "gemini", "openrouter", "local"}:
            raise ValueError("Provider must be one of: auto, groq, gemini, openrouter, local.")
        return provider

    def provider_status(self):
        self._load_env_file()
        configured = {
            "groq": bool(os.getenv("GROQ_API_KEY")),
            "gemini": bool(os.getenv("GEMINI_API_KEY")),
            "openrouter": bool(os.getenv("OPENROUTER_API_KEY"))
        }
        preference = self._configured_provider()
        available = [name for name in ("groq", "gemini", "openrouter") if configured[name]]
        selected = preference if preference in available else (available[0] if available else None)
        return {
            "provider": selected or "not-configured",
            "available_providers": available,
            "llm_configured": bool(available),
            "models": {
                "groq": os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile"),
                "gemini": os.getenv("GEMINI_MODEL", "gemini-2.5-flash"),
                "openrouter": os.getenv("OPENROUTER_MODEL", "openrouter/free")
            },
            "note": "Configured keys are not verified until a query is sent."
        }

    @staticmethod
    def _provider_error_message(exc, provider):
        try:
            payload = json.loads(exc.read().decode("utf-8"))
            details = payload.get("error", {})
            message = details.get("message") if isinstance(details, dict) else str(details)
        except (AttributeError, UnicodeDecodeError, json.JSONDecodeError):
            message = ""

        if not message:
            messages = {
                400: "The provider rejected the request. Check the configured model name.",
                401: "The API key is invalid or expired. Replace it with a newly generated key.",
                403: "The API key was rejected or lacks permission for this model. Check the key, project permissions, and model access.",
                404: "The configured model or API endpoint was not found.",
                429: "The provider quota or rate limit was reached. Check billing/quota or try the other provider.",
                500: "The LLM provider is temporarily unavailable."
            }
            message = messages.get(exc.code, f"Provider returned HTTP {exc.code}.")

        for key_name in ("GROQ_API_KEY", "GEMINI_API_KEY", "OPENROUTER_API_KEY"):
            secret = os.getenv(key_name)
            if secret:
                message = message.replace(secret, "[redacted]")
        return f"{provider}: {message}"

    @classmethod
    def _post_json(cls, url, headers, payload, provider="LLM"):
        request = Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={**headers, "Content-Type": "application/json"},
            method="POST"
        )
        try:
            with urlopen(request, timeout=30) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            message = cls._provider_error_message(exc, provider)
            raise RAGProviderError(message, status_code=exc.code) from exc
        except URLError as exc:
            raise RAGProviderError("Could not connect to the LLM provider.") from exc
        except (TimeoutError, json.JSONDecodeError) as exc:
            raise RAGProviderError("LLM provider timed out or returned invalid JSON.") from exc

    @staticmethod
    def _get_json(url, headers, provider="LLM"):
        request = Request(url, headers=headers, method="GET")
        try:
            with urlopen(request, timeout=15) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            message = ClinicalGuidelineRAG._provider_error_message(exc, provider)
            raise RAGProviderError(message, status_code=exc.code) from exc
        except (URLError, TimeoutError, json.JSONDecodeError) as exc:
            raise RAGProviderError(f"Could not discover available {provider} models.") from exc

    @staticmethod
    def _chat_model_candidates(models, preferred):
        model_ids = [
            item["id"]
            for item in models
            if isinstance(item, dict)
            and isinstance(item.get("id"), str)
            and item.get("active", True)
        ]
        non_chat_markers = ("embed", "whisper", "moderation", "speech", "transcri", "realtime")
        model_ids = [
            model_id for model_id in model_ids
            if not any(marker in model_id.lower() for marker in non_chat_markers)
        ]
        return sorted(
            [model_id for model_id in model_ids if model_id != preferred],
            key=lambda model_id: (
                model_id != preferred,
                not any(marker in model_id.lower() for marker in ("llama", "gpt", "qwen", "deepseek")),
                model_id
            )
        )

    def _list_groq_models(self, api_key):
        data = self._get_json(
            "https://api.groq.com/openai/v1/models",
            {"Authorization": f"Bearer {api_key}", "Accept": "application/json"},
            provider="Groq"
        )
        return self._chat_model_candidates(data.get("data", []), os.getenv("GROQ_MODEL", ""))

    def _generate_groq(self, api_key, system_prompt, prompt):
        try:
            return self._generate_groq_once(api_key, system_prompt, prompt)
        except RAGProviderError as exc:
            if exc.status_code not in (403, 404):
                raise

        candidates = self._list_groq_models(api_key)
        failures = []
        for candidate in candidates[:5]:
            try:
                return self._generate_groq_once(api_key, system_prompt, prompt, model=candidate)
            except RAGProviderError as exc:
                if exc.status_code not in (403, 404):
                    raise
                failures.append(f"{candidate}: {exc}")

        if not candidates:
            raise RAGProviderError(
                "Groq did not list any active chat-capable models for this key. "
                "Check the key, account access, and available models."
            )
        raise RAGProviderError(
            "Groq rejected the configured model and discovered alternatives. " + " ".join(failures)
        )

    def _generate_groq_once(self, api_key, system_prompt, prompt, model=None):
        model = model or os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
        data = self._post_json(
            "https://api.groq.com/openai/v1/chat/completions",
            {"Authorization": f"Bearer {api_key}"},
            {
                "model": model,
                "temperature": 0.2,
                "max_tokens": 800,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt}
                ]
            },
            provider="Groq"
        )
        return data["choices"][0]["message"]["content"].strip()

    def _generate_gemini(self, api_key, system_prompt, prompt):
        model = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
        try:
            return self._request_gemini_model(api_key, model, system_prompt, prompt)
        except RAGProviderError as exc:
            if exc.status_code != 404:
                raise

        models = self._get_json(
            "https://generativelanguage.googleapis.com/v1beta/models",
            {"x-goog-api-key": api_key},
            provider="Gemini"
        ).get("models", [])
        supported = [
            item.get("name", "").removeprefix("models/")
            for item in models
            if "generateContent" in item.get("supportedGenerationMethods", [])
            and item.get("name")
        ]
        if not supported:
            raise RAGProviderError(
                "Gemini could not find the configured model and the API key has no models "
                "that support generateContent. Check Gemini API access and GEMINI_MODEL."
            )

        ordered_models = sorted(
            [candidate for candidate in supported if candidate != model],
            key=lambda candidate: (
                candidate != model,
                "flash" not in candidate.lower(),
                candidate
            )
        )
        failures = []
        for candidate in ordered_models:
            try:
                return self._request_gemini_model(api_key, candidate, system_prompt, prompt)
            except RAGProviderError as exc:
                if exc.status_code != 404:
                    raise
                failures.append(candidate)

        attempted = ", ".join(failures)
        raise RAGProviderError(
            f"Gemini returned 404 for every listed generateContent model ({attempted}). "
            "Check that the key belongs to a project with the Gemini API enabled."
        )

    def _request_gemini_model(self, api_key, model, system_prompt, prompt):
        url = (
            "https://generativelanguage.googleapis.com/v1beta/models/"
            f"{quote(model, safe='')}:generateContent"
        )
        data = self._post_json(
            url,
            {"x-goog-api-key": api_key},
            {
                "system_instruction": {"parts": [{"text": system_prompt}]},
                "contents": [{"role": "user", "parts": [{"text": prompt}]}],
                "generationConfig": {"temperature": 0.2, "maxOutputTokens": 800}
            },
            provider="Gemini"
        )
        return "".join(
            part.get("text", "")
            for part in data["candidates"][0]["content"]["parts"]
        ).strip()

    def _generate_openrouter(self, api_key, system_prompt, prompt):
        model = os.getenv("OPENROUTER_MODEL", "openrouter/free")
        data = self._post_json(
            "https://openrouter.ai/api/v1/chat/completions",
            {"Authorization": f"Bearer {api_key}"},
            {
                "model": model,
                "temperature": 0.2,
                "max_tokens": 800,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt}
                ]
            },
            provider="OpenRouter"
        )
        return data["choices"][0]["message"]["content"].strip()

    def _generate(self, user_question, contexts, provider):
        credentials = {
            "groq": os.getenv("GROQ_API_KEY"),
            "gemini": os.getenv("GEMINI_API_KEY"),
            "openrouter": os.getenv("OPENROUTER_API_KEY")
        }
        preference = self._configured_provider(provider)
        if preference == "auto":
            providers = [name for name in ("groq", "gemini", "openrouter") if credentials[name]]
        else:
            providers = [preference] if credentials[preference] else []

        if not providers:
            if preference == "auto":
                raise RAGProviderError(
                    "No LLM API key is configured. Set GROQ_API_KEY, GEMINI_API_KEY, "
                    "or OPENROUTER_API_KEY in genai_rag/.env."
                )
            raise RAGProviderError(f"{preference.title()} is selected but its API key is not configured.")

        system_prompt = (
            "You are a helpful general-purpose assistant embedded in a healthcare data engineering "
            "project. Answer the user's question directly, including general questions unrelated to "
            "the local guideline excerpts, using your general knowledge where needed. Retrieved "
            "excerpts are project data, may be synthetic, and are not validated clinical advice. "
            "When excerpts are relevant, prioritize them and cite only their supplied IDs in square "
            "brackets; never invent citations. Clearly say when a claim is general knowledge rather "
            "than supported by a project guideline. Do not diagnose or assess an individual's "
            "symptoms/vitals, prescribe, or recommend personalized treatment or medication. If "
            "asked for personal medical advice or urgent care, explain this limitation and direct "
            "them to a licensed clinician or emergency service. Do not claim certainty when unsure."
        )
        retrieved_context = contexts or "No matching project guideline was retrieved."
        prompt = (
            f"Retrieved project guideline excerpts:\n{retrieved_context}\n\n"
            f"User question:\n{user_question}\n\n"
            "Answer helpfully and concisely. If no excerpt applies, answer from general knowledge "
            "and do not fabricate a project citation."
        )
        failures = []
        generators = {
            "groq": self._generate_groq,
            "gemini": self._generate_gemini,
            "openrouter": self._generate_openrouter
        }
        for selected in providers:
            try:
                answer = generators[selected](credentials[selected], system_prompt, prompt)
                if not answer:
                    raise RAGProviderError("LLM provider returned an empty answer.")
                return answer, selected
            except (RAGProviderError, KeyError, IndexError, TypeError) as exc:
                detail = str(exc)
                failures.append(detail if detail.lower().startswith(f"{selected}:") else f"{selected.title()}: {detail}")
                if preference != "auto":
                    break

        raise RAGProviderError(" ".join(failures))

    @staticmethod
    def _local_guideline_answer(retrieved, provider_error):
        if not retrieved:
            return (
                "The LLM providers are unavailable, and no matching project guideline was found. "
                "I can't answer this general question without a working provider. "
                f"Provider details: {provider_error}"
            )

        sections = [
            "The LLM providers are unavailable, so this is a direct extract from the local "
            "project guidelines, not a generated answer:"
        ]
        sections.extend(
            f"[{item['guideline_id']}] {item['topic']}\n{item['content']}"
            for item in retrieved
        )
        sections.append(f"Provider details: {provider_error}")
        return "\n\n".join(sections)

    def query(self, user_question: str, provider=None) -> dict:
        self._load_env_file()

        # 1. Guardrail Inspection
        passed, guardrail_msg = self.check_guardrails(user_question)
        if not passed:
            return {
                "question": user_question,
                "guardrail_triggered": True,
                "response": guardrail_msg,
                "citations": [],
                "provider": "guardrail"
            }

        selected_mode = self._configured_provider(provider)

        # 2. Semantic TF-IDF Retrieval
        q_vec = self.vectorizer.transform([user_question])
        scores = cosine_similarity(q_vec, self.tfidf_matrix)[0]
        ranked_indices = scores.argsort()[::-1]
        relevant_indices = [int(idx) for idx in ranked_indices[:2] if scores[idx] >= 0.05]
        best_score = float(scores[ranked_indices[0]])

        retrieved = [self.guidelines[idx] for idx in relevant_indices]
        contexts = "\n\n".join(
            f"[{guideline['guideline_id']}] {guideline['topic']}\n{guideline['content']}"
            for guideline in retrieved
        )
        if selected_mode == "local":
            if not retrieved:
                return {
                    "question": user_question,
                    "guardrail_triggered": False,
                    "response": (
                        "Local guideline mode only answers from this project's clinical knowledge base, "
                        "and no matching guideline was found. Choose Auto, Groq, Gemini, "
                        "or OpenRouter for general questions."
                    ),
                    "citations": [],
                    "relevance_score": round(best_score, 3),
                    "answer_basis": "local_retrieval_only",
                    "provider": "local-guidelines"
                }
            return {
                "question": user_question,
                "guardrail_triggered": False,
                "response": "\n\n".join(
                    f"[{item['guideline_id']}] {item['topic']}\n{item['content']}"
                    for item in retrieved
                ),
                "citations": [item["guideline_id"] for item in retrieved],
                "relevance_score": round(best_score, 3),
                "answer_basis": "local_retrieval_only",
                "provider": "local-guidelines"
            }

        try:
            response_text, selected_provider = self._generate(user_question, contexts, provider)
            provider_error = None
        except RAGProviderError as exc:
            response_text = self._local_guideline_answer(retrieved, str(exc))
            selected_provider = "local-guidelines-fallback"
            provider_error = str(exc)

        result = {
            "question": user_question,
            "guardrail_triggered": False,
            "response": response_text,
            "citations": [guideline["guideline_id"] for guideline in retrieved],
            "relevance_score": round(best_score, 3),
            "answer_basis": "project_guidelines_and_general_knowledge" if retrieved else "general_knowledge",
            "provider": selected_provider
        }
        if provider_error:
            result["provider_error"] = provider_error
        return result

if __name__ == "__main__":
    bot = ClinicalGuidelineRAG()
    
    # Test 1: Legitimate clinical guideline query
    print("Test 1: Legitimate query")
    res1 = bot.query("What is the protocol for SpO2 drop below 90% in ICU?")
    print(res1["response"])
    print("-" * 50)
    
    # Test 2: Diagnosis query (Must trigger guardrail)
    print("Test 2: Guardrail test")
    res2 = bot.query("Do I have heart failure? Diagnose me please.")
    print(res2["response"])
