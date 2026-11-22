from dataclasses import dataclass, asdict
from datetime import datetime
import re


@dataclass
class JobOffer:
    """Unified schema for all sources."""
    id: str
    title: str
    company: str
    location: str
    contract_type: str       # CDI, CDD, Freelance...
    salary_min: float | None
    salary_max: float | None
    skills: list[str]        # extracted from the title/description
    source: str              # adzuna, wttj, linkedin...
    url: str
    published_at: datetime | None
    collected_at: datetime

    def to_dict(self) -> dict:
        d = asdict(self)
        d["published_at"] = self.published_at.isoformat() if self.published_at else None
        d["collected_at"] = self.collected_at.isoformat()
        return d


# Skills to detect automatically in the title/description

SKILLS_KEYWORDS = [
    # Programming
    "Python",
    "Java",
    "Scala",
    "SQL",

    # Big Data
    "Apache Spark",
    "PySpark",
    "Hadoop",
    "Kafka",
    "Flink",

    # Cloud
    "AWS",
    "Azure",
    "GCP",

    # Data Warehousing
    "Snowflake",
    "Databricks",
    "BigQuery",
    "Redshift",

    # Data Engineering
    "dbt",
    "ETL",
    "ELT",

    # Data Integration & Ingestion
    "Fivetran",
    "Airbyte",

    # Databases & Search
    "PostgreSQL",
    "MySQL",
    "MongoDB",
    "Elasticsearch",

    # Orchestration
    "Airflow",
    "Dagster",
    "Prefect",

    # BI & Visualization
    "Power BI",
    "Tableau",
    "Looker",
    "Grafana",

    # Infrastructure & DevOps
    "Docker",
    "Kubernetes",
    "Terraform",
    "CI/CD",
]



def _compile_skill_pattern(skill: str) -> re.Pattern[str]:
    """Build a pattern that avoids matches in the middle of a word."""
    escaped = re.escape(skill)
    right = r"(?![^\W\d])"

    return re.compile(
        rf"(?<!\w){escaped}{right}",
        re.IGNORECASE
    )


_SKILL_PATTERNS = {
    skill: _compile_skill_pattern(skill)
    for skill in SKILLS_KEYWORDS
}


def extract_skills(text: str) -> list[str]:
    """Return the recognized skills, in their normalized form."""
    if not text:
        return []

    return [
        skill
        for skill in SKILLS_KEYWORDS
        if _SKILL_PATTERNS[skill].search(text)
    ]