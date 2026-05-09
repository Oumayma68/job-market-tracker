from dataclasses import dataclass, asdict
from datetime import datetime


@dataclass
class JobOffer:
    """Schéma unifié pour toutes les sources."""
    id: str
    title: str
    company: str
    location: str
    contract_type: str       # CDI, CDD, Freelance...
    salary_min: float | None
    salary_max: float | None
    skills: list[str]        # extraits du titre/description
    source: str              # adzuna, wttj, linkedin...
    url: str
    published_at: datetime
    collected_at: datetime

    def to_dict(self) -> dict:
        d = asdict(self)
        d["published_at"] = self.published_at.isoformat()
        d["collected_at"] = self.collected_at.isoformat()
        return d


# Skills à détecter automatiquement dans le titre/description
SKILLS_KEYWORDS = [
    "python", "spark", "kafka", "airflow", "dbt", "sql", "postgresql",
    "aws", "gcp", "azure", "docker", "kubernetes", "terraform",
    "pandas", "pyspark", "hadoop", "snowflake", "databricks",
    "power bi", "tableau", "looker", "grafana", "elasticsearch",
    "scala", "java", "bash", "git", "ci/cd", "mlflow",
]


def extract_skills(text: str) -> list[str]:
    """Détecte les skills mentionnés dans un texte."""
    text_lower = text.lower()
    return [skill for skill in SKILLS_KEYWORDS if skill in text_lower]

