
from collectors.base import extract_skills

def test_detects_existing_skills():
    assert sorted(
        extract_skills("Python, SQL, Apache Spark et Kafka")
    ) == sorted(["Python", "SQL", "Apache Spark", "Kafka"])



def test_detects_java_without_matching_javascript():
    assert extract_skills("Développeur Java") == ["java"]
    assert extract_skills("Développeur JavaScript") == ["javascript"]


def test_detects_skills_followed_by_version_number():
    assert sorted(
        extract_skills("Python3 et Java11")
    ) == sorted(["python", "java"])

    assert extract_skills("Spark3") == ["spark"]



def test_is_case_insensitive_and_deduplicates():
    assert extract_skills("PYTHON puis Python") == ["python"]


def test_empty_input():
    assert extract_skills("") == []
    assert extract_skills(None) == []
