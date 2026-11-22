
from collectors.francetravail import parse_salary_france_travail


def test_salary_parser_yearly():
    assert parse_salary_france_travail(
        "Salaire Annuel de 45000.0 Euros à 65000.0 Euros"
    ) == (45000.0, 65000.0, "yearly")


def test_salary_parser_yearly_single_value():
    assert parse_salary_france_travail(
        "Salaire Annuel de 33000.0 Euros"
    ) == (33000.0, None, "yearly")


def test_salary_parser_monthly():
    assert parse_salary_france_travail(
        "Salaire Mensuel de 2800.53 Euros"
    ) == (33606.36, None, "monthly")


def test_salary_parser_monthly_single_value():
    assert parse_salary_france_travail(
        "Salaire Mensuel de 1400.0 Euros"
    ) == (16800.0, None, "monthly")


def test_salary_parser_unknown_labels():
    unknown_labels = [
        "Salaire Rémunération attractive, selon expérience.",
        "Salaire En fonction de votre expérience",
        "Salaire 13ème mois",
        "Salaire 13 ème mois, participation aux bénéfices, accès CE",
        "Salaire A définir selon le profil",
        "Salaire N/A",
        "Selon profil",
    ]

    for label in unknown_labels:
        assert parse_salary_france_travail(label) == (
            None, None, "unknown"
        )


def test_salary_parser_empty():
    assert parse_salary_france_travail("") == (
        None, None, "unknown"
    )


def test_salary_parser_none():
    assert parse_salary_france_travail(None) == (
        None, None, "unknown"
    )


def test_salary_parser_monthly_with_extra_information():
    assert parse_salary_france_travail(
        "Salaire Mensuel de 2800.53 Euros - Variable si heures supplémentaires"
    ) == (33606.36, None, "monthly")


def test_salary_parser_yearly_with_extra_information():
    assert parse_salary_france_travail(
        "Salaire Annuel de 45000.0 Euros à 55000.0 Euros sur 12.0 mois"
    ) == (45000.0, 55000.0, "yearly")


def test_salary_parser_ignores_unknown_period():
    assert parse_salary_france_travail(
        "Salaire de 2000 Euros"
    ) == (None, None, "unknown")
