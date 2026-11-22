"""Parsing of France Travail salary labels .

The API's `salaire.libelle` field looks like:
    "Annuel de 45000.0 Euros à 55000.0 Euros sur 12.0 mois"
    "Mensuel de 3500.0 Euros sur 13.0 mois"
    "Horaire de 12.5 Euros sur 12.0 mois"
    "Selon profil"

"""
import re

# ─── Conversion assumptions  ──────────
HOURS_PER_YEAR = 35 * 52            # full time: 35 h x 52 weeks
DEFAULT_PAID_MONTHS = 12            # default number of paid months per year
PAID_MONTHS_RANGE = (12, 16)        # "sur 13 mois" = 13th month; outside this range, ignore
PLAUSIBLE_ANNUAL = (10_000, 300_000)  # safety bounds on the gross annual salary

# ─── Regular expressions ──────────────────────────────────────────────────────

_PERIOD_PATTERNS = {
    "yearly": re.compile(r"\bannuel(?:le)?s?\b|\bpar an\b|/\s*an\b|\bannée\b", re.IGNORECASE),
    "monthly": re.compile(r"\bmensuel(?:le)?s?\b|\bpar mois\b|/\s*mois\b", re.IGNORECASE),
    "hourly": re.compile(r"\bhoraire\b|\bpar heure\b|\bde l'heure\b|/\s*h\b", re.IGNORECASE),
}

# "sur 12.0 mois" / "sur 13 mois": number of monthly payments, not a salary.
_PAID_MONTHS = re.compile(r"\bsur\s+(\d+(?:[.,]\d+)?)\s*mois\b", re.IGNORECASE)
# Numbers that are not salaries: "10 % de primes", "pour 35 h".
_PERCENT = re.compile(r"\d+(?:[.,]\d+)?\s*%")
_WORK_HOURS = re.compile(r"\b\d+(?:[.,]\d+)?\s*(?:h|heures?)\b(?!\s*/)", re.IGNORECASE)
# Number: "35 000" (space = thousands separator), "12.5", "45K".
_NUMBER = re.compile(
    r"(\d{1,3}(?: \d{3})+|\d+)(?:[.,](\d+))?(?:\s*(k)(?![a-zà-ÿ]))?", re.IGNORECASE
)


# Between two amounts of a range: "à", "-", "et" (with an optional unit).
_RANGE_CONNECTOR = re.compile(r"^\s*(?:euros?|€)?\s*(?:à|a|-|–|—|et|to)\s*$", re.IGNORECASE)


def _detect_period(text: str) -> str:
    """Return the period of the keyword that appears first in the text."""
    best, best_pos = "unknown", len(text) + 1
    for period, pattern in _PERIOD_PATTERNS.items():
        match = pattern.search(text)
        if match and match.start() < best_pos:
            best, best_pos = period, match.start()
    return best


def _to_float(match: re.Match) -> float:
    integer, decimals, kilo = match.group(1), match.group(2), match.group(3)
    value = float(integer.replace(" ", "") + ("." + decimals if decimals else ""))
    return value * 1000 if kilo else value


def parse_salary_france_travail(salary_text: str | None):
    """Convert a salary label into (salary_min, salary_max, detected_period).

    Amounts are converted to a gross ANNUAL salary. The returned period
    ("yearly", "monthly", "hourly" or "unknown") is that of the original label.
    If the label is unusable, return (None, None, "unknown") rather than guess.
    """
    if not salary_text:
        return None, None, "unknown"

    text = salary_text.replace("\xa0", " ").replace("\u202f", " ")

    # 1. Number of paid months (13th month): removed from the text so it is not read as a salary.
    paid_months = DEFAULT_PAID_MONTHS
    months_match = _PAID_MONTHS.search(text)
    if months_match:
        months = float(months_match.group(1).replace(",", "."))
        if PAID_MONTHS_RANGE[0] <= months <= PAID_MONTHS_RANGE[1]:
            paid_months = months
        text = _PAID_MONTHS.sub(" ", text)

    # 2. Remove percentages and working hours, which are not salaries.
    text = _PERCENT.sub(" ", text)
    text = _WORK_HOURS.sub(" ", text)

    matches = list(_NUMBER.finditer(text))
    if not matches:
        return None, None, "unknown"
    values = [_to_float(m) for m in matches]

    # 3. Period: explicit keyword, otherwise an amount >= 10,000 is necessarily yearly.
    period = _detect_period(text)
    if period == "unknown":
        if values[0] >= PLAUSIBLE_ANNUAL[0]:
            period = "yearly"
        else:
            return None, None, "unknown"   # "2500 Euros": monthly? yearly? we don't guess

    # 4. Convert to annual, then plausibility filter on each amount.
    factor = {"yearly": 1, "monthly": paid_months, "hourly": HOURS_PER_YEAR}[period]
    annual = [round(v * factor, 2) for v in values]
    plausible = [PLAUSIBLE_ANNUAL[0] <= a <= PLAUSIBLE_ANNUAL[1] for a in annual]

    first = next((i for i, ok in enumerate(plausible) if ok), None)
    if first is None:
        return None, None, "unknown"

    # 5. Upper bound: only if the next amount is linked to the first by "à" / "-".
    #    Otherwise it is something else (bonus, allowance...) and it is ignored.
    low, high = annual[first], None
    nxt = first + 1
    if nxt < len(matches) and plausible[nxt]:
        between = text[matches[first].end():matches[nxt].start()]
        if _RANGE_CONNECTOR.match(between):
            high = annual[nxt]

    if high is not None and low > high:
        low, high = high, low
    return low, high, period