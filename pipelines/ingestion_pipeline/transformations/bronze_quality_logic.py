def quarantine_rule(rules: dict) -> str:
    if not rules:
        raise ValueError("quarantine_rule() requires at least one rule — an empty dict would produce invalid SQL (\"NOT()\").")
    return "NOT({0})".format(" AND ".join(rules.values()))
