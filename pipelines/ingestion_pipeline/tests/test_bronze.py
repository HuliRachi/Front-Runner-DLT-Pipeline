
import pytest

from bronze_quality_logic import quarantine_rule


def test_quarantine_rule_combines_multiple_rules():
    rules = {
        "valid_customer_id": "after.customer_id IS NOT NULL",
    }

    result = quarantine_rule(rules)

    assert result == "NOT(after.customer_id IS NOT NULL)"


def test_quarantine_rule_rejects_empty_dict():
   
    with pytest.raises(ValueError, match="requires at least one rule"):
        quarantine_rule({})
