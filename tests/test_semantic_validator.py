from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

from app.ai.graph import validator_node


def test_validator_rejects_nonexistent_table_or_column():
    state = {
        "intent": "CREATE_VISUAL",
        "action": {
            "operation": "CREATE_VISUAL",
            "dataRoles": {"Category": "Ventas[Region]"},
            "dax": "Total = SUM(Ventas[Monto])",
        },
        "semantic_context": "dummy",
        "semantic_schema": {"FactVentas": ["Region", "Monto"]},
        "retry_count": 0,
    }

    result = asyncio.run(validator_node(state))

    assert result["is_valid"] is False
    assert result["retry_count"] == 1
    assert result["validation_errors"]
    assert "tabla inexistente" in result["validation_errors"][0]


def test_validator_accepts_existing_refs_and_continues_llm_validation():
    state = {
        "intent": "CREATE_VISUAL",
        "action": {
            "operation": "CREATE_VISUAL",
            "dataRoles": {"Category": "FactVentas[Region]"},
            "dax": "Total = SUM(FactVentas[Monto])",
        },
        "semantic_context": "dummy",
        "semantic_schema": {"FactVentas": ["Region", "Monto"]},
        "retry_count": 0,
    }

    with patch("app.ai.graph.call_gemini", new_callable=AsyncMock) as mock_gemini:
        mock_gemini.return_value = {
            "is_valid": True,
            "errors": [],
            "suggestions": [],
            "corrected_dax": "",
        }
        result = asyncio.run(validator_node(state))

    assert result["is_valid"] is True
    assert result["validation_errors"] == []
