#!/usr/bin/env python3
"""
api_contract_validator.py
=========================
Валидирует ответ API по YAML-контракту.

Работает с JSON, а не с SQL. Поэтому правила записываются на Python
(в YAML как строка). Поддерживаются базовые проверки:
  - type (string, integer, number, boolean, object, array)
  - nullable (если False — поле не должно быть None или отсутствовать)
  - constraints (простые выражения на Python с {val})

Возвращает:
  - (True, []) если всё ок
  - (False, [список ошибок]) если есть нарушения
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml

# ---------------------------------------------------------------- helpers

TYPE_MAP = {
    "string": str,
    "integer": int,
    "number": (int, float),
    "boolean": bool,
    "object": dict,
    "array": list,
}


def check_type(value: Any, expected: str) -> bool:
    py_type = TYPE_MAP.get(expected)
    if py_type is None:
        return True  # неизвестный тип — не проверяем
    if expected == "number" and isinstance(value, bool):
        return False  # bool — это подкласс int, но для number не подходит
    return isinstance(value, py_type)


def check_constraints(value: Any, rules: List[Dict]) -> List[str]:
    errors = []
    for rule in rules or []:
        rule_expr = rule.get("rule", "")
        rule_name = rule.get("name", "constraint")
        if not rule_expr:
            continue
        try:
            # Подставляем значение в правило
            # ВАЖНО: используем eval с ограниченным контекстом
            # (это pet-проект, в продакшене — парсер выражений)
            safe_context = {"val": value, "len": len, "None": None}
            ok = eval(rule_expr.replace("{val}", "val"), {"__builtins__": {}}, safe_context)
            if not ok:
                errors.append(f"constraint '{rule_name}' failed: {rule_expr} (value={value!r})")
        except Exception as e:
            errors.append(f"constraint '{rule_name}' error: {e} (value={value!r})")
    return errors


def validate_item(item: Dict, contract: Dict) -> List[str]:
    
    # 1. Проверка полей верхнего уровня
    for field_name, spec in fields.items():
        expected_type = spec.get("type")
        nullable = spec.get("nullable", True)

        if field_name not in item:
            if not nullable:
                errors.append(f"field '{field_name}' is missing (required)")
            continue

        value = item[field_name]
        if value is None:
            if not nullable:
                errors.append(f"field '{field_name}' is None but nullable=False")
            continue

        if not check_type(value, expected_type):
            errors.append(
                f"field '{field_name}': expected {expected_type}, got {type(value).__name__} (value={value!r})"
            )
            continue

        # constraints
        for err in check_constraints(value, spec.get("constraints", [])):
            errors.append(f"field '{field_name}': {err}")

    # 2. Проверка вложенных объектов
    for obj_name, obj_spec in nested.items():
        required = obj_spec.get("required", False)
        if obj_name not in item:
            if required:
                errors.append(f"nested object '{obj_name}' is missing (required)")
            continue

        obj_value = item[obj_name]
        if not isinstance(obj_value, dict):
            errors.append(f"nested object '{obj_name}': expected dict, got {type(obj_value).__name__}")
            continue

        for subfield_name, subfield_spec in (obj_spec.get("fields") or {}).items():
            sub_expected = subfield_spec.get("type")
            sub_nullable = subfield_spec.get("nullable", True)
            if subfield_name not in obj_value:
                if not sub_nullable:
                    errors.append(f"nested '{obj_name}.{subfield_name}' is missing (required)")
                continue
            sub_value = obj_value[subfield_name]
            if sub_value is None:
                if not sub_nullable:
                    errors.append(f"nested '{obj_name}.{subfield_name}' is None but nullable=False")
                continue
            if not check_type(sub_value, sub_expected):
                errors.append(
                    f"nested '{obj_name}.{subfield_name}': expected {sub_expected}, got {type(sub_value).__name__}"
                )

    return errors


def validate_api_response(api_response: Dict, contract_path: Path) -> Tuple[bool, List[str]]:
    """
    api_response — это dict из ответа API (с ключом 'results').
    contract_path — путь к YAML-контракту.
    Возвращает (passed, errors).
    """
    with contract_path.open("r", encoding="utf-8") as f:
        contract_doc = yaml.safe_load(f)
    contract = contract_doc.get("contract") or {}

    results = api_response.get("results")
    if not isinstance(results, list):
        return False, ["'results' is missing or not a list"]

    all_errors = []
    for idx, item in enumerate(results):
        if not isinstance(item, dict):
            all_errors.append(f"item[{idx}]: expected dict, got {type(item).__name__}")
            continue
        item_errors = validate_item(item, contract)
        for err in item_errors:
            all_errors.append(f"item[{idx}]: {err}")

    return len(all_errors) == 0, all_errors


if __name__ == "__main__":
    # CLI для локальной проверки: python scripts/api_contract_validator.py data/sample.json
    import sys

    if len(sys.argv) < 2:
        print("Usage: python api_contract_validator.py <json_file>")
        sys.exit(1)

    json_path = Path(sys.argv[1])
    contract_path = Path(__file__).resolve().parent.parent / "contracts" / "launch_library.yml"

    with json_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    passed, errors = validate_api_response(data, contract_path)
    if passed:
        print("[OK] Contract passed")
        sys.exit(0)
    else:
        print(f"[FAIL] {len(errors)} error(s):")
        for e in errors[:20]:
            print(f"  - {e}")
        sys.exit(1)
