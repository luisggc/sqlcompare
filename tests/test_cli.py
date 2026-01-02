import json
import tempfile
import unittest
from pathlib import Path

from typer.testing import CliRunner

from sqlcompare.cli import app


class TestCli(unittest.TestCase):
    def setUp(self) -> None:
        self.runner = CliRunner()
        self.fixtures_dir = Path(__file__).resolve().parent / "fixtures"

    def test_parse_outputs_json(self) -> None:
        result = self.runner.invoke(app, ["parse", str(self.fixtures_dir)])
        self.assertEqual(result.exit_code, 0, result.output)
        payload = json.loads(result.output)
        expected_count = len(list(self.fixtures_dir.rglob("*.sql")))
        self.assertEqual(len(payload), expected_count)
        for item in payload:
            self.assertIn("sql_source", item)
            self.assertNotIn("{{", item["sql_source"])
            self.assertTrue(item["directives"])

    def test_plan_writes_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            plan_dir = Path(temp_dir)
            result = self.runner.invoke(
                app, ["plan", str(self.fixtures_dir), "--plan-dir", str(plan_dir)]
            )
            self.assertEqual(result.exit_code, 0, result.output)
            plan_files = list(plan_dir.glob("*.plan.json"))
            expected_count = len(list(self.fixtures_dir.rglob("*.sql")))
            self.assertEqual(len(plan_files), expected_count)

    def test_plan_writes_json(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            json_path = Path(temp_dir) / "plans.json"
            result = self.runner.invoke(
                app, ["plan", str(self.fixtures_dir), "--json", str(json_path)]
            )
            self.assertEqual(result.exit_code, 0, result.output)
            payload = json.loads(json_path.read_text(encoding="utf-8"))
            expected_count = len(list(self.fixtures_dir.rglob("*.sql")))
            self.assertEqual(len(payload), expected_count)


if __name__ == "__main__":
    unittest.main()
