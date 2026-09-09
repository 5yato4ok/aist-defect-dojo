from pathlib import Path

import yaml
from django.test import SimpleTestCase

PROJECT_ROOT = Path(__file__).resolve().parents[2]


class RuntimeComposeConfigurationTests(SimpleTestCase):
    def test_obsolete_services_are_not_part_of_the_runtime_stack(self):
        compose = yaml.safe_load((PROJECT_ROOT / "docker-compose.yml").read_text(encoding="utf-8"))

        self.assertNotIn("context-extractor-mcp", compose["services"])
        self.assertNotIn("pgadmin", compose["services"])
        self.assertNotIn("pgadmin_data", compose["volumes"])
