"""Tests for study CRUD functions in config.py."""

import yaml
import pytest

from pegasus_v2f.config import (
    get_study_list,
    get_study_by_id,
    add_study_to_yaml,
    remove_study_from_yaml,
    update_study_in_yaml,
    add_trait_to_study,
    remove_trait_from_study,
)


STUDY_A = {
    "id_prefix": "shrine_2023",
    "traits": ["FEV1", "FVC"],
    "genome_build": "GRCh38",
    "gwas_source": "PMID:36539618",
    "ancestry": "European",
}

STUDY_B = {
    "id_prefix": "cho_2024",
    "traits": ["PEF"],
    "genome_build": "GRCh38",
}


class TestGetStudyList:
    def test_no_pegasus_section(self):
        assert get_study_list({"version": 1}) == []

    def test_empty_study(self):
        assert get_study_list({"pegasus": {}}) == []

    def test_list_format(self):
        config = {"pegasus": {"study": [STUDY_A, STUDY_B]}}
        assert len(get_study_list(config)) == 2

    def test_legacy_dict_format(self):
        """Single dict (pre-migration) is wrapped in a list."""
        config = {"pegasus": {"study": STUDY_A}}
        studies = get_study_list(config)
        assert len(studies) == 1
        assert studies[0]["id_prefix"] == "shrine_2023"


class TestGetStudyById:
    def test_found(self):
        config = {"pegasus": {"study": [STUDY_A, STUDY_B]}}
        s = get_study_by_id(config, "shrine_2023")
        assert s is not None
        assert s["id_prefix"] == "shrine_2023"

    def test_not_found(self):
        config = {"pegasus": {"study": [STUDY_A]}}
        assert get_study_by_id(config, "nonexistent") is None


class TestAddStudyToYaml:
    def test_creates_list(self, tmp_path):
        config_path = tmp_path / "v2f.yaml"
        config_path.write_text("version: 1\npegasus: {}\n")
        add_study_to_yaml(config_path, STUDY_A)

        config = yaml.safe_load(config_path.read_text())
        studies = config["pegasus"]["study"]
        assert isinstance(studies, list)
        assert len(studies) == 1
        assert studies[0]["id_prefix"] == "shrine_2023"

    def test_appends(self, tmp_path):
        config_path = tmp_path / "v2f.yaml"
        config_path.write_text("version: 1\npegasus: {}\n")
        add_study_to_yaml(config_path, STUDY_A)
        add_study_to_yaml(config_path, STUDY_B)

        config = yaml.safe_load(config_path.read_text())
        assert len(config["pegasus"]["study"]) == 2

    def test_rejects_duplicate(self, tmp_path):
        config_path = tmp_path / "v2f.yaml"
        config_path.write_text("version: 1\npegasus: {}\n")
        add_study_to_yaml(config_path, STUDY_A)
        with pytest.raises(ValueError, match="already exists"):
            add_study_to_yaml(config_path, STUDY_A)

    def test_rejects_reserved_name(self, tmp_path):
        config_path = tmp_path / "v2f.yaml"
        config_path.write_text("version: 1\npegasus: {}\n")
        with pytest.raises(ValueError, match="reserved"):
            add_study_to_yaml(config_path, {"id_prefix": "list", "traits": ["X"]})

    def test_adds_locus_config(self, tmp_path):
        config_path = tmp_path / "v2f.yaml"
        config_path.write_text("version: 1\npegasus: {}\n")
        add_study_to_yaml(config_path, STUDY_A, locus_config={"window_kb": 500})

        config = yaml.safe_load(config_path.read_text())
        assert config["pegasus"]["locus_definition"]["window_kb"] == 500


class TestRemoveStudyFromYaml:
    def test_removes(self, tmp_path):
        config_path = tmp_path / "v2f.yaml"
        config_path.write_text("version: 1\npegasus: {}\n")
        add_study_to_yaml(config_path, STUDY_A)
        add_study_to_yaml(config_path, STUDY_B)
        remove_study_from_yaml(config_path, "shrine_2023")

        config = yaml.safe_load(config_path.read_text())
        studies = config["pegasus"]["study"]
        assert len(studies) == 1
        assert studies[0]["id_prefix"] == "cho_2024"

    def test_raises_if_missing(self, tmp_path):
        config_path = tmp_path / "v2f.yaml"
        config_path.write_text("version: 1\npegasus: {}\n")
        with pytest.raises(ValueError, match="not found"):
            remove_study_from_yaml(config_path, "nonexistent")


class TestUpdateStudyInYaml:
    def test_updates_field(self, tmp_path):
        config_path = tmp_path / "v2f.yaml"
        config_path.write_text("version: 1\npegasus: {}\n")
        add_study_to_yaml(config_path, STUDY_A)

        update_study_in_yaml(config_path, "shrine_2023", "ancestry", "Multi-ethnic")
        config = yaml.safe_load(config_path.read_text())
        assert config["pegasus"]["study"][0]["ancestry"] == "Multi-ethnic"

    def test_rejects_invalid_key(self, tmp_path):
        config_path = tmp_path / "v2f.yaml"
        config_path.write_text("version: 1\npegasus: {}\n")
        add_study_to_yaml(config_path, STUDY_A)
        with pytest.raises(ValueError, match="not settable"):
            update_study_in_yaml(config_path, "shrine_2023", "traits", "bad")

    def test_rejects_id_prefix_change(self, tmp_path):
        config_path = tmp_path / "v2f.yaml"
        config_path.write_text("version: 1\npegasus: {}\n")
        add_study_to_yaml(config_path, STUDY_A)
        with pytest.raises(ValueError, match="immutable"):
            update_study_in_yaml(config_path, "shrine_2023", "id_prefix", "new_name")

    def test_raises_if_study_missing(self, tmp_path):
        config_path = tmp_path / "v2f.yaml"
        config_path.write_text("version: 1\npegasus: {}\n")
        with pytest.raises(ValueError, match="not found"):
            update_study_in_yaml(config_path, "nonexistent", "ancestry", "X")


class TestAddTraitToStudy:
    def test_adds_trait(self, tmp_path):
        config_path = tmp_path / "v2f.yaml"
        config_path.write_text("version: 1\npegasus: {}\n")
        add_study_to_yaml(config_path, STUDY_A)

        add_trait_to_study(config_path, "shrine_2023", "PEF")
        config = yaml.safe_load(config_path.read_text())
        assert "PEF" in config["pegasus"]["study"][0]["traits"]

    def test_rejects_duplicate_trait(self, tmp_path):
        config_path = tmp_path / "v2f.yaml"
        config_path.write_text("version: 1\npegasus: {}\n")
        add_study_to_yaml(config_path, STUDY_A)
        with pytest.raises(ValueError, match="already exists"):
            add_trait_to_study(config_path, "shrine_2023", "FEV1")

    def test_raises_if_study_missing(self, tmp_path):
        config_path = tmp_path / "v2f.yaml"
        config_path.write_text("version: 1\npegasus: {}\n")
        with pytest.raises(ValueError, match="not found"):
            add_trait_to_study(config_path, "nonexistent", "X")


class TestRemoveTraitFromStudy:
    def test_removes_trait(self, tmp_path):
        config_path = tmp_path / "v2f.yaml"
        config_path.write_text("version: 1\npegasus: {}\n")
        add_study_to_yaml(config_path, STUDY_A)

        remove_trait_from_study(config_path, "shrine_2023", "FVC")
        config = yaml.safe_load(config_path.read_text())
        assert "FVC" not in config["pegasus"]["study"][0]["traits"]
        assert "FEV1" in config["pegasus"]["study"][0]["traits"]

    def test_raises_if_trait_missing(self, tmp_path):
        config_path = tmp_path / "v2f.yaml"
        config_path.write_text("version: 1\npegasus: {}\n")
        add_study_to_yaml(config_path, STUDY_A)
        with pytest.raises(ValueError, match="not found"):
            remove_trait_from_study(config_path, "shrine_2023", "NONEXISTENT")
