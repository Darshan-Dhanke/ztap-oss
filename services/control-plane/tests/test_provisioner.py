import pytest

from app.provisioner import validate_name, ProvisionError, ProjectState


@pytest.mark.parametrize("name", ["analytics", "a1", "my_project", "x_2_y"])
def test_valid_names(name):
    validate_name(name)  # should not raise


@pytest.mark.parametrize("name", ["", "1abc", "Abc", "with-dash", "a", "with space", "x" * 60])
def test_invalid_names(name):
    with pytest.raises(ProvisionError):
        validate_name(name)


def test_project_state_serializes():
    s = ProjectState(name="demo", status="ready", schema="proj_demo")
    d = s.to_dict()
    assert d["name"] == "demo"
    assert d["status"] == "ready"
    assert d["schema"] == "proj_demo"
    assert "steps_completed" in d
