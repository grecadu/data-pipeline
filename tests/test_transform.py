import pytest
from consumer import transform


def test_transform_filters_and_maps():
    click_event = {"id": 42, "type": "click", "value": 7}
    view_event = {"id": 43, "type": "view", "value": 5}

    result = transform(click_event)
    assert result == {"id": 42, "value": 7}

    result2 = transform(view_event)
    assert result2 == {}