# -*- coding: utf-8 -*-

from datetime import datetime
import os

import pytest

from DDR import format_json


def test_format_json():
    class Unserializable():
        pass
    data0 = {'a': 1}
    data1 = {'a': datetime(2018,8,30,11,35,54)}
    data2 = {'a': Unserializable()}
    expected0 = '{\n    "a": 1\n}'
    expected1 = '{\n    "a": "2018-08-30T11:35:54"\n}'
    expected3 = '{\n    "a": 1\n}'
    out0 = format_json(data0)
    out1 = format_json(data1)
    assert out0 == expected0
    assert out1 == expected1
    with pytest.raises(TypeError):
        out2 = format_json(data2)
