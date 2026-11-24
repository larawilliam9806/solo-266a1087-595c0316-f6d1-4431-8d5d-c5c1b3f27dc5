"""Pytest 引导：把项目根目录加入 sys.path。

这样无论用 ``python -m pytest`` 还是直接 ``pytest``，也无论从哪个目录
发起收集，``import app`` 都能成功。
"""

import os
import sys

_ROOT = os.path.dirname(os.path.abspath(__file__))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
