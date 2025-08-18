# scripts/__init__.py
"""
Package marker for 'scripts'.
Ensures `from scripts...` imports work in GitHub Actions and locally.
"""

# (Optional) re-export helpers if present
try:
    from .update_core import update_with_xls  # noqa: F401
except Exception:
    pass