import importlib
import pytest


class SqlGlobalOverrideMixin:
    """
    Mixin for automatically patching SQL globals in base test modules.

    Usage:
      - Set `BASE_TEST_CLASS` to the base class whose module defines the globals.
      - Define `SQL_GLOBAL_OVERRIDES` as a dictionary mapping global names to new SQL definitions.
    """

    BASE_TEST_CLASS = None
    SQL_GLOBAL_OVERRIDES = {}

    @pytest.fixture(autouse=True, scope="class")
    def patch_sql_globals(self, request):
        if self.BASE_TEST_CLASS is None:
            raise RuntimeError(
                "SqlGlobalOverrideMixin requires `BASE_TEST_CLASS` to be defined."
            )

        base_module = importlib.import_module(self.BASE_TEST_CLASS.__module__)
        mp = pytest.MonkeyPatch()

        try:
            for global_name, sql_value in self.SQL_GLOBAL_OVERRIDES.items():
                mp.setattr(base_module, global_name, sql_value, raising=False)
        finally:
            # Ensure patches are always reverted after the class completes
            request.addfinalizer(mp.undo)
