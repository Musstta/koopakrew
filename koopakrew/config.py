import os


class BaseConfig:
    """Shared defaults for every environment."""

    SECRET_KEY = os.environ.get("KOOPAKREW_SECRET")
    DATABASE_FILENAME = os.environ.get("KOOPAKREW_DB_FILENAME", "koopakrew.db")
    LOCAL_TIMEZONE = os.environ.get("KOOPAKREW_TZ", "America/Costa_Rica")
    JSON_SORT_KEYS = False
    GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
    MISTRAL_API_KEY = os.environ.get("MISTRAL_API_KEY")


class DevelopmentConfig(BaseConfig):
    ENV = "development"
    DEBUG = True
    SECRET_KEY = os.environ.get("KOOPAKREW_SECRET", "koopakrew-dev-secret")
    DATABASE_FILENAME = os.environ.get("KOOPAKREW_DB_FILENAME", "dev.sqlite")


class TestingConfig(BaseConfig):
    ENV = "testing"
    TESTING = True
    SECRET_KEY = "koopakrew-test-secret"
    DATABASE_FILENAME = "koopakrew-test.db"


class ProductionConfig(BaseConfig):
    ENV = "production"
    DEBUG = False
    TESTING = False
    DATABASE_FILENAME = os.environ.get("KOOPAKREW_DB_FILENAME", "koopakrew.sqlite")


CONFIG_MAP = {
    "baseconfig": BaseConfig,
    "base": BaseConfig,
    "developmentconfig": DevelopmentConfig,
    "development": DevelopmentConfig,
    "dev": DevelopmentConfig,
    "testingconfig": TestingConfig,
    "testing": TestingConfig,
    "test": TestingConfig,
    "productionconfig": ProductionConfig,
    "production": ProductionConfig,
    "prod": ProductionConfig,
}


__all__ = [
    "BaseConfig",
    "DevelopmentConfig",
    "TestingConfig",
    "ProductionConfig",
    "CONFIG_MAP",
]
