import pytest
import mongomock
from unittest.mock import patch
from app import create_app
from app.core.config import DevelopmentConfig


class TestConfig(DevelopmentConfig):
    TESTING = True
    MONGO_URI = "mongodb://localhost:27017/test_imdb"
    PAGE_SIZE_DEFAULT = 20
    PAGE_SIZE_MAX = 100


@pytest.fixture(scope="session")
def mock_db():
    """Session-scoped mongomock database."""
    client = mongomock.MongoClient()
    db = client["test_imdb"]
    return db


@pytest.fixture(autouse=True)
def patch_get_db(mock_db):
    with patch("app.core.database.get_db", return_value=mock_db):
        with patch("app.services.csv_service.get_db", return_value=mock_db):
            with patch("app.services.movie_service.get_db", return_value=mock_db):
                yield


@pytest.fixture(scope="session")
def app(mock_db):
    application = create_app(TestConfig)
    application.config["TESTING"] = True
    return application


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture(autouse=True)
def clean_collections(mock_db):
    yield
    mock_db["movies"].drop()
    mock_db["upload_jobs"].drop()
