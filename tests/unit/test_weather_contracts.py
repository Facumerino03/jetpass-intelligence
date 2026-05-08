from app.core.config import Settings
from app.intelligence.contracts import OrchestratorRequest, OrchestratorResponse, WeatherIntelResult


def test_weather_intent_is_accepted_by_orchestrator_request():
    request = OrchestratorRequest.model_validate(
        {"weather": {"icao": "saez", "force_refresh": True}}
    )

    assert request.weather is not None
    assert request.weather.icao == "saez"
    assert request.weather.force_refresh is True


def test_orchestrator_response_can_include_weather_result():
    weather = WeatherIntelResult(icao="SAEZ", source="cache")
    response = OrchestratorResponse(intent="weather_context", weather=weather)

    assert response.weather is not None
    assert response.weather.icao == "SAEZ"
    assert response.weather.source == "cache"


def test_metar_hours_back_defaults_to_none_and_can_be_set():
    request = OrchestratorRequest.model_validate(
        {"weather": {"icao": "saez"}}
    )

    assert request.weather is not None
    assert request.weather.metar_hours_back is None

    request = OrchestratorRequest.model_validate(
        {"weather": {"icao": "saez", "metar_hours_back": 5.0}}
    )

    assert request.weather.metar_hours_back == 5.0


def test_weather_settings_defaults_are_operational():
    settings = Settings()

    assert settings.aviation_weather_base_url == "https://aviationweather.gov/api/data"
    assert settings.weather_station_cache_ttl_seconds == 604800
    assert settings.weather_metar_cache_ttl_seconds == 120
    assert settings.weather_taf_cache_ttl_seconds == 600
    assert settings.weather_sigmet_cache_ttl_seconds == 120
    assert settings.weather_metar_hours_back == 2.0
    assert settings.weather_http_timeout_seconds == 10.0
    assert settings.weather_user_agent == "jetpass-intelligence"
