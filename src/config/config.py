config = {
    "openai": {
        "api_key": "YOUR_API_KEY"
    },
    "kafka": {
        "sasl.username": "YOUR_USERNAME",
        "sasl.password": "YOUR_PASSWORD",
        "bootstrap.servers": "YOUR_BOOTSTRAP_SERVER",
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'session.timeout.ms': 50000
    },
    "schema_registry": {
        "url": "YOUR_SCHEMA_URL",
        "basic.auth.user.info": "YOUR_AUTH_USER_INFO"

    }
}
