import yaml
import json
from app.main import app

# Generate OpenAPI schema
openapi_schema = app.openapi()

# Write to YAML file
with open('swagger.yml', 'w') as f:
    yaml.dump(openapi_schema, f, default_flow_style=False, sort_keys=False)

print("Swagger YAML file generated: swagger.yml")
