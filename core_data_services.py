import requests
import json

import requests

class CoreDataService:
    def __init__(self, template_ids):
        self.template_ids = template_ids
        self.service_url = 'https://data-api-dev.swoop-adventures.com'

    def getSchemaWithArrayLevel(self):
        schemas = []
        for idx, template_id in enumerate(self.template_ids):
            res = requests.get(f"{self.service_url}/core-data-service/v1/templates/{template_id}")
            response = res.json()

            schema_str = response.get("validationSchemas", {}).get("componentSchema")
            if schema_str:
                schema = json.loads(schema_str)
                schemas.append(schema)
                if idx == len(self.template_ids) - 1:
                    self.template_name = response.get('name')
                    self.template_id = response.get('id')
            else:
                schemas.append({})
        return schemas

    def pushValidRowToDB(self, components):
        for idx, component in enumerate(components):
            res = requests.post(f"{self.service_url}/core-data-service/v1/components", json=component)
            if res.status_code == 201:
                print(f"✅ Row {idx + 1} has been pushed!")
                try:
                    data = res.json()
                    comp_id = data.get("id")
                    comp_name = component.get("name")
                    template_id = component.get("templateId")
                    if comp_id and comp_name and template_id:
                        COMPONENT_ID_MAP[(template_id, comp_name)] = comp_id
                        print(f"   ↳ Stored in ID map: ({template_id}, {comp_name}) -> {comp_id}")
                except Exception as e:
                    print(f"⚠️ Could not parse returned ID for row {idx+1}: {e}")
            else:
                try:
                    error_msg = res.json()
                    print(f"❌ Failed to push Row {idx + 1}. Error: {error_msg}")
                except Exception:
                    print(f"❌ Failed to push Row {idx + 1}. HTTP Status: {res.status_code}")

