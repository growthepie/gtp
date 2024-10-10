# Generating schema.py

## 1. Generate schema.json by running the following command:

```bash
curl -X POST   -H "Content-Type: application/json"   --data '{"query": "{ __schema { queryType { name } mutationType { name } subscriptionType { name } types { ...FullType } directives { name description locations args { ...InputValue } } } } fragment FullType on __Type { kind name description fields(includeDeprecated: true) { name description args { ...InputValue } type { ...TypeRef } isDeprecated deprecationReason } inputFields { ...InputValue } interfaces { ...TypeRef } enumValues(includeDeprecated: true) { name description isDeprecated deprecationReason } possibleTypes { ...TypeRef } } fragment InputValue on __InputValue { name description type { ...TypeRef } defaultValue } fragment TypeRef on __Type { kind name ofType { kind name ofType { kind name ofType { kind name ofType { kind name } } } } }"}'   https://graph.mainnet.octant.app/subgraphs/name/octant > schema.json
```

## 2. Install sgqlc library by running the following command:

```bash
pip install sgqlc
```

## 3. Generate schema.py by running the following command:

```bash
sgqlc-codegen.exe schema --docstrings schema.json schema.py
```

## 4. Use schema.py to query the subgraph

```python
from sgqlc.operation import Operation
from sgqlc.endpoint.requests import RequestsEndpoint

from schema import schema

# Create an endpoint
endpoint = RequestsEndpoint('https://graph.mainnet.octant.app/subgraphs/name/octant')

# Create an operation
op = Operation(schema.Query)

# Query the subgraph
op.query {
  allAccounts {
    id
    balance
  }
}

# Execute the operation
data = endpoint(op)

# Print the response
print(data)
```
