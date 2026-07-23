# Frontend configuration { #configuration-frontend }

## API url

Data.Rentgen UI requires REST API to be accessible from browser. API url is set up using config file:

```yaml title="config.yml"
ui:
   api_browser_url: http://localhost:8000
```

If both REST API and frontend are served on the same domain (e.g. through Nginx reverse proxy), for example:

- REST API → `/api`
- Frontend → `/`

Then you can use relative path:

```yaml title="config.yml"
ui:
   api_browser_url: /api
```

## Auth provider

By default, Data.Rentgen UI shows login page with username & password fields, designed for [auth-server-dummy][auth-server-dummy].
To show a login page for [auth-server-keycloak][auth-server-keycloak], you should set config option:

```yaml title="config.yml"
ui:
   auth_provider: keycloakAuthProvider
   # auth_provider: dummyAuthProvider
```
