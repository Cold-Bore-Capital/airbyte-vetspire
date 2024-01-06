# Vetspire V2 Source

This is the repository for the Vetspire V2 source connector, written in Python.
For information about how to use this connector within Airbyte, see [the documentation](https://docs.airbyte.com/integrations/sources/vetspire-v2).

## Create a docker image
Images must be built with the correct platform for the target environment.  The following commands will build and push the image for the target platform.
- Build Image:
    - docker build . -t coldborecapital/source-vetspire:x.x.x --platform linux/amd64
- Push Image:
    - docker push coldborecapital/source-vetspire:x.x.x

## Json Schema 
We use Json Schema version 7 for this repository. With graphql, 
we drill into objects in a given query. For example, in 
the appointments graphql query, we need to extract locationId. 
We do this with the following example: 

```query {appointments {location {id}}}```

In order to define a json schema to extract the locationId, one must use an object type
as follows: 

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#", "type": "object", "properties": {
    "location": {
      "type": ["object", "null"], "properties": {
        "id": {"type": ["string", "null"]}
      }
    }
  }
}
```

There are other instances where we might expect more than one id to be returned. An example is if we want to extract 
encounters from appointments as an appointment can have more than one encounter.  

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#", "type": "object", "properties": {
    "encounters": {
      "type": ["array", "null"], "properties": {
        "id": {"type": ["string", "null"]}
      }
    }
  }
}
```
## Local development

### Prerequisites
**To iterate on this connector, make sure to complete this prerequisites section.**

#### Minimum Python version required `= 3.9.0`

#### Build & Activate Virtual Environment and install dependencies
From this connector directory, create a virtual environment:
```
python -m venv .venv
```

This will generate a virtualenv for this module in `.venv/`. Make sure this venv is active in your
development environment of choice. To activate it from the terminal, run:
```
source .venv/bin/activate
pip install -r requirements.txt
pip install '.[tests]'
```
If you are in an IDE, follow your IDE's instructions to activate the virtualenv.

Note that while we are installing dependencies from `requirements.txt`, you should only edit `setup.py` for your dependencies. `requirements.txt` is
used for editable installs (`pip install -e`) to pull in Python dependencies from the monorepo and will call `setup.py`.
If this is mumbo jumbo to you, don't worry about it, just put your deps in `setup.py` but install using `pip install -r requirements.txt` and everything
should work as you expect.

#### Building via Gradle
You can also build the connector in Gradle. This is typically used in CI and not needed for your development workflow.

To build using Gradle, from the Airbyte repository root, run:
```
./gradlew :airbyte-integrations:connectors:source-vetspire-v2:build
```

#### Create credentials
**If you are a community contributor**, follow the instructions in the [documentation](https://docs.airbyte.com/integrations/sources/vetspire-v2)
to generate the necessary credentials. Then create a file `secrets/config.json` conforming to the `source_vetspire_v2/spec.yaml` file.
Note that any directory named `secrets` is gitignored across the entire Airbyte repo, so there is no danger of accidentally checking in sensitive information.
See `integration_tests/sample_config.json` for a sample config file.

**If you are an Airbyte core member**, copy the credentials in Lastpass under the secret name `source vetspire-v2 test creds`
and place them into `secrets/config.json`.

### Locally running the connector
```
python main.py spec
python main.py check --config secrets/config.json
python main.py discover --config secrets/config.json
python main.py read --config secrets/config.json --catalog integration_tests/configured_catalog.json
```

### Locally running the connector docker image

#### Build
First, make sure you build the latest Docker image:
```
docker build . -t airbyte/source-vetspire-v2:dev
```

If you want to build the Docker image with the CDK on your local machine (rather than the most recent package published to pypi), from the airbyte base directory run:
```bash
CONNECTOR_TAG=<TAG_NAME> CONNECTOR_NAME=<CONNECTOR_NAME> sh airbyte-integrations/scripts/build-connector-image-with-local-cdk.sh
```


You can also build the connector image via Gradle:
```
./gradlew :airbyte-integrations:connectors:source-vetspire-v2:airbyteDocker
```
When building via Gradle, the docker image name and tag, respectively, are the values of the `io.airbyte.name` and `io.airbyte.version` `LABEL`s in
the Dockerfile.

#### Run
Then run any of the connector commands as follows:
```
docker run --rm airbyte/source-vetspire-v2:dev spec
docker run --rm -v $(pwd)/secrets:/secrets airbyte/source-vetspire-v2:dev check --config /secrets/config.json
docker run --rm -v $(pwd)/secrets:/secrets airbyte/source-vetspire-v2:dev discover --config /secrets/config.json
docker run --rm -v $(pwd)/secrets:/secrets -v $(pwd)/integration_tests:/integration_tests airbyte/source-vetspire-v2:dev read --config /secrets/config.json --catalog /integration_tests/configured_catalog.json
```
## Testing
Make sure to familiarize yourself with [pytest test discovery](https://docs.pytest.org/en/latest/goodpractices.html#test-discovery) to know how your test files and methods should be named.
First install test dependencies into your virtual environment:
```
pip install .[tests]
```
### Unit Tests
To run unit tests locally, from the connector directory run:
```
python -m pytest unit_tests
```

### Integration Tests
There are two types of integration tests: Acceptance Tests (Airbyte's test suite for all source connectors) and custom integration tests (which are specific to this connector).
#### Custom Integration tests
Place custom tests inside `integration_tests/` folder, then, from the connector root, run
```
python -m pytest integration_tests
```
#### Acceptance Tests
Customize `acceptance-test-config.yml` file to configure tests. See [Connector Acceptance Tests](https://docs.airbyte.com/connector-development/testing-connectors/connector-acceptance-tests-reference) for more information.
If your connector requires to create or destroy resources for use during acceptance tests create fixtures for it and place them inside integration_tests/acceptance.py.
To run your integration tests with acceptance tests, from the connector root, run
```
python -m pytest integration_tests -p integration_tests.acceptance
```
To run your integration tests with docker

### Using gradle to run tests
All commands should be run from airbyte project root.
To run unit tests:
```
./gradlew :airbyte-integrations:connectors:source-vetspire-v2:unitTest
```
To run acceptance and custom integration tests:
```
./gradlew :airbyte-integrations:connectors:source-vetspire-v2:integrationTest
```

## Dependency Management
All of your dependencies should go in `setup.py`, NOT `requirements.txt`. The requirements file is only used to connect internal Airbyte dependencies in the monorepo for local development.
We split dependencies between two groups, dependencies that are:
* required for your connector to work need to go to `MAIN_REQUIREMENTS` list.
* required for the testing need to go to `TEST_REQUIREMENTS` list

### Publishing a new version of the connector
You've checked out the repo, implemented a million dollar feature, and you're ready to share your changes with the world. Now what?
1. Make sure your changes are passing unit and integration tests.
1. Bump the connector version in `Dockerfile` -- just increment the value of the `LABEL io.airbyte.version` appropriately (we use [SemVer](https://semver.org/)).
1. Create a Pull Request.
1. Pat yourself on the back for being an awesome contributor.
1. Someone from Airbyte will take a look at your PR and iterate with you to merge it into master.
