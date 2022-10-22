FROM mcr.microsoft.com/vscode/devcontainers/python

COPY . /workspaces/datacula/
RUN pip3 --disable-pip-version-check --no-cache-dir install -e "/workspaces/datacula/[dev]"