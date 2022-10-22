FROM mcr.microsoft.com/vscode/devcontainers/python

COPY . /workspaces/template/
RUN pip3 --disable-pip-version-check --no-cache-dir install -e "/workspaces/template/[dev]"