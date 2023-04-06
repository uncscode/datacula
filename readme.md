# *DATACULA*
 
Unveiling Datacula, the Dracula of Python packages, fearlessly devouring complexities in the analysis and data collection from aerosol-gas instruments. Developed by the creators of [Particula](https://github.com/uncscode/particula), Datacula offers seamless integration with their cutting-edge modeling tools.

Datacula furnishes you with a comprehensive suite of tools to import, process, and visualize data from a diverse range of instruments in air quality monitoring. If your instrument data format isn't supported yet, you can contribute by adding it, thereby paving the way for future scientists.

For those with instrumental data, discover Particula—your go-to aerosol particle simulator. Designed for simplicity and adaptability, Particula delivers a robust aerosol simulation system for both gas and particle phases. Harness its power to tackle scientific questions arising from your Datacula data.

## Goals & conduct

The main goal is to develop an aerosol data management and processing tool that is usable, efficient, and productive. In this process, we all will learn developing models in Python and associated packages. Let us all be friendly, respectful, and nice to each other. Any code added to this repository is automatically owned by all. Please speak up if something (even if trivial) bothers you. Talking through things always helps. This is an open-source project, so feel free to contribute, however small or big your contribution may be.

We follow the Google Python style guide [here](https://google.github.io/styleguide/pyguide.html). We have contribution guidelines [here](https://github.com/uncscode/datacula/blob/main/docs/CONTRIBUTING.md) and a code of conduct [here](https://github.com/uncscode/datacula/blob/main/docs/CODE_OF_CONDUCT.md) as well.

## Usage & development

The development of this package will be illustrated through Jupyter notebooks ([here](https://github.com/uncscode/datacula/blob/main/docs)) that will be put together in the form of a Jupyter book on our (to be added).
<!-- [website](https://uncscode.github.io/datacula).  -->
<!-- To use it, you can install `particula` from PyPI or conda-forge with `pip install particula` or `conda install -c conda-forge particula`, respectively. -->

For development, you can fork this repository and then install `particula` in an editable (`-e`) mode --- this is achieved by `pip install -e ".[dev]"` in the root of this repository. Invoking `pip install -e ".[dev]"` will install `particula`, its runtime requirements, and the development and test requirements. The editable mode is useful because it allows seeing the manifestation of code edits globally through the `particula` package in your environment (in a way, with the `-e` mode, `particula` self-updates to account for the latest local code edits).

## Python version

We support python 3.9 and above. To check your python version, run `python --version` in your terminal. To upgrade your python version, run `pip install --upgrade python` or with conda `conda install python=3.10`.

## Tour & Examples
TBD
