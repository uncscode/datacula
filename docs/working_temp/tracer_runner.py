"""run tracer load and analysis"""

import subprocess
import os

# get current file path
path = os.path.dirname(os.path.realpath(__file__))
# change directory to the path of this file
os.chdir(path)

# subprocess.run(["python", "tracer_load.py"])

subprocess.run(["python", "tracer_analysis.py"])

subprocess.run(["python", "tracer_graphs.py"])