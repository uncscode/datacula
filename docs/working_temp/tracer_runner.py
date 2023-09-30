"""run tracer load and analysis"""

import subprocess


subprocess.run(["python", "tracer_load.py"])

subprocess.run(["python", "tracer_analysis.py"])