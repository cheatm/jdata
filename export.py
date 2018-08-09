import os

variables = os.environ

exports = ["export %s=%s" % (key, value) for key, value in variables.items()]
text = "#! /bin/bash\n" + "\n".join(exports)
print(text)