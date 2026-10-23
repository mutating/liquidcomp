"""Support python -m liquidcomp as well as the installed liquid executable."""

import sys

from liquidcomp.cli import main

sys.exit(main())
